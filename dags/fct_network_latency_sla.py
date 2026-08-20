import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# DAG Configuration
OWNER = "maksym"
DAG_ID = "fct_network_latency_sla"

# Target layer settings
LAYER = "dm"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_network_latency_sla"

# DWH connection
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Data Mart: Network Detection Latency & SLA Monitoring
Measures the time lag between event occurrence (`time`) and catalog registration/update (`updated`).
Calculates p50, p95 latencies and review rates per seismic network and location source.
"""

SHORT_DESCRIPTION = "Seismic network detection latency and SLA performance data mart"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 6, 1, tz="Europe/Kyiv"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    schedule="0 2 * * *",
    default_args=args,
    tags=["dm", "pg", "sla", "dataops"],
    description=SHORT_DESCRIPTION,
    catchup=True,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        WITH calculated_delays AS (
            SELECT
                time::date AS date,
                COALESCE(net, 'unknown') AS net,
                COALESCE(location_source, 'unknown') AS location_source,
                status,
                -- Вычисление задержки в секундах
                GREATEST(EXTRACT(EPOCH FROM (updated - time)), 0)::FLOAT8 AS latency_sec
            FROM ods.fct_earthquake
            WHERE time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
              AND updated >= time
        )
        SELECT
            date,
            net,
            location_source,
            COUNT(*)::INTEGER AS total_events,
            COUNT(CASE WHEN LOWER(status) = 'reviewed' THEN 1 END)::INTEGER AS reviewed_events_cnt,
            ROUND(
                (COUNT(CASE WHEN LOWER(status) = 'reviewed' THEN 1 END)::numeric / NULLIF(COUNT(*), 0)) * 100.0, 
                2
            ) AS manual_review_rate_pct,
            ROUND(AVG(latency_sec)::numeric, 2) AS avg_latency_sec,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_sec)::numeric, 2) AS p50_latency_sec,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_sec)::numeric, 2) AS p95_latency_sec,
            ROUND(MAX(latency_sec)::numeric, 2) AS max_latency_sec,
            COUNT(CASE WHEN latency_sec <= 300 THEN 1 END)::INTEGER AS fast_detection_cnt,
            COUNT(CASE WHEN latency_sec > 3600 THEN 1 END)::INTEGER AS slow_detection_cnt
        FROM calculated_delays
        GROUP BY 1, 2, 3;
        """,
    )

    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN
        (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """,
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
        start
        >> sensor_on_raw_layer
        >> drop_stg_table_before
        >> create_stg_table
        >> drop_from_target_table
        >> insert_into_target_table
        >> drop_stg_table_after
        >> end
    )