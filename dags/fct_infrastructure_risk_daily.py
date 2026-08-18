import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# DAG Configuration
OWNER = "maksym"
DAG_ID = "fct_infrastructure_risk_daily"

# Target layer settings
LAYER = "dm"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_infrastructure_risk_daily"

# DWH connection
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Data Mart: Daily Infrastructure & Insurance Risk
Evaluates structural hazard potential by combining focal depth and magnitude 
into a ground impact score, categorizing earthquakes into ultra-surface (<=15km), 
shallow (15-70km), and intermediate/deep (>70km).
"""

SHORT_DESCRIPTION = "Daily seismic infrastructure risk assessment by region"

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
    tags=["dm", "pg", "risk"],
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
        WITH prepared_events AS (
            SELECT
                time::date AS date,
                CASE 
                    WHEN place LIKE '% of %' THEN TRIM(SPLIT_PART(place, ' of ', 2))
                    WHEN place LIKE '%,%' THEN TRIM(SPLIT_PART(place, ',', 2))
                    ELSE COALESCE(TRIM(place), 'Unknown Region')
                END AS region,
                mag,
                COALESCE(depth, 10.0) AS depth,
                -- Ground impact approximation
                POWER(10, 1.5 * mag) / POWER(GREATEST(COALESCE(depth, 10.0), 5.0), 1.5) AS raw_impact_score
            FROM ods.fct_earthquake
            WHERE time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
              AND type = 'earthquake'
        ),
        regional_agg AS (
            SELECT
                date,
                region,
                COUNT(CASE WHEN depth <= 15.0 THEN 1 END)::INTEGER AS ultra_surface_quake_cnt,
                COUNT(CASE WHEN depth > 15.0 AND depth <= 70.0 THEN 1 END)::INTEGER AS shallow_quake_cnt,
                COUNT(CASE WHEN depth > 70.0 THEN 1 END)::INTEGER AS intermediate_deep_cnt,
                COUNT(*)::INTEGER AS total_quake_cnt,
                MAX(mag)::REAL AS max_mag,
                ROUND(AVG(depth::numeric), 2) AS avg_depth_km,
                ROUND(MAX(raw_impact_score)::numeric, 2)::NUMERIC(18,2) AS peak_impact_score,
                COUNT(CASE WHEN raw_impact_score >= 1000.0 OR (mag >= 5.5 AND depth <= 20.0) THEN 1 END)::INTEGER AS hazardous_event_cnt
            FROM prepared_events
            GROUP BY date, region
        )
        SELECT
            date,
            region,
            CASE 
                WHEN peak_impact_score >= 5000 OR hazardous_event_cnt >= 2 THEN 'Critical'
                WHEN peak_impact_score >= 1000 OR max_mag >= 5.0 THEN 'High'
                WHEN peak_impact_score >= 200 OR max_mag >= 3.5 THEN 'Medium'
                ELSE 'Low'
            END AS risk_level,
            ultra_surface_quake_cnt,
            shallow_quake_cnt,
            intermediate_deep_cnt,
            total_quake_cnt,
            max_mag,
            avg_depth_km,
            peak_impact_score,
            hazardous_event_cnt
        FROM regional_agg;
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