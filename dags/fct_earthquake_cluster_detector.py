import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# DAG Configuration
OWNER = "maksym"
DAG_ID = "fct_earthquake_cluster_detector"

# Target layer settings
LAYER = "dm"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_earthquake_cluster_detector"

# DWH connection
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Data Mart: Earthquake Cluster, Swarm & Aftershock Detector
Analyzes spatial and temporal density of seismic events to detect earthquake swarms,
aftershock sequences, and hourly bursts.
"""

SHORT_DESCRIPTION = "Cluster and aftershock detector data mart"

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
    tags=["dm", "pg", "clusters", "swarms"],
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
                time,
                time::date AS date,
                DATE_TRUNC('hour', time) AS event_hour,
                CASE 
                    WHEN place LIKE '% of %' THEN TRIM(SPLIT_PART(place, ' of ', 2))
                    WHEN place LIKE '%,%' THEN TRIM(SPLIT_PART(place, ',', 2))
                    ELSE COALESCE(TRIM(place), 'Unknown Region')
                END AS region,
                latitude,
                longitude,
                mag,
                depth
            FROM ods.fct_earthquake
            WHERE time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
              AND type = 'earthquake'
        ),
        hourly_bursts AS (
            SELECT
                date,
                region,
                event_hour,
                COUNT(*) AS hourly_cnt
            FROM prepared_events
            GROUP BY date, region, event_hour
        ),
        max_hourly_stats AS (
            SELECT
                date,
                region,
                MAX(hourly_cnt)::INTEGER AS max_hourly_burst_cnt
            FROM hourly_bursts
            GROUP BY date, region
        ),
        regional_summary AS (
            SELECT
                p.date,
                p.region,
                COUNT(*)::INTEGER AS daily_event_cnt,
                MAX(p.mag)::REAL AS max_mag,
                MIN(p.mag)::REAL AS min_mag,
                ROUND((MAX(p.mag) - MIN(p.mag))::numeric, 2) AS mag_spread,
                ROUND(AVG(p.depth::numeric), 2) AS avg_depth_km,
                ROUND(
                    GREATEST(
                        MAX(p.latitude) - MIN(p.latitude), 
                        MAX(p.longitude) - MIN(p.longitude)
                    )::numeric, 
                    3
                ) AS geo_span_degrees,
                ROUND(AVG(p.latitude)::numeric, 4)::REAL AS latitude,
                ROUND(AVG(p.longitude)::numeric, 4)::REAL AS longitude
            FROM prepared_events p
            GROUP BY p.date, p.region
        )
        SELECT
            r.date,
            r.region,
            CASE 
                -- Рой: компактная площадь (разброс <= 0.8 градуса), >= 10 событий, но нет явного супер-удара (< 5.0)
                WHEN r.daily_event_cnt >= 10 AND r.geo_span_degrees <= 0.8 AND r.max_mag < 5.0 THEN 'Swarm'
                -- Главный толчок + афтершоки: сильное событие (>= 5.0) и серия сопровождающих (>= 5)
                WHEN r.max_mag >= 5.0 AND r.daily_event_cnt >= 5 THEN 'Mainshock-Aftershocks'
                -- Повышенная рассеянная активность
                WHEN r.daily_event_cnt >= 10 THEN 'High Activity'
                ELSE 'Isolated'
            END AS cluster_type,
            r.daily_event_cnt,
            COALESCE(h.max_hourly_burst_cnt, 1) AS max_hourly_burst_cnt,
            r.max_mag,
            r.min_mag,
            r.mag_spread,
            r.avg_depth_km,
            r.geo_span_degrees,
            CASE 
                WHEN (r.daily_event_cnt >= 10 AND r.geo_span_degrees <= 0.8) 
                    OR (COALESCE(h.max_hourly_burst_cnt, 1) >= 5) 
                THEN TRUE 
                ELSE FALSE 
            END AS is_swarm_alert,
            r.latitude,
            r.longitude
        FROM regional_summary r
        LEFT JOIN max_hourly_stats h 
          ON r.date = h.date 
         AND r.region = h.region;
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