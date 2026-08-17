import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# DAG Configuration
OWNER = "maksym"
DAG_ID = "fct_earthquake_geo_power"

# Used tables in DAG
LAYER = "dm"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_earthquake_geo_power"

# DWH connection
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Data Mart: Seismic Activity & Energy Release (Geo & Power)
Aggregated data mart tracking earthquake frequency, focal depth categories, geographic regions, 
and total seismic energy release calculated via the Gutenberg-Richter energy formula ($10^{4.8 + 1.5M}$).
"""

SHORT_DESCRIPTION = "Seismic activity, geographic region extraction, and energy release aggregation"

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
    tags=["dm", "pg", "geo"],
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
        WITH parsed_source AS (
            SELECT
                time::date AS date,
                CASE 
                    WHEN place LIKE '% of %' THEN TRIM(SPLIT_PART(place, ' of ', 2))
                    WHEN place LIKE '%,%' THEN TRIM(SPLIT_PART(place, ',', 2))
                    ELSE COALESCE(TRIM(place), 'Unknown Region')
                END AS region,
                CASE 
                    WHEN depth IS NULL THEN 'unknown'
                    WHEN depth < 70 THEN 'shallow'         -- Shallow (<70 km)
                    WHEN depth >= 70 AND depth < 300 THEN 'intermediate' -- Intermediate (70-300 km)
                    ELSE 'deep'                             -- Deep (>300 km)
                END AS depth_category,
                latitude,
                longitude,
                mag,
                depth
            FROM ods.fct_earthquake
            WHERE time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
              AND type = 'earthquake'
        )
        SELECT
            date,
            region,
            depth_category,
            ROUND(AVG(latitude)::numeric, 4)::REAL AS latitude,
            ROUND(AVG(longitude)::numeric, 4)::REAL AS longitude,
            COUNT(*)::INTEGER AS earthquake_cnt,
            ROUND(AVG(mag::numeric), 2) AS avg_mag,
            MAX(mag)::REAL AS max_mag,
            ROUND(AVG(depth::numeric), 2) AS avg_depth_km,
            SUM(POWER(10, 4.8 + (1.5 * mag))) AS total_energy_joules
        FROM parsed_source
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