import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# DAG Configuration
OWNER = "maksym"
DAG_ID = "fct_earthquake_richter_class"

# Used tables in DAG
LAYER = "dm"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_earthquake_richter_class"

# DWH connection
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Data Mart: Richter Scale Earthquake Categorization
Aggregated earthquake data mart grouped by dates and Richter scale categories.
Serves as a data source for monitoring seismic activity and alert dashboards in Metabase.
"""

SHORT_DESCRIPTION = "Earthquake aggregation by magnitude categories (Richter scale)"

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
    tags=["dm", "pg"],
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
        SELECT
            time::date AS date,
            CASE 
                WHEN mag IS NULL THEN 'unknown'
                WHEN mag < 2.0 THEN 'micro'
                WHEN mag >= 2.0 AND mag < 4.0 THEN 'minor'
                WHEN mag >= 4.0 AND mag < 5.0 THEN 'light'
                WHEN mag >= 5.0 AND mag < 6.0 THEN 'moderate'
                WHEN mag >= 6.0 AND mag < 7.0 THEN 'strong'
                WHEN mag >= 7.0 AND mag < 8.0 THEN 'major'
                ELSE 'great'
            END AS mag_category_code,
            CASE 
                WHEN mag IS NULL THEN 'Unknown'
                WHEN mag < 2.0 THEN 'Micro (<2.0)'
                WHEN mag >= 2.0 AND mag < 4.0 THEN 'Minor (2.0-3.9)'
                WHEN mag >= 4.0 AND mag < 5.0 THEN 'Light (4.0-4.9)'
                WHEN mag >= 5.0 AND mag < 6.0 THEN 'Moderate (5.0-5.9)'
                WHEN mag >= 6.0 AND mag < 7.0 THEN 'Strong (6.0-6.9)'
                WHEN mag >= 7.0 AND mag < 8.0 THEN 'Major (7.0-7.9)'
                ELSE 'Great (>=8.0)'
            END AS mag_category_name,
            COUNT(*)::INTEGER AS earthquake_cnt,
            ROUND(AVG(mag::numeric), 2) AS avg_mag,
            MAX(mag)::REAL AS max_mag,
            ROUND(AVG(depth::numeric), 2) AS avg_depth,
            COUNT(CASE WHEN mag >= 6.0 THEN 1 END)::INTEGER AS high_risk_cnt
        FROM
            ods.fct_earthquake
        WHERE
            time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
            AND type = 'earthquake'
        GROUP BY 
            1, 2, 3;
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