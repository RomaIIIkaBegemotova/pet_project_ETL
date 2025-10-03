import logging
import pendulum
import duckdb
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.python import PythonOperator

OWNER = "Roman"
DAG_ID = "data_from_s3_to_postgres"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

DATABASE = Variable.get("pg_db")
USER = Variable.get("user_pg")
PASSWORD = Variable.get("password_pg")

TARGET_TABLE = "earthshakes"
LONG_DESCRIPTION = """
LONG_DESCRIPTION 
"""

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 1, tz="Asia/Krasnoyarsk"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")

    end_date = (context["data_interval_start"] + pendulum.duration(days=1)).format(
        "YYYY-MM-DD"
    )

    return start_date, end_date


def extract_data_s3_load_postgre(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()
    con.sql(
        f"""
        SET s3_use_ssl = FALSE;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';


        CREATE OR REPLACE SECRET secret_pg (
        TYPE postgres,
        HOST 'postgres_dwh',
        PORT 5432,
        DATABASE postgres,
        USER 'postgres',
        PASSWORD '{PASSWORD}'
    );

    ATTACH '' AS postgres_db (TYPE postgres, SECRET secret_pg);

    INSERT INTO postgres_db.{TARGET_TABLE}
        (
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType,
            nst,
            gap,
            dmin,
            rms,
            net,
            some_id,
            updated,
            place,
            type,
            horizontalError,
            depthError,
            magError,
            magNst,
            status,
            locationSource,
            magSource
        )
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError,
            depthError,
            magError,
            magNst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://prod/row/{start_date}/{start_date}_00-00-00.gz.parquet';
    """
    )


with DAG(
    dag_id=DAG_ID,
    description="Extract data from S3 && load to PG",
    tags=["s3", "pg", "ETL"],
    schedule="0 5 * * *",
    default_args=args,
    catchup=True,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    catch_external_dag_load_to_s3 = ExternalTaskSensor(
        task_id="wait_for_raw_dag",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600 * 24,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    extract_data_s3_load_postgre = PythonOperator(
        task_id="extract_data_s3_load_postgre",
        python_callable=extract_data_s3_load_postgre,
    )

    end = EmptyOperator(
        task_id="end",
    )

start >> catch_external_dag_load_to_s3 >> extract_data_s3_load_postgre >> end
