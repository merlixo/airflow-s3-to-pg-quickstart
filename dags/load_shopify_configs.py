
""" DAG for shopify configs ETL."""

import logging

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from io import StringIO
from datetime import timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import pandas as pd
from pandas import DataFrame

logger = logging.getLogger('load_shopify_configs')
logger.setLevel(logging.INFO)

S3_BUCKET = 'alg-data-public'  # In enterprise projects, this param would be loaded from a config file, with different values for prod vs dev. 
TARGET_TABLE = 'shopify_configs'

def csv_file_name(date: str) -> str:
    return f'{date}.csv'
    
def delete_existing_data_sql(date: str) -> str:
    """ SQL to delete all existing records from the target table for a given day.
    If the table does not exist yet (1st run), the SQL must not fail."""
    
    sql = f"""DO $$
    BEGIN
      DELETE FROM {TARGET_TABLE} 
      WHERE export_date='{date}';
    EXCEPTION WHEN UNDEFINED_TABLE THEN
      -- do nothing
    END$$;"""
    return sql;

def extract(file_name: str) -> DataFrame:
    """ Extract data from S3 into a pandas frame """
    
    logger.info(f"Read data from file '{file_name}' on bucket '{S3_BUCKET}'.")
    s3_hook = S3Hook(aws_conn_id='aws')
    csv_content = s3_hook.read_key(key=file_name, bucket_name=S3_BUCKET)
    df = pd.read_csv(StringIO(csv_content))
    return df


def transform(df: DataFrame) -> DataFrame:
    """ Transform data using pandas """
    
    logger.info(f'Clean and enrich data.')
    
    # Filter rows where application_id is not empty
    df = df[df.application_id.str.len() > 0].copy()
    
    # Add the 'has_specific_prefix' column
    df['has_specific_prefix'] = df.index_prefix.ne('shopify_')
    return df

    
def load(df: DataFrame) -> None:
    """ Load data in the dwh """
    
    logger.info(f'Load {df.shape[0]} records in Postgres table {TARGET_TABLE}.')
    pg_hook = PostgresHook(postgres_conn_id='pg')
    df.to_sql(
        name=TARGET_TABLE,
        con=pg_hook.get_sqlalchemy_engine(),
        if_exists='append',
        index=False,
        chunksize=1000,
        method='multi'
    )


def process_csv(ds, **kwargs):
    """ This function is exectuted by the 'process_csv' task.
    
    Extract, Transform and Load actions are grouped in a single Airflow task because the process is simple enough, 
    and to avoid data transfers between tasks using XCOM.
    """
    
    df = extract(csv_file_name(ds))
    df = transform(df)
    load(df)



with DAG(
    dag_id='load_shopify_configs',
    description='Extracts new Shopify configs from S3, cleans & enriches data, and loads the result in Postgres',
    schedule='0 3 * * *',
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        # "on_failure_callback": slack_alert,
        "retries": 3,
        "retry_delay": timedelta(seconds=10)
    },
    start_date=datetime(2019, 4, 1),
    end_date=datetime(2019, 4, 8),
    # On startup, DAG will be triggered for each days of the scheduling period, with parallelism of 5
    # Parallelism is possible because the current dagrun does not depend on the previous one (cf "depends_on_past": False).
    catchup=True,
    max_active_runs=5 # Limit DAG concurency (overkill here)
) as dag:
    
    # Wait for the CSV to land on S3, 12h max then fail to alert.
    wait_csv_on_s3_task = S3KeySensor(
        task_id=f'wait_csv_s3',
        bucket_name=S3_BUCKET,
        bucket_key=csv_file_name('{{ ds }}'),
        timeout=12*3600,  # 12 hours
        aws_conn_id='aws'
    )
    
    # For idempotency, we delete eventual existing data matching with the current execution date (loaded by a previous dagrun).
    # Idempotency is necessary for backfilling.
    delete_existing_data_task = SQLExecuteQueryOperator(
        task_id="delete_existing_data",
        sql=delete_existing_data_sql('{{ ds }}'),
        conn_id="pg"
    )

    # Once the CSV is on S3, run the ETL process.
    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv
    )
    
    wait_csv_on_s3_task >> delete_existing_data_task >> process_csv_task