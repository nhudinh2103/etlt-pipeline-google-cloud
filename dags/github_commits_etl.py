from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from plugins.operators.github_to_gcs import GitHubToGCSOperator
# from plugins.operators.duckdb_transform import DuckDBTransformOperator
from plugins.operators.gcs_transform import GCSTransformOperator
from plugins.operators.gcs_json_to_parquet import GCSJsonToParquetOperator
from dags.config.pipeline_config import PipelineConfig
import logging

from airflow.operators.empty import EmptyOperator

import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define end_date as the current time with Ho_Chi_Minh timezone
end_date = pendulum.now("Asia/Ho_Chi_Minh")
# Define start_date as 6 months before the end_date
start_date = end_date.subtract(months=6)

with DAG(
    'github_commits_etl',
    default_args=default_args,
    description='ETL pipeline for GitHub commits',
    schedule_interval='0 8 * * *',  # Daily UTC
    start_date=start_date,
    end_date=end_date,
    catchup=True,
    tags=['github', 'etl', 'airr_labs'],
    max_active_runs=18
) as dag:

    # Task 1: Extract raw data from GitHub API to GCS (Bronze)
    extract_raw_data = GitHubToGCSOperator(
        task_id='extract_raw_data',
        github_token=PipelineConfig.GITHUB_TOKEN,
        bronze_path=PipelineConfig.BRONZE_PATH,
        api_url=PipelineConfig.GITHUB_API_URL,
        batch_size=PipelineConfig.API_BATCH_SIZE
    )

    # Task 2: Transform data (normalize json to keep only necessary fields)
    transform_json_gcs_data = GCSTransformOperator(
        task_id='transform_json_gcs_data',
        src_path=PipelineConfig.BRONZE_PATH,
        dest_path=PipelineConfig.SILVER_PATH
    )
    
    # Task 3: Convert normalized json to parquet files
    convert_parquet_gcs_data = GCSJsonToParquetOperator(
        task_id='convert_parquet_gcs_data',
        src_path=PipelineConfig.SILVER_PATH,
        dest_path=PipelineConfig.GOLD_PATH
    )
    
    # transform_staging = DuckDBTransformOperator(
    #     task_id='transform_staging',
    #     source_path=f"gs://{PipelineConfig.GCS_BUCKET}/{PipelineConfig.BRONZE_PATH}/dt={{{{ ds }}}}/commits.parquet",
    #     destination_path=f"gs://{PipelineConfig.GCS_BUCKET}/{PipelineConfig.STAGING_PATH}/dt={{{{ ds }}}}/commits_transformed.parquet",
    #     sql_path='dags/sql/transform_commits.sql'
    # )

    # Task 4: Load data to warehouse
    # load_data_to_warehouse = EmptyOperator(task_id='load_data_to_warehouse')
    load_data_to_warehouse = GCSToBigQueryOperator(
        task_id='load_data_to_warehouse',
        gcp_conn_id=PipelineConfig.GCS_AIRR_LAB_CONNECTION,
        bucket=PipelineConfig.GCS_BUCKET,
        source_objects=[
            f"{PipelineConfig.GOLD_PREFIX_PATH}/dt={{{{ ds }}}}/commits.parquet"
        ],
        destination_project_dataset_table=(
            f"{PipelineConfig.PROJECT_ID}.{PipelineConfig.DATASET_ID}.{PipelineConfig.TABLE_ID}${{{{ ds_nodash }}}}"
        ),
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True,
        time_partitioning={
            'type': 'DAY',
            'field': 'dt',
        }
    )

    # Set task dependencies
    extract_raw_data >> transform_json_gcs_data >> convert_parquet_gcs_data >> load_data_to_warehouse
