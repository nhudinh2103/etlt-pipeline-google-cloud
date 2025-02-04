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

with DAG(
    'github_commits_etl',
    default_args=default_args,
    description='ETL pipeline for GitHub commits',
    schedule_interval='0 8 * * *',  # Daily UTC
    start_date=pendulum.datetime(2025,1,27,tz='Asia/Ho_Chi_Minh'),
    end_date=pendulum.datetime(2025,1,28,tz='Asia/Ho_Chi_Minh'),
    catchup=True,
    tags=['github', 'etl', 'airr_labs'],
) as dag:

    # Task 1: Extract raw data from GitHub API to GCS (Bronze)
    extract_raw_data = GitHubToGCSOperator(
        task_id='extract_raw_data',
        github_token=PipelineConfig.GITHUB_TOKEN,
        gcs_bucket=PipelineConfig.GCS_BUCKET,
        bronze_path=PipelineConfig.BRONZE_PATH,
        api_url=PipelineConfig.GITHUB_API_URL,
        batch_size=PipelineConfig.API_BATCH_SIZE,
        filter_date='{{ execution_date }}'
    )

    # Task 2: Transform data (normalize json to keep only necessary fields)
    transform_json_gcs_data = GCSTransformOperator(
        task_id='transform_json_gcs_data',
        src_path=PipelineConfig.BRONZE_PATH,
        dest_path=PipelineConfig.SILVER_PATH,
        partition_date='{{ execution_date }}'
    )
    
    # Task 3: Convert normalized json to parquet files
    convert_parquet_gcs_data = GCSJsonToParquetOperator(
        task_id='convert_parquet_gcs_data',
        src_path=PipelineConfig.SILVER_PATH,
        dest_path=PipelineConfig.GOLD_PATH,
        partition_date='{{ execution_date }}'
    )
    
    # transform_staging = DuckDBTransformOperator(
    #     task_id='transform_staging',
    #     source_path=f"gs://{PipelineConfig.GCS_BUCKET}/{PipelineConfig.BRONZE_PATH}/dt={{{{ ds }}}}/commits.parquet",
    #     destination_path=f"gs://{PipelineConfig.GCS_BUCKET}/{PipelineConfig.STAGING_PATH}/dt={{{{ ds }}}}/commits_transformed.parquet",
    #     sql_path='dags/sql/transform_commits.sql'
    # )

    # Task 4: Load data to warehouse
    load_data_to_warehouse = EmptyOperator(task_id='load_data_to_warehouse')
    # load_data_to_warehouse = GCSToBigQueryOperator(
    #     task_id='load_data_to_warehouse',
    #     bucket=PipelineConfig.GCS_BUCKET,
    #     source_objects=[
    #         f"{PipelineConfig.STAGING_PATH}/dt={{{{ ds }}}}/commits_transformed.parquet"
    #     ],
    #     destination_project_dataset_table=(
    #         f"{PipelineConfig.PROJECT_ID}."
    #         f"{PipelineConfig.DATASET_ID}."
    #         f"{PipelineConfig.TABLE_ID}${{{{ ds_nodash }}}}"
    #     ),
    #     source_format='PARQUET',
    #     write_disposition='WRITE_TRUNCATE',
    #     create_disposition='CREATE_IF_NEEDED',
    #     schema_fields=[
    #         {'name': 'commit_sha', 'type': 'STRING', 'mode': 'REQUIRED'},
    #         {'name': 'author_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    #         {'name': 'author_email', 'type': 'STRING', 'mode': 'REQUIRED'},
    #         {'name': 'commit_message', 'type': 'STRING', 'mode': 'REQUIRED'},
    #         {'name': 'committed_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    #         {'name': 'created_date', 'type': 'DATE', 'mode': 'REQUIRED'}
    #     ],
    #     time_partitioning={
    #         'type': 'DAY',
    #         'field': 'created_date',
    #     }
    # )

    # Set task dependencies
    extract_raw_data >> transform_json_gcs_data >> convert_parquet_gcs_data >> load_data_to_warehouse
