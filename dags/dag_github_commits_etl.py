from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta

from plugins.operators.github_to_gcs import GitHubToGCSOperator
from plugins.operators.gcs_transform import GCSTransformOperator
from plugins.operators.gcs_json_to_parquet import GCSJsonToParquetOperator
from dags.config.config import Config

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
    
    # Task 0: Init necessary table (if not created)
    init_table = BigQueryInsertJobOperator(
        task_id='init_table',
        gcp_conn_id=Config.GCS_AIRR_LAB_CONNECTION,
        project_id=Config.PROJECT_ID,
        configuration={
            "query": {
                'query': "{% include 'sql/init_table.sql' %}",
                'useLegacySql': False,
            }    
        }
    )

    # Task 1: Extract raw data from GitHub API to GCS (Bronze)
    extract_github_raw_data_to_gcs = GitHubToGCSOperator(
        task_id='extract_github_raw_data_to_gcs',
        github_token=Config.GITHUB_TOKEN,
        bronze_path=Config.BRONZE_PATH,
        api_url=Config.GITHUB_API_URL,
        batch_size=Config.API_BATCH_SIZE
    )

    # Task 2: Transform data (normalize json to keep only necessary fields)
    transform_gcs_raw_to_staging_data = GCSTransformOperator(
        task_id='transform_gcs_raw_to_staging_data',
        src_path=Config.BRONZE_PATH,
        dest_path=Config.SILVER_PATH
    )
    
    # Task 3: Convert normalized json to parquet files
    convert_json_to_parquet_gcs_data = GCSJsonToParquetOperator(
        task_id='convert_json_to_parquet_gcs_data',
        src_path=Config.SILVER_PATH,
        dest_path=Config.GOLD_PATH
    )
    
    # Task 4: Load data to warehouse
    update_staging_commits_table = GCSToBigQueryOperator(
        task_id='update_staging_commits_table',
        gcp_conn_id=Config.GCS_AIRR_LAB_CONNECTION,
        bucket=Config.GCS_BUCKET,
        source_objects=[
            f"{Config.GOLD_PREFIX_PATH}/dt={{{{ ds }}}}/commits.parquet"
        ],
        destination_project_dataset_table=(
            f"{Config.PROJECT_ID}.{Config.DATASET_ID}.{Config.STAGING_COMMITS_TABLE_NAME}${{{{ ds_nodash }}}}"
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
    
    # Task 5: Update record in date dimension
    update_d_date = BigQueryInsertJobOperator(
        task_id='update_d_date',
        gcp_conn_id=Config.GCS_AIRR_LAB_CONNECTION,
        project_id=Config.PROJECT_ID,
        configuration={
            "query": {
                'query': "{% include 'sql/merge_d_date.sql' %}",
                'useLegacySql': False,
            }    
        }
    )

    # Task 6: Update record in fact
    update_f_commits_hourly = BigQueryInsertJobOperator(
        task_id='update_f_commits_hourly',
        gcp_conn_id=Config.GCS_AIRR_LAB_CONNECTION,
        project_id=Config.PROJECT_ID,
        configuration={
            "query": {
                'query': "{% include 'sql/merge_f_commits_hourly.sql' %}",
                'useLegacySql': False,
            }    
        }
    )

    # Set task dependencies
    
    # Ingest new data to staging table
    init_table >> extract_github_raw_data_to_gcs >> transform_gcs_raw_to_staging_data >> convert_json_to_parquet_gcs_data >> update_staging_commits_table
    
    # Update data mart
    update_staging_commits_table >> [update_d_date, update_f_commits_hourly]
