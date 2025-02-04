from dataclasses import dataclass
from airflow.models import Variable

@dataclass
class PipelineConfig:
    # GitHub API Configuration
    GITHUB_API_URL = "https://api.github.com/repos/apache/airflow/commits"
    GITHUB_TOKEN = Variable.get("GITHUB_TOKEN_SECRET")
    
    # GCS Configuration
    GCS_BUCKET = "airr-labs-interview"
    
    BRONZE_PREFIX_PATH = "bronze/github_commits"
    SILVER_PREFIX_PATH = "silver/github_commits"
    GOLD_PREFIX_PATH = "gold/github_commits"
    
    BRONZE_PATH = f"gs://{GCS_BUCKET}/{BRONZE_PREFIX_PATH}"
    SILVER_PATH = f"gs://{GCS_BUCKET}/{SILVER_PREFIX_PATH}"
    GOLD_PATH = f"gs://{GCS_BUCKET}/{GOLD_PREFIX_PATH}"
    
    # BigQuery Configuration
    PROJECT_ID = "personal-project-447516"
    DATASET_ID = "github_data"
    TABLE_ID = "commits"
    
    GCS_AIRR_LAB_CONNECTION = 'gcs_airr_lab_interviews'
    
    # Batch Configuration
    API_BATCH_SIZE = 100
