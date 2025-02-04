from dataclasses import dataclass
from airflow.models import Variable

@dataclass
class PipelineConfig:
    # GitHub API Configuration
    GITHUB_API_URL = "https://api.github.com/repos/apache/airflow/commits"
    GITHUB_TOKEN = Variable.get("GITHUB_TOKEN_SECRET")
    
    # GCS Configuration
    GCS_BUCKET = "airr-labs-interview"
    BRONZE_PATH = "bronze/github_commits"
    STAGING_PATH = "staging/github_commits"
    
    # BigQuery Configuration
    PROJECT_ID = "personal-project-447516"
    DATASET_ID = "github_data"
    TABLE_ID = "commits"
    
    # Batch Configuration
    API_BATCH_SIZE = 100
