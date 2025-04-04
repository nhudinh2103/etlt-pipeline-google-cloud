from airflow.models import BaseOperator
from typing import List, Dict
import json
from datetime import datetime, timedelta
from plugins.gcs import GCS
from plugins.utils.time_utils import get_hive_partition_prefix_str,get_execution_date_as_datetime

class GCSTransformOperator(BaseOperator):
    
    """
    Operator that transforms GitHub commits data and saves to staging area.
    """
    def __init__(
        self,
        *,
        src_path: str,
        dest_path: str,
        **kwargs
    ) -> None:
        """
        Initialize the operator.
        
        Args:
            src_path: Source GCS path (gs://bucket/path)
            dest_path: Destination GCS path for transformed data (gs://bucket/path)
        """
        super().__init__(**kwargs)
        self.src_path = src_path
        self.dest_path = dest_path

    def transform_github_commits(self, commits_data: List[Dict]) -> List[Dict]:
        """
        Transform GitHub commits data by keeping only specific fields.
        
        Args:
            commits_data: List of commit data dictionaries
            
        Returns:
            List[Dict]: Transformed commit data with only required fields
        """
        transformed_data = []
        for commit in commits_data:
            commit_date = datetime.strptime(commit['commit']['committer']['date'], '%Y-%m-%dT%H:%M:%SZ')
            transformed_commit = {
                'commit_sha': commit.get('sha', ''),
                'committer_id': commit['committer'].get('id') if commit.get('committer') else -1,
                'committer_name': commit['commit']['committer']['name'],
                'committer_email': commit['commit']['committer']['email'],
                'committer_date': commit['commit']['committer']['date'],
                'dt': (commit_date + timedelta(hours=7)).strftime('%Y-%m-%d')
            }
            
            transformed_data.append(transformed_commit)
        return transformed_data

    def execute(self, context) -> None:
        """
        Execute the operator to transform GitHub commits data and save to staging.
        
        Args:
            context: Airflow context containing execution_date
        """
        
        partition_date = context['execution_date']
        
        # Process files in partition
        partition_path = get_hive_partition_prefix_str(partition_date)
        
        self.log.info(f"Starting transformation for partition date: {partition_path}")
        self.log.info(f"Source path: {self.src_path}")
        
        src_bucket, src_blob = self.src_path.replace("gs://", "").split("/", 1)
        dest_bucket, dest_blob = self.dest_path.replace("gs://", "").split("/", 1)
        
        full_src_prefix = f"{src_blob}/{partition_path}"
        
        gcs = GCS(partition_date=partition_date, log=self.log)        
        blobs = gcs.gcs_hook.list(bucket_name=src_bucket, prefix=full_src_prefix)
        processed_files = []
        
        for src_blob_path in blobs:
            if not src_blob_path.endswith('.json'):
                continue
                
            self.log.info(f"Processing file: gs://{src_bucket}/{src_blob_path}")
            
            # Download and transform
            file_content = gcs.gcs_hook.download(
                bucket_name=src_bucket,
                object_name=src_blob_path
            )
            
            if not file_content:
                continue
            
            # Transform the data
            json_content = json.loads(file_content)
            transformed_data = self.transform_github_commits(json_content)
            
            # Upload transformed data
            dest_blob_path = f"{dest_blob}/{partition_path}/{src_blob_path.split('/')[-1]}"
            
            self.log.info(f"Saving transformed data to: gs://{dest_bucket}/{dest_blob_path}")
            
            gcs.gcs_hook.upload(
                bucket_name=dest_bucket,
                object_name=dest_blob_path,
                data=json.dumps(transformed_data, indent=2),
                mime_type='application/json'
            )
            
            processed_files.append({
                "source": f"gs://{src_bucket}/{src_blob_path}",
                "destination": f"gs://{dest_bucket}/{dest_blob_path}"
            })
        
        self.log.info(f"Successfully transformed {len(processed_files)} files for partition date: {partition_path}")
        for file_info in processed_files:
            self.log.info(f"Transformed: {file_info['source']} -> {file_info['destination']}")
