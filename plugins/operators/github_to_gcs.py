from airflow.models import BaseOperator
from typing import List
import requests
import json
from datetime import datetime, timedelta
from plugins.gcs import GCS

class GitHubToGCSOperator(BaseOperator):
    template_fields = ('partition_date')
    def __init__(
        self,
        task_id: str,
        github_token: str,
        gcs_bucket: str,
        bronze_path: str,
        api_url: str,
        partition_date: datetime = None,
        batch_size: int = 100,
        **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.github_token = github_token
        self.gcs_bucket = gcs_bucket
        self.bronze_path = bronze_path
        self.api_url = api_url
        self.batch_size = batch_size
        self.partition_date = partition_date

    def execute(self, context):
        
        self.log.info(f"GitHubToGCSOperator execute")
        self.log.info(f"partition_date = {self.partition_date}")
        
        # Get execution date
        run_date = self.partition_date
        if not run_date:
            run_date = context['execution_date']
        
        # Fetch commits for the execution date
        commits = self._fetch_commits(run_date)
        if not commits:
            self.log.info(f"No commits found for date {run_date}")
            return
        
        # Initialize GCS client and upload
        gcs = GCS(partition_date=run_date, log=self.log)
        gcs_path = gcs.upload_to_gcs(
            gcs_bucket=self.gcs_bucket, 
            prefix=self.bronze_path, 
            blob_name="commits.json", 
            contents=commits
        )
        
        self.log.info(f"Saved {len(commits)} commits to {gcs_path}")

    def _fetch_commits(self, date: datetime) -> List[dict]:
        self.log.info(f"Github Personal Access Token = {self.github_token}")
        headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Format date for GitHub API
        since = (date - timedelta(days=1)).replace(hour=17, minute=0, second=0).isoformat() + 'Z'
        until = date.replace(hour=16, minute=59, second=59).isoformat() + 'Z'
        
        self.log.info(f"Call Github API: {self.api_url}")
        self.log.info(f"since: {since}")
        self.log.info(f"until: {until}")
        
        params = {
            "since": since,
            "until": until,
            "per_page": self.batch_size
        }
        
        commits = []
        page = 1
        
        while True:
            params["page"] = page
            response = requests.get(
                self.api_url,
                headers=headers,
                params=params
            )
            response.raise_for_status()
            
            page_commits = response.json()
            if not page_commits:
                break
            
            commits.extend(page_commits)
            page += 1
            self.log.info(f"Fetched page {page-1} with {len(page_commits)} commits")
            
        return commits
