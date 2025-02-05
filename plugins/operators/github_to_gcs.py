from airflow.models import BaseOperator
from typing import List
import requests
import json
from datetime import datetime, timedelta
from plugins.gcs import GCS
from plugins.utils.time_utils import get_execution_date_as_datetime


class GitHubToGCSOperator(BaseOperator):
    # template_fields = ('partition_date')
    def __init__(
        self,
        *,
        task_id: str,
        github_token: str,
        bronze_path: str,
        api_url: str,
        
        batch_size: int = 100,
        **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.github_token = github_token
        self.bronze_path = bronze_path
        self.api_url = api_url
        self.batch_size = batch_size
        
    def execute(self, context):
        
        self.log.info(f"GitHubToGCSOperator execute")
        
        # Get execution date
        run_date = context['execution_date']
        self.log.info(f"run_date = {run_date}")
            
        # Fetch commits for the execution date
        commits = self._fetch_commits(run_date)
        if not commits:
            self.log.info(f"No commits found for date {run_date}")
            return
        
        bucket, prefix = self.bronze_path.replace("gs://", "").split("/", 1)
        
        # Initialize GCS client and upload
        gcs = GCS(partition_date=run_date, log=self.log)
        gcs_path = gcs.upload_to_gcs(
            gcs_bucket=bucket, 
            prefix=prefix, 
            blob_name="commits.json", 
            contents=commits
        )
        
        self.log.info(f"Saved {len(commits)} commits to {gcs_path}")

    def _fetch_commits(self, date: datetime) -> List[dict]:
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
            
            try:
                params["page"] = page
                response = requests.get(
                    self.api_url,
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                self.log.error(f"HTTP error occurred while fetching commits: {str(e)}")
                self.log.error(f"Response status code: {response.status_code}")
                self.log.error(f"Response content: {response.text}")
                break
            
            self.log.info(f"response = {response}")
            self.log.info(f"Response status code: {response.status_code}")
            self.log.info(f"Response content: {response.text}")
            page_commits = response.json()
            if not page_commits:
                break
            
            commits.extend(page_commits)
            page += 1
            self.log.info(f"Fetched page {page-1} with {len(page_commits)} commits")
            
        return commits
