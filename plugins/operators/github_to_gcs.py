from airflow.models import BaseOperator
from typing import List
import requests
import json
from datetime import datetime, timedelta
from plugins.gcs import upload_to_gcs

class GitHubToGCSOperator(BaseOperator):
    def __init__(
        self,
        task_id: str,
        github_token: str,
        gcs_bucket: str,
        bronze_path: str,
        api_url: str,
        batch_size: int = 100,
        **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.github_token = github_token
        self.gcs_bucket = gcs_bucket
        self.bronze_path = bronze_path
        self.api_url = api_url
        self.batch_size = batch_size

    def execute(self, context):
        
        self.log.info(f"GitHubToGCSOperator execute")
        
        # Get execution date
        execution_date = context['execution_date']
        
        # Fetch commits for the execution date
        commits = self._fetch_commits(execution_date)
        if not commits:
            self.log.info(f"No commits found for date {execution_date}")
            return

        # Save commits as JSON
        partition_date = execution_date.strftime('%Y-%m-%d')
        blob_name = f"{self.bronze_path}/dt={partition_date}/commits.json"
        
        # Initialize GCS client
        upload_to_gcs(gcs_bucket=self.gcs_bucket, blob_name=blob_name, contents=commits)
        
        self.log.info(f"Saved {len(commits)} commits to gs://{self.gcs_bucket}/{blob_name}")

    def _fetch_commits(self, date: datetime) -> List[dict]:
        self.log.info(f"Github Personal Access Token = {self.github_token}")
        headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Format date for GitHub API
        since = date.replace(hour=0, minute=0, second=0).isoformat() + 'Z'
        until = date.replace(hour=16, minute=59, second=59).isoformat() + 'Z'
        
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
            # commits.extend([{
            #     'commit_sha': commit['sha'],
            #     'author_name': commit['commit']['author']['name'],
            #     'author_email': commit['commit']['author']['email'],
            #     'commit_message': commit['commit']['message'],
            #     'committed_at': commit['commit']['author']['date'],
            #     'created_date': date.strftime('%Y-%m-%d')
            # } for commit in page_commits])
            
            page += 1
            self.log.info(f"Fetched page {page-1} with {len(page_commits)} commits")
            
        return commits
