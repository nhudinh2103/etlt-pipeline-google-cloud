from airflow.models import BaseOperator
import pandas as pd

class GCSTransformOperator(BaseOperator):
    def __init__(
        self,
        task_id: str,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.source_bucket = source_bucket
        self.source_path = source_path
        self.destination_bucket = destination_bucket
        self.destination_path = destination_path

    def execute(self, context):
        # Read from bronze layer
        source_path = f"gs://{self.source_bucket}/{self.source_path}"
        self.log.info(f"Reading from {source_path}")
        
        
        # try:
        #     df = pd.read_parquet(source_path)
        # except Exception as e:
        #     self.log.info(f"No data found at {source_path}. Error: {str(e)}")
        #     return

        # # Apply transformations
        # df['author_name'] = df['author_name'].str.strip()
        # df['author_email'] = df['author_email'].str.lower()
        # df['commit_message'] = df['commit_message'].str.strip()
        
        # # Convert timestamp to UTC
        # df['committed_at'] = pd.to_datetime(df['committed_at']).dt.tz_convert('UTC')
        
        # # Save to staging layer
        # destination_path = f"gs://{self.destination_bucket}/{self.destination_path}"
        # self.log.info(f"Saving transformed data to {destination_path}")
        
        # df.to_parquet(
        #     destination_path,
        #     engine='pyarrow',
        #     compression='snappy'
        # )
        # self.log.info(f"Successfully transformed and saved {len(df)} records")
