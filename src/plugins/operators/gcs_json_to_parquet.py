from airflow.models import BaseOperator
from typing import Optional
from datetime import datetime
from plugins.gcs import GCS
from plugins.utils.time_utils import get_execution_date_as_datetime

class GCSJsonToParquetOperator(BaseOperator):
    """
    Operator that converts JSON data from GCS to Parquet format.
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
            src_path: Source GCS path with JSON files (gs://bucket/path)
            dest_path: Destination GCS path for parquet files (gs://bucket/path)
        """
        super().__init__(**kwargs)
        self.src_path = src_path
        self.dest_path = dest_path
        

    def execute(self, context) -> None:
        """
        Execute the operator to convert JSON files to parquet format.
        
        Args:
            context: Airflow context containing execution_date
        """
        
        partition_date = context['execution_date']
        
        self.log.info(f"Converting JSON files for partition date: {partition_date.strftime('%Y-%m-%d')}")
        self.log.info(f"Source path: {self.src_path}")
        self.log.info(f"Destination path: {self.dest_path}")
        
        src_bucket, src_blob = self.src_path.replace("gs://", "").split("/", 1)
        dest_bucket, dest_blob = self.dest_path.replace("gs://", "").split("/", 1)
        
        gcs = GCS(partition_date=partition_date, log=self.log)
        processed_files = gcs.process_bronze_files(src_bucket, src_blob, dest_bucket, dest_blob)
        
        self.log.info(f"Successfully converted {len(processed_files)} files to parquet for partition date: {partition_date.strftime('%Y-%m-%d')}")
        for file_info in processed_files:
            self.log.info(f"Converted: {file_info['source']} -> {file_info['destination']}")
