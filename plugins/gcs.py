from airflow.providers.google.cloud.hooks.gcs import GCSHook
from typing import List, Dict, Any
import json
import pandas as pd
from datetime import datetime
from io import BytesIO
import tempfile
import os
from dags.config.pipeline_config import PipelineConfig
from plugins.utils.time_utils import get_hive_partition_prefix_str

import pyarrow as pa
import pyarrow.parquet as pq
import io



class GCS:
    """
    Helper class for GCS operations with built-in partition date handling.
    """
    
    def __init__(self, partition_date: datetime, gcp_conn_id = PipelineConfig.GCS_AIRR_LAB_CONNECTION, log = None) -> None:
        """
        Initialize GCSHelper with partition date.
        
        Args:
            partition_date (datetime): The partition date to use for operations
        """
        self.partition_date = partition_date
        self.hook_args = {'gcp_conn_id': gcp_conn_id}
        self.gcs_hook = GCSHook(**self.hook_args)
        self.log = log
        self.log.info(f"partition_date = {partition_date}")
        
    def process_bronze_files(self, src_gcs_bucket: str, src_prefix: str, dest_gcs_bucket: str, dest_prefix: str) -> List[Dict[str, str]]:
        """
        Process JSON files from bronze layer and convert to parquet.
        
        Args:
            gcs_bucket (str): The GCS bucket name
            prefix (str): The prefix path in the bucket
            
        Returns:
            List[Dict[str, str]]: List of processed files with their source and destination paths
        """
        # Format partition path using hive format
        partition_path = get_hive_partition_prefix_str(self.partition_date)
        full_src_prefix = f"{src_prefix}/{partition_path}"
        
        # List all files in the partition
        blobs = self.gcs_hook.list(bucket_name=src_gcs_bucket, prefix=full_src_prefix)
        
        dirpath = tempfile.mkdtemp()
        processed_files = []
        for src_blob in blobs:
            if not src_blob.endswith('.json'):
                continue
            
            dest_file_name = os.path.basename(os.path.splitext(src_blob)[0])  + '.parquet'
            dest_blob = f"{dest_prefix}/{partition_path}/{dest_file_name}"
            
            if self.log:
                self.log.info(f"Processing file: gs://{src_gcs_bucket}/{src_blob} -> gs://{dest_gcs_bucket}/{full_dest_prefix}")
            
            self.__download_json_upload_parquet(
                src_gcs_bucket=src_gcs_bucket, 
                src_blob=src_blob,
                dest_gcs_bucket=dest_gcs_bucket, 
                dest_blob=dest_blob,
                tmp_dir=dirpath
            )
            
            processed_files.append({
                "source": f"gs://{src_gcs_bucket}/{src_blob}",
                "destination": f"gs://{dest_gcs_bucket}/{dest_blob}"
            })
        
        return processed_files
    
    def __download_json_upload_parquet(self, src_gcs_bucket: str, src_blob: str, dest_gcs_bucket, dest_blob: str, tmp_dir: str) -> None:
        """
        Download JSON file from GCS, convert to parquet, and upload back to GCS.
        
        Args:
            gcs_bucket (str): The GCS bucket name
            src_blob (str): Source blob path (JSON file)
            dest_blob (str): Destination blob path (Parquet file)
            tmp_dir (str): Directory to store temporary files
        """
        # Generate temporary file paths with random suffixes
        temp_json = tempfile.NamedTemporaryFile(suffix='.json', dir=tmp_dir)
        temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', dir=tmp_dir)
        
        # Download content to temporary file
        if self.log:
            self.log.info(f"Downloading from GCS: gs://{src_gcs_bucket}/{src_blob}")
            self.log.info(f"Writing to temporary file: {temp_json.name}")
            
        json_input_path = self.gcs_hook.download(
            bucket_name=src_gcs_bucket,
            object_name=src_blob,
            filename=temp_json.name
        )
        
        if not json_input_path:
            return
        
        self.__convert_json_to_parquet(json_input_path=json_input_path, parquet_output_path=temp_parquet.name)

        # Upload parquet data
        if self.log:
            self.log.info(f"Uploading to GCS: gs://{dest_gcs_bucket}/{dest_blob}")
            self.log.info(f"Reading from temporary file: {temp_parquet.name}")
        self.gcs_hook.upload(
            bucket_name=dest_gcs_bucket,
            object_name=dest_blob,
            filename=temp_parquet.name,
            mime_type='application/octet-stream'
        )

    def __convert_json_to_parquet(self, json_input_path, parquet_output_path):
        """
        Convert JSON content to parquet format.
        
        Args:
            json_input_content: JSON content to convert
            parquet_output_path (str): Path to save parquet file
        """
        if self.log:
            self.log.info(f"json_input_path = {json_input_path}") 
            self.log.info(f"parquet_output_path = {parquet_output_path}") 
        
        with io.open(json_input_path, 'r', encoding='utf-8') as input_file:
            file_content = input_file.read()
            json_content = json.loads(file_content)
            df = pd.DataFrame(json_content)
            
            # Only convert dt column to date if it exists
            if 'dt' in df.columns:
                df['dt'] = pd.to_datetime(df['dt']).dt.date
            
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_output_path)
    
    def upload_to_gcs(self, gcs_bucket: str, prefix: str, blob_name, contents) -> str:
        """
        Upload JSON data to Google Cloud Storage.
        
        Args:
            gcs_bucket (str): The GCS bucket name
            blob_name (str): The path and name of the blob in GCS
            contents: The data to upload
            
        Returns:
            str: Full GCS path of the uploaded file
        """
        
        prefix = f"{prefix}/{get_hive_partition_prefix_str(self.partition_date)}/{blob_name}"
        
        if self.log:
            self.log.info(f"Uploading to GCS: gs://{gcs_bucket}/{prefix}")
            
        self.gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=prefix,
            data=json.dumps(contents, indent=2),
            mime_type='application/json'
        )
        
        return f"gs://{gcs_bucket}/{prefix}"
