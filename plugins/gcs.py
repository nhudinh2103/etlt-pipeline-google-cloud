from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json

def upload_to_gcs(gcs_bucket: str, blob_name: str, contents):
    """
    Upload JSON data to Google Cloud Storage using GCSHook.   
        
    Returns:
        None
    """
    # Initialize GCS client with optional service account key
    hook_args = {'gcp_conn_id': 'google_cloud_default'}
    
    gcs_hook = GCSHook(**hook_args)
    
    # Upload JSON data
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=blob_name,
        data=json.dumps(contents, indent=2),
        mime_type='application/json'
    )
