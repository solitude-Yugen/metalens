import json
import os
import boto3
from io import BytesIO
import pandas as pd
from urllib.parse import urlparse
import pyarrow.parquet as pq

def analyze_delta_log(log_path, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Analyze Delta Lake _delta_log directory to extract table information
    
    Args:
        log_path: Path to _delta_log directory or S3 URI
        is_local: Boolean indicating if the path is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        dict: Dictionary containing parsed metadata information
    """
    try:
        if is_local:
            # For local files
            if not os.path.isdir(log_path):
                raise ValueError(f"Delta log path {log_path} is not a directory")
                
            # Read checkpoint and json files
            checkpoint_files = [f for f in os.listdir(log_path) if f.endswith('.checkpoint.parquet')]
            json_files = [f for f in os.listdir(log_path) if f.endswith('.json')]
            
            # Sort by version number
            checkpoint_files.sort(key=lambda x: int(x.split('.')[0]))
            json_files.sort(key=lambda x: int(x.split('.')[0]))
            
            # Read latest checkpoint if available, otherwise read json files
            schema = None
            actions = []
            
            if checkpoint_files:
                latest_checkpoint = os.path.join(log_path, checkpoint_files[-1])
                checkpoint_data = pq.read_table(latest_checkpoint)
                # Extract schema from checkpoint
                for row in checkpoint_data.to_pandas().itertuples():
                    if hasattr(row, 'add') and row.add:
                        add_data = json.loads(row.add)
                        if 'metaData' in add_data:
                            schema = add_data['metaData'].get('schema')
                    actions.append(row)
            else:
                # Read from JSON logs
                for json_file in json_files[-10:]:  # Read last 10 logs
                    with open(os.path.join(log_path, json_file), 'r') as f:
                        for line in f:
                            action = json.loads(line.strip())
                            actions.append(action)
                            if 'metaData' in action and schema is None:
                                schema = action['metaData'].get('schema')
        else:
            # For S3 path
            parsed_url = urlparse(log_path)
            bucket = parsed_url.netloc
            prefix = parsed_url.path.lstrip('/')
            
            # Initialize S3 client
            session_kwargs = {}
            if aws_access_key and aws_secret_key:
                session_kwargs["aws_access_key_id"] = aws_access_key
                session_kwargs["aws_secret_access_key"] = aws_secret_key
                
            s3_client = boto3.client('s3', **session_kwargs)
            
            # List objects to find checkpoint and json files
            checkpoint_files = []
            json_files = []
            
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.checkpoint.parquet'):
                            checkpoint_files.append(key)
                        elif key.endswith('.json'):
                            json_files.append(key)
            
            # Sort by version number
            checkpoint_files.sort(key=lambda x: int(os.path.basename(x).split('.')[0]))
            json_files.sort(key=lambda x: int(os.path.basename(x).split('.')[0]))
            
            # Read latest checkpoint if available, otherwise read json files
            schema = None
            actions = []
            
            if checkpoint_files:
                # Get the latest checkpoint
                latest_checkpoint = checkpoint_files[-1]
                obj = s3_client.get_object(Bucket=bucket, Key=latest_checkpoint)
                buffer = BytesIO(obj['Body'].read())
                
                checkpoint_data = pq.read_table(buffer)
                # Extract schema from checkpoint
                for row in checkpoint_data.to_pandas().itertuples():
                    if hasattr(row, 'add') and row.add:
                        add_data = json.loads(row.add)
                        if 'metaData' in add_data:
                            schema = add_data['metaData'].get('schema')
                    actions.append(row)
            else:
                # Read from JSON logs
                for json_file in json_files[-10:]:  # Read last 10 logs
                    obj = s3_client.get_object(Bucket=bucket, Key=json_file)
                    content = obj['Body'].read().decode('utf-8')
                    for line in content.splitlines():
                        action = json.loads(line.strip())
                        actions.append(action)
                        if 'metaData' in action and schema is None:
                            schema = action['metaData'].get('schema')
        
        # Parse schema information
        schema_fields = []
        partition_fields = []
        
        if schema:
            if 'fields' in schema:
                for field in schema['fields']:
                    schema_fields.append({
                        'name': field.get('name'),
                        'type': field.get('type'),
                        'nullable': field.get('nullable', True)
                    })
            
            # Find partitioning information
            for action in actions:
                if isinstance(action, dict) and 'metaData' in action:
                    partition_cols = action['metaData'].get('partitionColumns', [])
                    partition_fields = partition_cols
                    break
        
        # Create schema DataFrame
        schema_df = pd.DataFrame({
            "Column": [field['name'] for field in schema_fields],
            "Type": [field['type'] for field in schema_fields],
            "Partition": ["Yes" if field['name'] in partition_fields else "No" for field in schema_fields]
        })
        
        # Extract version history
        versions = []
        for action in actions:
            if isinstance(action, dict) and 'commitInfo' in action:
                commit_info = action['commitInfo']
                versions.append({
                    'version': commit_info.get('version'),
                    'timestamp': commit_info.get('timestamp'),
                    'operation': commit_info.get('operation')
                })
        
        # Extract statistics
        # This would ideally parse all add/remove actions to get file counts and sizes
        # For the prototype we use simplified stats
        
        return {
            "schema": schema_df,
            "properties": {
                "format": "Delta",
                "last_updated": versions[0]['timestamp'] if versions else "Unknown",
                "location": log_path.replace('/_delta_log', '')
            },
            "statistics": {
                "files": len([a for a in actions if isinstance(a, dict) and 'add' in a]),
                "size": "Unknown",  # Would need to parse add actions and sum sizes
                "rows": "Unknown",  # Would need to parse add actions and sum rows
                "partitions": len(partition_fields)
            },
            "versions": versions,
            "partitions": partition_fields
        }
    except Exception as e:
        raise Exception(f"Error analyzing Delta log: {str(e)}")

def get_delta_versions(log_path, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Extract version history from Delta log
    
    Args:
        log_path: Path to _delta_log directory or S3 URI
        is_local: Boolean indicating if the path is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        list: List of versions with their details
    """
    metadata = analyze_delta_log(log_path, is_local, aws_access_key, aws_secret_key)
    return metadata.get("versions", [])

def get_delta_partition_data(log_path, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Extract partition information from Delta log
    
    Args:
        log_path: Path to _delta_log directory or S3 URI
        is_local: Boolean indicating if the path is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        dict: Partition statistics and information
    """
    metadata = analyze_delta_log(log_path, is_local, aws_access_key, aws_secret_key)
    return {
        "partition_columns": metadata.get("partitions", []),
        "partition_count": len(metadata.get("partitions", []))
    }