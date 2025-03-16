import json
import os
import boto3
from io import BytesIO
import pandas as pd
from urllib.parse import urlparse
import pyarrow.parquet as pq

def analyze_hudi_metadata(path, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Analyze Hudi table metadata to extract table information
    
    Args:
        path: Path to Hudi table directory or S3 URI
        is_local: Boolean indicating if the path is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        dict: Dictionary containing parsed metadata information
    """
    try:
        # Look for .hoodie directory containing metadata
        hoodie_dir = os.path.join(path, '.hoodie') if is_local else f"{path.rstrip('/')}/.hoodie"
        
        if is_local:
            # For local files
            if not os.path.isdir(hoodie_dir):
                raise ValueError(f"Hudi metadata directory {hoodie_dir} does not exist")
                
            # Check for hoodie.properties
            properties_file = os.path.join(hoodie_dir, 'hoodie.properties')
            if not os.path.isfile(properties_file):
                raise ValueError(f"Hudi properties file not found at {properties_file}")
                
            # Read properties file
            properties = {}
            with open(properties_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        properties[key.strip()] = value.strip()
            
            # Look for schema in .hoodie/schema
            schema_file = os.path.join(hoodie_dir, 'schema')
            schema = {}
            if os.path.isfile(schema_file):
                with open(schema_file, 'r') as f:
                    schema = json.load(f)
            
            # Look for commit metadata
            commits_dir = os.path.join(hoodie_dir, 'commits')
            commits = []
            if os.path.isdir(commits_dir):
                commit_files = sorted([f for f in os.listdir(commits_dir) if f.endswith('.commit')])
                for commit_file in commit_files[-10:]:  # Get last 10 commits
                    with open(os.path.join(commits_dir, commit_file), 'r') as f:
                        commit_data = json.load(f)
                        commits.append(commit_data)
        else:
            # For S3 path
            parsed_url = urlparse(path)
            bucket = parsed_url.netloc
            prefix = parsed_url.path.lstrip('/')
            hoodie_prefix = f"{prefix.rstrip('/')}/.hoodie/"

            # Initialize S3 client
            session_kwargs = {}
            if aws_access_key and aws_secret_key:
                session_kwargs["aws_access_key_id"] = aws_access_key
                session_kwargs["aws_secret_access_key"] = aws_secret_key
                
            s3_client = boto3.client('s3', **session_kwargs)
            
            # Check for hoodie.properties
            properties = {}
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=f"{hoodie_prefix}hoodie.properties")
                content = obj['Body'].read().decode('utf-8')
                for line in content.splitlines():
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        properties[key.strip()] = value.strip()
            except s3_client.exceptions.NoSuchKey:
                raise ValueError(f"Hudi properties file not found at s3://{bucket}/{hoodie_prefix}hoodie.properties")
            
            # Look for schema
            schema = {}
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=f"{hoodie_prefix}schema")
                content = obj['Body'].read().decode('utf-8')
                schema = json.loads(content)
            except s3_client.exceptions.NoSuchKey:
                # Schema might not be present
                pass
            
            # Look for commit metadata
            commits = []
            commits_prefix = f"{hoodie_prefix}commits/"
            try:
                # List commit files
                commit_files = []
                paginator = s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=commits_prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if obj['Key'].endswith('.commit'):
                                commit_files.append(obj['Key'])
                
                # Sort and get last 10
                commit_files.sort()
                for commit_file in commit_files[-10:]:
                    obj = s3_client.get_object(Bucket=bucket, Key=commit_file)
                    content = obj['Body'].read().decode('utf-8')
                    commit_data = json.loads(content)
                    commits.append(commit_data)
            except Exception:
                # Commits might not be found
                pass
        
        # Parse schema information
        schema_fields = []
        if schema and 'fields' in schema:
            for field in schema['fields']:
                schema_fields.append({
                    'name': field.get('name'),
                    'type': field.get('type'),
                    'doc': field.get('doc', '')
                })
        
        # Get partition fields
        partition_fields = []
        partition_path_field = properties.get('hoodie.table.partition.fields')
        if partition_path_field:
            partition_fields = partition_path_field.split(',')
        
        # Create schema DataFrame
        schema_df = pd.DataFrame({
            "Column": [field['name'] for field in schema_fields],
            "Type": [field['type'] for field in schema_fields],
            "Partition": ["Yes" if field['name'] in partition_fields else "No" for field in schema_fields]
        })
        
        # Extract version history
        versions = []
        for commit in commits:
            versions.append({
                'version': commit.get('commitTime'),
                'timestamp': commit.get('commitTime'),
                'operation': commit.get('operationType', 'Unknown')
            })
        
        # Extract table properties
        table_properties = {
            'format': 'Hudi',
            'table_type': properties.get('hoodie.table.type', 'Unknown'),
            'table_name': properties.get('hoodie.table.name', 'Unknown'),
            'base_path': path,
            'compaction_strategy': properties.get('hoodie.compaction.strategy', 'Unknown'),
            'archiving_enabled': properties.get('hoodie.archive.enabled', 'Unknown')
        }
        
        # Try to extract file statistics
        # This would be more accurate by parsing the file listing
        total_files = 0
        total_size = 0
        total_rows = 0
        
        # For a simple implementation, we'll try to extract some stats from the commit metadata
        for commit in commits:
            if 'fileAdded' in commit:
                total_files += len(commit['fileAdded'])
            if 'recordsWritten' in commit:
                total_rows += commit.get('recordsWritten', 0)
        
        # Look for sample data file to provide preview
        preview_data = pd.DataFrame()
        try:
            if is_local:
                # Search for a parquet file in the table directory
                for root, dirs, files in os.walk(path):
                    for file in files:
                        if file.endswith('.parquet'):
                            # Found a parquet file, read for preview
                            parquet_path = os.path.join(root, file)
                            parquet_file = pq.ParquetFile(parquet_path)
                            preview_data = next(parquet_file.iter_batches(batch_size=10)).to_pandas()
                            break
                    if not preview_data.empty:
                        break
            else:
                # For S3, list objects to find a parquet file
                paginator = s3_client.get_paginator('list_objects_v2')
                found_parquet = False
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix, MaxItems=100):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if obj['Key'].endswith('.parquet'):
                                # Found a parquet file
                                obj_response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                                buffer = BytesIO(obj_response['Body'].read())
                                parquet_file = pq.ParquetFile(buffer)
                                preview_data = next(parquet_file.iter_batches(batch_size=10)).to_pandas()
                                found_parquet = True
                                break
                    if found_parquet:
                        break
        except Exception:
            # If can't read preview data, continue without it
            pass
        
        return {
            "schema": schema_df,
            "properties": table_properties,
            "statistics": {
                "files": total_files if total_files > 0 else "Unknown",
                "size": "Unknown",  # Would need to scan all files for accurate size
                "rows": f"{total_rows:,}" if total_rows > 0 else "Unknown",
                "partitions": len(partition_fields)
            },
            "versions": versions,
            "partitions": partition_fields,
            "preview_data": preview_data if not preview_data.empty else None
        }
    except Exception as e:
        raise Exception(f"Error analyzing Hudi metadata: {str(e)}")

def get_hudi_commits(path, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Extract commit information from Hudi metadata
    
    Args:
        path: Path to Hudi table directory or S3 URI
        is_local: Boolean indicating if the path is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        list: List of commits with their details
    """
    metadata = analyze_hudi_metadata(path, is_local, aws_access_key, aws_secret_key)
    return metadata.get("versions", [])

def get_hudi_partition_data(path, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Extract partition information from Hudi metadata
    
    Args:
        path: Path to Hudi table directory or S3 URI
        is_local: Boolean indicating if the path is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        dict: Partition statistics and information
    """
    metadata = analyze_hudi_metadata(path, is_local, aws_access_key, aws_secret_key)
    return {
        "partition_columns": metadata.get("partitions", []),
        "partition_count": len(metadata.get("partitions", [])),
        "table_type": metadata.get("properties", {}).get("table_type", "Unknown")
    }