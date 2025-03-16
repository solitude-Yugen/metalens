import json
import os
import boto3
from io import BytesIO
import pandas as pd
from urllib.parse import urlparse

def analyze_iceberg_metadata(metadata_file, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Analyze Iceberg metadata file to extract table information
    
    Args:
        metadata_file: Path to metadata.json file or S3 URI
        is_local: Boolean indicating if the file is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        dict: Dictionary containing parsed metadata information
    """
    try:
        # Read the metadata file
        if is_local:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
        else:
            # Parse S3 URI
            parsed_url = urlparse(metadata_file)
            bucket = parsed_url.netloc
            key = parsed_url.path.lstrip('/')
            
            # Initialize S3 client
            session_kwargs = {}
            if aws_access_key and aws_secret_key:
                session_kwargs["aws_access_key_id"] = aws_access_key
                session_kwargs["aws_secret_access_key"] = aws_secret_key
                
            s3_client = boto3.client('s3', **session_kwargs)
            
            # Get the object
            response = s3_client.get_object(Bucket=bucket, Key=key)
            metadata = json.loads(response['Body'].read().decode('utf-8'))
        
        # Extract schema information
        schema_fields = []
        partition_fields = []
        
        if 'current-schema' in metadata:
            schema = metadata['current-schema']
            for field in schema.get('fields', []):
                schema_fields.append({
                    'name': field.get('name'),
                    'type': field.get('type'),
                    'required': field.get('required', False)
                })
        
        if 'partition-spec' in metadata:
            partition_spec = metadata['partition-spec']
            for field in partition_spec.get('fields', []):
                partition_fields.append(field.get('name'))
        
        # Create schema DataFrame
        schema_df = pd.DataFrame({
            "Column": [field['name'] for field in schema_fields],
            "Type": [field['type'] for field in schema_fields],
            "Partition": ["Yes" if field['name'] in partition_fields else "No" for field in schema_fields]
        })
        
        # Extract table properties
        properties = metadata.get('properties', {})
        
        # Extract snapshots information
        snapshots = metadata.get('snapshots', [])
        snapshots_df = pd.DataFrame([{
            'version': snap.get('snapshot-id'),
            'timestamp': snap.get('timestamp-ms'),
            'operation': snap.get('operation')
        } for snap in snapshots]) if snapshots else pd.DataFrame()
        
        # Extract summary information
        format_version = metadata.get('format-version', 'Unknown')
        
        # Parse manifest lists to get file statistics
        total_files = 0
        total_size = 0
        manifest_lists = metadata.get('manifest-lists', [])
        
        # We'd need to parse each manifest list and manifest file for complete stats
        # This is simplified for the prototype
        
        return {
            "schema": schema_df,
            "properties": {
                "format": "Iceberg",
                "format_version": format_version,
                "last_updated": properties.get('last-updated-ms', 'Unknown'),
                "location": metadata.get('location', 'Unknown'),
                "table_uuid": metadata.get('table-uuid', 'Unknown')
            },
            "statistics": {
                "files": total_files,
                "size": "Unknown",  # Need to read manifests for this
                "rows": "Unknown",  # Need to read manifests for this
                "partitions": len(partition_fields)
            },
            "versions": snapshots_df.to_dict('records') if not snapshots_df.empty else [],
            "partitions": partition_fields
        }
    except Exception as e:
        raise Exception(f"Error analyzing Iceberg metadata: {str(e)}")

def get_iceberg_snapshots(metadata_file, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Extract snapshot information from Iceberg metadata
    
    Args:
        metadata_file: Path to metadata.json file or S3 URI
        is_local: Boolean indicating if the file is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        list: List of snapshots with their details
    """
    metadata = analyze_iceberg_metadata(metadata_file, is_local, aws_access_key, aws_secret_key)
    return metadata.get("versions", [])

def get_iceberg_partition_data(metadata_file, is_local=True, aws_access_key=None, aws_secret_key=None):
    """
    Extract partition information from Iceberg metadata
    
    Args:
        metadata_file: Path to metadata.json file or S3 URI
        is_local: Boolean indicating if the file is local or on S3
        aws_access_key: Optional AWS access key for S3 access
        aws_secret_key: Optional AWS secret key for S3 access
        
    Returns:
        dict: Partition statistics and information
    """
    # This would require parsing manifests to get detailed partition statistics
    # For the prototype, we return basic information
    metadata = analyze_iceberg_metadata(metadata_file, is_local, aws_access_key, aws_secret_key)
    return {
        "partition_columns": metadata.get("partitions", []),
        "partition_count": len(metadata.get("partitions", []))
    }