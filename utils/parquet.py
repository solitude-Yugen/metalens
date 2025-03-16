import pyarrow.parquet as pq
import pandas as pd
import os
import boto3
from io import BytesIO

def analyze_local_parquet(file_path):
    """
    Analyze a local parquet file to extract metadata
    
    Args:
        file_path: Path to local parquet file
        
    Returns:
        dict: Dictionary containing schema, statistics and sample data
    """
    try:
        # Read parquet file metadata
        parquet_file = pq.ParquetFile(file_path)
        
        # Extract schema
        schema = parquet_file.schema.to_arrow_schema()
        schema_df = pd.DataFrame({
            "Column": [field.name for field in schema],
            "Type": [str(field.type) for field in schema],
            "Partition": ["No"] * len(schema)  # Parquet itself doesn't encode partition info
        })
        
        # Extract statistics
        metadata = parquet_file.metadata
        num_rows = metadata.num_rows
        num_columns = metadata.num_columns
        
        # Calculate total size
        file_size = os.path.getsize(file_path)
        if file_size > 1_000_000_000:
            size_str = f"{file_size / 1_000_000_000:.2f} GB"
        else:
            size_str = f"{file_size / 1_000_000:.2f} MB"
            
        # Extract preview data (first 10 rows)
        preview_data = next(parquet_file.iter_batches(batch_size=10)).to_pandas()
        
        return {
            "schema": schema_df,
            "properties": {
                "format": "Parquet",
                "created_by": metadata.created_by if metadata.created_by else "Unknown",
                "num_columns": num_columns,
                "num_row_groups": metadata.num_row_groups
            },
            "statistics": {
                "files": 1,  # Just counting the current file
                "size": size_str,
                "rows": f"{num_rows:,}",
                "partitions": 0  # Single file has no partitions
            },
            "preview_data": preview_data
        }
    except Exception as e:
        raise Exception(f"Error analyzing Parquet file: {str(e)}")

def analyze_s3_parquet(s3_path, aws_access_key=None, aws_secret_key=None, aws_session_token=None):
    """
    Analyze a Parquet file or dataset from S3
    
    Args:
        s3_path: S3 URI (s3://bucket/path)
        aws_access_key: Optional AWS access key
        aws_secret_key: Optional AWS secret key
        aws_session_token: Optional AWS session token
        
    Returns:
        dict: Dictionary containing schema, statistics and sample data
    """
    try:
        # Parse S3 URI
        if not s3_path.startswith('s3://'):
            raise ValueError("Invalid S3 URI. Must start with s3://")
            
        s3_parts = s3_path[5:].split('/', 1)
        bucket = s3_parts[0]
        key_prefix = s3_parts[1] if len(s3_parts) > 1 else ''
        
        # Initialize S3 client
        session_kwargs = {}
        if aws_access_key and aws_secret_key:
            session_kwargs['aws_access_key_id'] = aws_access_key
            session_kwargs['aws_secret_access_key'] = aws_secret_key
        if aws_session_token:
            session_kwargs['aws_session_token'] = aws_session_token
            
        session = boto3.Session(**session_kwargs)
        s3_client = session.client('s3')
        
        # List objects to find parquet files
        parquet_files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        parquet_files.append(obj)
        
        if not parquet_files:
            raise ValueError(f"No Parquet files found at {s3_path}")
        
        # Get the first file for schema and sample data
        first_key = parquet_files[0]['Key']
        obj = s3_client.get_object(Bucket=bucket, Key=first_key)
        buffer = BytesIO(obj['Body'].read())
        
        # Read parquet file
        parquet_file = pq.ParquetFile(buffer)
        
        # Extract schema
        schema = parquet_file.schema.to_arrow_schema()
        schema_df = pd.DataFrame({
            "Column": [field.name for field in schema],
            "Type": [str(field.type) for field in schema],
            "Partition": ["No"] * len(schema)  # Will need to infer partitioning from path patterns
        })
        
        # Extract basic statistics
        metadata = parquet_file.metadata
        
        # Calculate total size of all files
        total_size = sum(obj['Size'] for obj in parquet_files)
        if total_size > 1_000_000_000:
            size_str = f"{total_size / 1_000_000_000:.2f} GB"
        else:
            size_str = f"{total_size / 1_000_000:.2f} MB"
            
        # Extract preview data
        preview_data = next(parquet_file.iter_batches(batch_size=10)).to_pandas()
        
        # Try to infer partitioning from file paths
        partition_cols = infer_partitions_from_paths(parquet_files)
        
        # Update the partition flag in schema
        for col in partition_cols:
            if col in schema_df['Column'].values:
                schema_df.loc[schema_df['Column'] == col, 'Partition'] = 'Yes'
        
        return {
            "schema": schema_df,
            "properties": {
                "format": "Parquet",
                "created_by": metadata.created_by if metadata.created_by else "Unknown",
                "location": s3_path,
                "num_columns": metadata.num_columns
            },
            "statistics": {
                "files": len(parquet_files),
                "size": size_str,
                "rows": "Unknown",  # Would need to read all files to get accurate count
                "partitions": len(partition_cols)
            },
            "preview_data": preview_data,
            "partitions": partition_cols
        }
    except Exception as e:
        raise Exception(f"Error analyzing S3 Parquet dataset: {str(e)}")

def infer_partitions_from_paths(file_objects):
    """
    Infer partition columns from S3 object paths
    
    Args:
        file_objects: List of S3 objects
        
    Returns:
        list: Inferred partition columns
    """
    # Extract just the keys
    paths = [obj['Key'] for obj in file_objects]
    
    # Common pattern is path/to/table/col1=val1/col2=val2/file.parquet
    partition_cols = set()
    
    for path in paths:
        parts = path.split('/')
        for part in parts:
            if '=' in part:
                col_name = part.split('=')[0]
                partition_cols.add(col_name)
    
    return list(partition_cols)