import streamlit as st
import pandas as pd
import json
import os
import boto3
import tempfile
import pyarrow.parquet as pq
from io import BytesIO
from urllib.parse import urlparse
import deltalake
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
import hudi

# Set page configuration
st.set_page_config(
    page_title="MetaLens - Metastore Viewer",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS to improve the UI appearance
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #4285F4;
        padding-bottom: 1rem;
        text-align: center;
    }
    .section-header {
        font-size: 1.5rem;
        padding-top: 1rem;
        padding-bottom: 0.5rem;
        color: #4285F4;
    }
    .info-box {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
    }
    .stButton > button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("<div class='main-header'>MetaLens - Metastore Viewer</div>", unsafe_allow_html=True)

# Initialize session state variables if they don't exist
if 'table_schema' not in st.session_state:
    st.session_state.table_schema = None
if 'table_properties' not in st.session_state:
    st.session_state.table_properties = {}
if 'table_statistics' not in st.session_state:
    st.session_state.table_statistics = {}
if 'preview_data' not in st.session_state:
    st.session_state.preview_data = None
if 'table_versions' not in st.session_state:
    st.session_state.table_versions = []
if 'table_format' not in st.session_state:
    st.session_state.table_format = None
if 'partitions' not in st.session_state:
    st.session_state.partitions = []

# Sidebar navigation
st.sidebar.title("Navigation")
selected_section = st.sidebar.radio(
    "Select View", 
    ["Schema Browser", "Partition Explorer", "Version History", "Data Preview", "SQL Query"]
)

# Source selection tabs at the top
source_tab = st.radio("Select Data Source", ["S3/Cloud URL", "Local Upload"], horizontal=True)

if source_tab == "S3/Cloud URL":
    # S3/Cloud connector 
    col1, col2 = st.columns([3, 1])
    with col1:
        s3_path = st.text_input("S3 Path", placeholder="s3://example-bucket/path/to/table")
    with col2:
        if st.button("Connect", key="connect_s3"):
            try:
                with st.spinner("Connecting to S3 and loading metadata..."):
                    # Placeholder for the actual connection logic
                    if "s3://" in s3_path:
                        # Detect table format based on files or metadata
                        if "iceberg" in s3_path.lower() or "metadata/v" in s3_path:
                            st.session_state.table_format = "Iceberg"
                        elif "delta" in s3_path.lower() or "_delta_log" in s3_path:
                            st.session_state.table_format = "Delta"
                        elif "hudi" in s3_path.lower() or ".hoodie" in s3_path:
                            st.session_state.table_format = "Hudi"
                        else:
                            st.session_state.table_format = "Parquet"
                        
                        # Mock data for prototype
                        st.session_state.table_schema = pd.DataFrame({
                            "Column": ["order_id", "customer_id", "order_date", "amount", "status"],
                            "Type": ["string", "string", "date", "decimal(10,2)", "string"],
                            "Partition": ["No", "No", "Yes", "No", "Yes"]
                        })
                        
                        st.session_state.table_properties = {
                            "format": st.session_state.table_format,
                            "last_updated": "2023-04-15",
                            "created_by": "user@example.com",
                            "description": "Customer orders table"
                        }
                        
                        st.session_state.table_statistics = {
                            "files": 237,
                            "size": "2.4 GB",
                            "rows": "~87M",
                            "partitions": 124
                        }
                        
                        # Mock preview data
                        st.session_state.preview_data = pd.DataFrame({
                            "order_id": ["ORD001", "ORD002", "ORD003"],
                            "customer_id": ["C123", "C456", "C789"],
                            "order_date": ["2023-01-15", "2023-02-20", "2023-03-05"],
                            "amount": [120.50, 89.99, 250.00],
                            "status": ["completed", "processing", "completed"]
                        })
                        
                        # Mock version history for Delta/Iceberg tables
                        st.session_state.table_versions = [
                            {"version": 5, "timestamp": "2023-04-15 14:30:22", "operation": "UPDATE"},
                            {"version": 4, "timestamp": "2023-04-10 09:15:45", "operation": "INSERT"},
                            {"version": 3, "timestamp": "2023-03-28 16:42:11", "operation": "DELETE"},
                        ]
                        
                        # Mock partition information
                        st.session_state.partitions = [
                            {"value": "2023-01", "files": 42, "size": "350 MB"},
                            {"value": "2023-02", "files": 38, "size": "320 MB"},
                            {"value": "2023-03", "files": 45, "size": "380 MB"},
                            {"value": "2023-04", "files": 40, "size": "310 MB"},
                        ]
                        
                        st.success(f"Successfully connected to {st.session_state.table_format} table!")
                    else:
                        st.error("Please enter a valid S3 URI starting with s3://")
            except Exception as e:
                st.error(f"Error connecting to S3: {str(e)}")

else:  # Local Upload
    uploaded_file = st.file_uploader("Upload Parquet, Delta, Iceberg, or Hudi files", 
                                    type=["parquet", "json", "metadata"],
                                    accept_multiple_files=True)
    
    if uploaded_file and st.button("Process Files", key="process_local"):
        with st.spinner("Processing uploaded files..."):
            try:
                # For the prototype, we'll look at file extensions to determine format
                filenames = [f.name for f in uploaded_file] if isinstance(uploaded_file, list) else [uploaded_file.name]
                
                # Determine table format based on files
                if any("iceberg-metadata" in f.lower() for f in filenames) or any("metadata.json" in f.lower() for f in filenames):
                    st.session_state.table_format = "Iceberg"
                elif any("delta-log" in f.lower() for f in filenames) or any("_delta_log" in f.lower() for f in filenames):
                    st.session_state.table_format = "Delta"
                elif any(".hoodie" in f.lower() for f in filenames) or any("hudi" in f.lower() for f in filenames):
                    st.session_state.table_format = "Hudi"
                else:
                    st.session_state.table_format = "Parquet"
                    
                # For actual implementation, parse the first Parquet file to extract schema
                # Here we'll use mock data similar to the S3 example
                st.session_state.table_schema = pd.DataFrame({
                    "Column": ["order_id", "customer_id", "order_date", "amount", "status"],
                    "Type": ["string", "string", "date", "decimal(10,2)", "string"],
                    "Partition": ["No", "No", "Yes", "No", "Yes"]
                })
                
                st.session_state.table_properties = {
                    "format": st.session_state.table_format,
                    "last_updated": "2023-04-15",
                    "created_by": "user@example.com",
                    "description": "Customer orders table (local)"
                }
                
                st.session_state.table_statistics = {
                    "files": len(filenames),
                    "size": "1.2 GB",
                    "rows": "~45M",
                    "partitions": 62
                }
                
                # Mock preview data
                st.session_state.preview_data = pd.DataFrame({
                    "order_id": ["ORD001", "ORD002", "ORD003"],
                    "customer_id": ["C123", "C456", "C789"],
                    "order_date": ["2023-01-15", "2023-02-20", "2023-03-05"],
                    "amount": [120.50, 89.99, 250.00],
                    "status": ["completed", "processing", "completed"]
                })
                
                st.success(f"Successfully processed {st.session_state.table_format} files!")
                
            except Exception as e:
                st.error(f"Error processing files: {str(e)}")

# If we have schema data, display the appropriate section
if st.session_state.table_schema is not None:
    # Display table basic info
    if st.session_state.table_format:
        st.markdown(f"### Table: customer_orders ({st.session_state.table_format})")
        st.markdown(f"Last Updated: {st.session_state.table_properties['last_updated']}")
    
    # Display content based on selected section
    if selected_section == "Schema Browser":
        st.markdown("<div class='section-header'>Schema Browser</div>", unsafe_allow_html=True)
        st.dataframe(st.session_state.table_schema, use_container_width=True)
        
        # Display table statistics
        st.markdown("<div class='section-header'>Table Statistics</div>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Files", st.session_state.table_statistics["files"])
        with col2:
            st.metric("Size", st.session_state.table_statistics["size"])
        with col3:
            st.metric("Rows", st.session_state.table_statistics["rows"])
        with col4:
            st.metric("Partitions", st.session_state.table_statistics["partitions"])
            
        # Additional properties
        st.markdown("<div class='section-header'>Table Properties</div>", unsafe_allow_html=True)
        st.json(st.session_state.table_properties)
        
    elif selected_section == "Partition Explorer":
        st.markdown("<div class='section-header'>Partition Explorer</div>", unsafe_allow_html=True)
        
        # Show partition columns
        partition_cols = st.session_state.table_schema[st.session_state.table_schema["Partition"] == "Yes"]["Column"].tolist()
        st.markdown("**Partition Columns:** " + ", ".join(partition_cols))
        
        # Display partitions
        if st.session_state.partitions:
            st.dataframe(pd.DataFrame(st.session_state.partitions), use_container_width=True)
        else:
            st.info("No partition information available")
            
    elif selected_section == "Version History":
        st.markdown("<div class='section-header'>Version History</div>", unsafe_allow_html=True)
        if st.session_state.table_format in ["Iceberg", "Delta"]:
            if st.session_state.table_versions:
                versions_df = pd.DataFrame(st.session_state.table_versions)
                st.dataframe(versions_df, use_container_width=True)
                
                # Add version comparison feature (placeholder for now)
                col1, col2 = st.columns(2)
                with col1:
                    v1 = st.selectbox("Compare version", options=[v["version"] for v in st.session_state.table_versions], index=0)
                with col2:
                    v2 = st.selectbox("With version", options=[v["version"] for v in st.session_state.table_versions], index=min(1, len(st.session_state.table_versions)-1))
                
                if st.button("Compare Versions"):
                    st.info("Version comparison feature will be implemented in the next iteration.")
            else:
                st.info("No version history available")
        else:
            st.info(f"{st.session_state.table_format} format does not support version history")
            
    elif selected_section == "Data Preview":
        st.markdown("<div class='section-header'>Data Preview</div>", unsafe_allow_html=True)
        if st.session_state.preview_data is not None:
            st.dataframe(st.session_state.preview_data, use_container_width=True)
        else:
            st.info("No preview data available")
            
    elif selected_section == "SQL Query":
        st.markdown("<div class='section-header'>SQL Query</div>", unsafe_allow_html=True)
        query = st.text_area("Enter SQL Query", height=150, 
                            value=f"SELECT * FROM customer_orders LIMIT 10")
        
        if st.button("Execute Query"):
            st.info("Query execution feature will be implemented with Trino integration in the next iteration.")
            st.dataframe(st.session_state.preview_data, use_container_width=True)

else:
    # Display initial instructions when no data is loaded
    st.markdown("""
    <div class='info-box'>
        <h3>Welcome to MetaLens!</h3>
        <p>Please connect to an S3 path or upload files to view table metadata.</p>
        <p>Supported formats:</p>
        <ul>
            <li>Apache Parquet</li>
            <li>Apache Iceberg</li>
            <li>Delta Lake</li>
            <li>Apache Hudi</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("MetaLens - A web-based metastore viewer for lakehouse formats | GitHub: [Add your repository link]")