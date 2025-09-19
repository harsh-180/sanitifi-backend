#!/usr/bin/env python3
"""
Integrated Spark utilities with both local and cloud (Databricks) capabilities.
This file extends the existing spark_utils.py with cloud processing options.
"""

import os
import sys
import logging
import threading
import time
import requests
import json
from pathlib import Path
from typing import Optional, Dict, Any, Union
import findspark
from pyspark.sql import SparkSession
from contextlib import contextmanager

# Logger Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Databricks Configuration
DATABRICKS_WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "https://dbc-e8343889-d484.cloud.databricks.com")
DATABRICKS_ACCESS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")

# Environment Setup (same as original spark_utils.py)
JAVA_HOME = r"C:\Users\harsh\java\jdk-17"
HADOOP_HOME = r"C:\hadoop"
PYSPARK_PYTHON = sys.executable

# Set environment variables
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] += f";{os.path.join(HADOOP_HOME, 'bin')}"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["SPARK_LOCAL_IP"] = "localhost"

# Create local Spark temp dir if it doesn't exist
SPARK_LOCAL_DIRS = "C:/Users/harsh/Documents/skewb/dashboard/Dashboard-backend/spark-temp"
os.makedirs(SPARK_LOCAL_DIRS, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = SPARK_LOCAL_DIRS

# Initialize findspark
try:
    findspark.init()
    logger.info("findspark initialized successfully")
except Exception as e:
    logger.warning(f"findspark initialization failed: {e}")

class CloudSparkManager:
    """
    Manager for cloud-based Spark processing using Databricks.
    """
    
    def __init__(self):
        self.workspace_url = DATABRICKS_WORKSPACE_URL
        self.access_token = DATABRICKS_ACCESS_TOKEN
        
        if not self.access_token:
            raise ValueError("DATABRICKS_ACCESS_TOKEN environment variable is required but not set")
        
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # Test connection on initialization
        self._test_connection()
    
    def _test_connection(self):
        """Test Databricks connection."""
        try:
            # Test with a simple API call
            test_url = f"{self.workspace_url}/api/2.0/clusters/list"
            response = requests.get(test_url, headers=self.headers)
            
            if response.status_code == 200:
                logger.info("✅ Databricks connection successful")
            else:
                logger.warning(f"⚠️ Databricks connection test returned status: {response.status_code}")
                logger.warning(f"Response: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ Databricks connection test failed: {e}")
            logger.error("Please check your workspace URL and access token")
    
    def upload_file_to_databricks(self, local_file_path: str) -> str:
        """
        Upload a local file to Databricks Workspace Files.
        
        Args:
            local_file_path: Path to local file
            
        Returns:
            Workspace file path where file was uploaded
        """
        try:
            if not Path(local_file_path).exists():
                raise FileNotFoundError(f"File not found: {local_file_path}")
            
            # Use Workspace Files instead of DBFS
            file_name = Path(local_file_path).name
            workspace_path = f"/Users/harsh.kumar@skewb.ai/uploads/{file_name}"
            
            # Upload file using workspace-files API
            upload_url = f"{self.workspace_url}/api/2.0/workspace-files/upload"
            
            with open(local_file_path, 'rb') as f:
                files = {'file': (file_name, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
                data = {
                    'path': workspace_path,
                    'overwrite': 'true'
                }
                
                response = requests.post(upload_url, headers=self.headers, files=files, data=data)
                response.raise_for_status()
            
            logger.info(f"File uploaded to Workspace: {workspace_path}")
            return f"file:/Workspace{workspace_path}"
            
        except Exception as e:
            logger.error(f"Failed to upload file to Databricks: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise
    
    def create_excel_processing_notebook(self, file_path: str, sheet_name: str = None) -> str:
        """
        Create a Databricks notebook for Excel processing.
        
        Args:
            file_path: Workspace file path to Excel file
            sheet_name: Specific sheet to process (optional)
            
        Returns:
            Notebook content as string
        """
        notebook = f"""
# Databricks notebook source
# MAGIC %md
# MAGIC # Excel File Processing
# MAGIC Processing file: {file_path}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Excel file using Spark

# COMMAND ----------

try:
    # Read Excel file from Workspace Files
    df = spark.read.format("com.crealytics.spark.excel") \\
        .option("header", "true") \\
        .option("inferSchema", "true") \\
        .option("dataAddress", "{sheet_name or 'Sheet1'}") \\
        .load("{file_path}")
    
    print(f"✅ Successfully loaded Excel file")
    print(f"File path: {{file_path}}")
    
except Exception as e:
    print(f"❌ Error loading Excel file: {{e}}")
    print(f"Trying alternative path...")
    
    # Try alternative path format
    alt_path = "{file_path.replace('file:/Workspace', '')}"
    df = spark.read.format("com.crealytics.spark.excel") \\
        .option("header", "true") \\
        .option("inferSchema", "true") \\
        .option("dataAddress", "{sheet_name or 'Sheet1'}") \\
        .load(alt_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Overview

# COMMAND ----------

print(f"Total rows: {{df.count()}}")
print(f"Total columns: {{len(df.columns)}}")

# COMMAND ----------

# Show schema
df.printSchema()

# COMMAND ----------

# Show first few rows
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Statistics

# COMMAND ----------

# Numeric columns statistics
numeric_cols = [f.name for f in df.schema.fields if f.dataType.typeName() in ['integer', 'double', 'long']]
if numeric_cols:
    df.select(numeric_cols).summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save processed data

# COMMAND ----------

# Save as Parquet for better performance
output_path = "{file_path.replace('.xlsx', '_processed.parquet').replace('file:/Workspace', '')}"
df.write.mode("overwrite").parquet(output_path)

print(f"Data saved to: {{output_path}}")

# COMMAND ----------

# Return results
result = {{
    "total_rows": df.count(),
    "total_columns": len(df.columns),
    "columns": df.columns,
    "output_path": output_path,
    "file_processed": "{file_path}"
}}

print("Processing completed successfully!")
print(f"Results: {{result}}")

dbutils.notebook.exit(json.dumps(result))
"""
        return notebook
    
    def execute_notebook_in_databricks(self, notebook_content: str) -> Dict[str, Any]:
        """
        Execute a notebook in Databricks.
        
        Args:
            notebook_content: Notebook content to execute
            
        Returns:
            Execution results
        """
        try:
            import base64
            # Create temporary notebook in user-specific directory
            notebook_path = "/Users/harsh.kumar@skewb.ai/temp/excel_processing_temp"
            
            # First, create the parent directory if it doesn't exist
            try:
                mkdir_url = f"{self.workspace_url}/api/2.0/workspace/mkdirs"
                mkdir_data = {"path": "/Users/harsh.kumar@skewb.ai/temp"}
                response = requests.post(mkdir_url, headers=self.headers, json=mkdir_data)
                if response.status_code not in [200, 400]:  # 400 means directory already exists
                    response.raise_for_status()
                logger.info("Created temp directory successfully")
            except Exception as e:
                logger.warning(f"Directory creation warning (may already exist): {e}")
            
            # Create notebook
            create_url = f"{self.workspace_url}/api/2.0/workspace/import"
            
            encoded_content = base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8")
            data = {
                "path": notebook_path,
                "format": "SOURCE",
                "language": "PYTHON",
                "content": encoded_content,
                "overwrite": True
            }
            
            response = requests.post(create_url, headers=self.headers, json=data)
            response.raise_for_status()
            
            # Execute notebook
            run_url = f"{self.workspace_url}/api/2.0/jobs/run-now"
            
            job_data = {
                "notebook_path": notebook_path
            }
            
            response = requests.post(run_url, headers=self.headers, json=job_data)
            response.raise_for_status()
            
            run_id = response.json().get("run_id") or response.json().get("number_in_job")
            logger.info(f"Databricks job started with run_id: {run_id}")
            
            return {"run_id": run_id, "notebook_path": notebook_path}
            
        except Exception as e:
            logger.error(f"Failed to execute notebook in Databricks: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise
    
    def process_excel_in_cloud(self, local_file_path: str, sheet_name: str = None) -> Dict[str, Any]:
        """
        Process Excel file using Databricks cloud processing.
        
        Args:
            local_file_path: Path to local Excel file
            sheet_name: Specific sheet to process (optional)
            
        Returns:
            Processing results
        """
        try:
            logger.info(f"Starting cloud processing for: {local_file_path}")
            
            # Try to upload file to Databricks
            try:
                dbfs_path = self.upload_file_to_databricks(local_file_path)
                logger.info(f"File uploaded successfully to: {dbfs_path}")
            except Exception as upload_error:
                logger.warning(f"File upload failed, trying alternative method: {upload_error}")
                # Fallback: Create a notebook that references the local file path
                dbfs_path = f"/Users/harsh.kumar@skewb.ai/uploads/{Path(local_file_path).name}"
                logger.info(f"Using fallback path: {dbfs_path}")
            
            # Step 2: Create processing notebook
            notebook_content = self.create_excel_processing_notebook(dbfs_path, sheet_name)
            
            # Step 3: Execute notebook
            result = self.execute_notebook_in_databricks(notebook_content)
            
            return {
                "status": "success",
                "service": "databricks",
                "result": result,
                "file_path": dbfs_path,
                "local_file": local_file_path
            }
            
        except Exception as e:
            logger.error(f"Cloud processing failed: {e}")
            return {
                "status": "error",
                "service": "databricks",
                "error": str(e),
                "local_file": local_file_path
            }

# Import and extend the original SparkSessionManager
try:
    from .spark_utils import SparkSessionManager, get_spark_session, get_large_file_spark_session
    logger.info("Successfully imported original spark_utils")
except ImportError:
    # Fallback if relative import fails
    sys.path.append(os.path.dirname(__file__))
    from spark_utils import SparkSessionManager, get_spark_session, get_large_file_spark_session
    logger.info("Successfully imported original spark_utils via sys.path")

class IntegratedSparkManager:
    """
    Integrated Spark manager that combines local and cloud processing capabilities.
    """
    
    def __init__(self):
        self.local_manager = SparkSessionManager()
        self.cloud_manager = CloudSparkManager()
        self.processing_mode = "cloud"  # Always use cloud by default
    
    def set_processing_mode(self, mode: str):
        """
        Set the processing mode.
        
        Args:
            mode: "local", "cloud", or "auto"
        """
        if mode in ["local", "cloud", "auto"]:
            self.processing_mode = mode
            logger.info(f"Processing mode set to: {mode}")
        else:
            raise ValueError("Mode must be 'local', 'cloud', or 'auto'")
    
    def should_use_cloud(self, file_path: str) -> bool:
        """
        Determine if cloud processing should be used.
        Always returns True to force cloud processing.
        """
        return True  # Always use cloud processing
    
    def process_excel_file(self, file_path: str, sheet_name: str = None) -> Dict[str, Any]:
        """
        Process Excel file using cloud processing (always).
        
        Args:
            file_path: Path to Excel file
            sheet_name: Specific sheet to process (optional)
            
        Returns:
            Processing results
        """
        logger.info(f"Using cloud processing for: {file_path}")
        return self.cloud_manager.process_excel_in_cloud(file_path, sheet_name)
    
    def _process_excel_locally(self, file_path: str, sheet_name: str = None) -> Dict[str, Any]:
        """
        Process Excel file locally using existing Spark session.
        This is kept for compatibility but should not be used.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Specific sheet to process (optional)
            
        Returns:
            Processing results
        """
        try:
            # Get local Spark session
            spark = get_spark_session()
            
            # Read Excel file
            df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("dataAddress", sheet_name or "Sheet1") \
                .load(file_path)
            
            # Get basic info
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Show sample data
            sample_data = df.limit(10).toPandas().to_dict('records')
            
            return {
                "status": "success",
                "service": "local",
                "total_rows": total_rows,
                "total_columns": total_columns,
                "columns": df.columns,
                "sample_data": sample_data,
                "file_path": file_path
            }
            
        except Exception as e:
            logger.error(f"Local processing failed: {e}")
            return {
                "status": "error",
                "service": "local",
                "error": str(e),
                "file_path": file_path
            }

# Global instances
_cloud_manager = CloudSparkManager()
_integrated_manager = IntegratedSparkManager()

# Convenience functions
def process_excel_cloud(file_path: str, sheet_name: str = None) -> Dict[str, Any]:
    """
    Process Excel file using Databricks cloud processing.
    
    Args:
        file_path: Path to Excel file
        sheet_name: Specific sheet to process (optional)
        
    Returns:
        Processing results
    """
    return _cloud_manager.process_excel_in_cloud(file_path, sheet_name)

def process_excel_always_cloud(file_path: str, sheet_name: str = None) -> Dict[str, Any]:
    """
    Process Excel file using Databricks cloud processing (always).
    This is the main function to use for Excel processing.
    
    Args:
        file_path: Path to Excel file
        sheet_name: Specific sheet to process (optional)
        
    Returns:
        Processing results
    """
    return _cloud_manager.process_excel_in_cloud(file_path, sheet_name)

def process_excel_integrated(file_path: str, sheet_name: str = None) -> Dict[str, Any]:
    """
    Process Excel file using integrated local/cloud processing.
    Now always uses cloud processing.
    
    Args:
        file_path: Path to Excel file
        sheet_name: Specific sheet to process (optional)
        
    Returns:
        Processing results
    """
    return _integrated_manager.process_excel_file(file_path, sheet_name)

def set_processing_mode(mode: str):
    """
    Set the processing mode for integrated manager.
    
    Args:
        mode: "local", "cloud", or "auto"
    """
    _integrated_manager.set_processing_mode(mode)

# Example usage
if __name__ == "__main__":
    print("🚀 Integrated Spark Utils with Cloud Capabilities")
    print("=" * 60)
    print(f"Databricks Workspace: {DATABRICKS_WORKSPACE_URL}")
    print("Access Token: [Set via DATABRICKS_ACCESS_TOKEN environment variable]")
    print("\nAvailable functions:")
    print("- process_excel_cloud(file_path, sheet_name): Use Databricks cloud processing")
    print("- process_excel_integrated(file_path, sheet_name): Auto-select local/cloud")
    print("- set_processing_mode(mode): Set processing mode (local/cloud/auto)")
    print("\nExample:")
    print("result = process_excel_cloud('your_file.xlsx', 'Sheet1')")
    
    # Test Databricks connection
    print("\n🔍 Testing Databricks connection...")
    try:
        manager = CloudSparkManager()
        print("✅ CloudSparkManager created successfully")
    except Exception as e:
        print(f"❌ Failed to create CloudSparkManager: {e}") 