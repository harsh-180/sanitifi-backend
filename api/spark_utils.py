import os
import sys
import logging
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any
import findspark
from pyspark.sql import SparkSession
from contextlib import contextmanager

# Logger Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Setup
import getpass
current_user = getpass.getuser()

# Try to detect Java installation paths
JAVA_HOME = None
HADOOP_HOME = None

# Common Java installation paths for Windows
java_paths = [
    f"C:\\Users\\{current_user}\\java\\jdk-17",
    f"C:\\Program Files\\Java\\jdk-17",
    f"C:\\Program Files\\Java\\jdk-11",
    f"C:\\Program Files\\Java\\jdk-8",
    "C:\\java\\jdk-17",
    "C:\\hadoop"  # Sometimes Java is bundled with Hadoop
]

# Find Java installation
for java_path in java_paths:
    if os.path.exists(java_path):
        JAVA_HOME = java_path
        break

# If no Java found, try environment variable
if not JAVA_HOME:
    JAVA_HOME = os.environ.get("JAVA_HOME", f"C:\\Users\\{current_user}\\java\\jdk-17")

# Hadoop paths
hadoop_paths = [
    "C:\\hadoop",
    f"C:\\Users\\{current_user}\\hadoop",
    "C:\\opt\\hadoop"
]

for hadoop_path in hadoop_paths:
    if os.path.exists(hadoop_path):
        HADOOP_HOME = hadoop_path
        break

if not HADOOP_HOME:
    HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:\\hadoop")

PYSPARK_PYTHON = sys.executable

# Set environment variables
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
if os.path.exists(HADOOP_HOME):
    os.environ["PATH"] += f";{os.path.join(HADOOP_HOME, 'bin')}"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["SPARK_LOCAL_IP"] = "localhost"

# --- Linux/Server Environment Setup ---
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
# os.environ["HADOOP_HOME"] = "/opt/hadoop"
# os.environ["SPARK_HOME"] = "/opt/spark"
# os.environ["PATH"] += f":/opt/hadoop/bin:/opt/spark/bin"
# os.environ["PYSPARK_PYTHON"] = sys.executable

# Create local Spark temp dir using current user's directory
SPARK_LOCAL_DIRS = os.path.join(os.path.expanduser("~"), "spark-temp")
os.makedirs(SPARK_LOCAL_DIRS, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = SPARK_LOCAL_DIRS

# Initialize findspark
try:
    findspark.init()
    logger.info("findspark initialized successfully")
except Exception as e:
    logger.warning(f"findspark initialization failed: {e}")
    # Continue anyway, as this might not be critical

class SparkSessionManager:
    """
    Thread-safe Spark session manager with session pooling and lifecycle management.
    """
    
    def __init__(self):
        self._sessions: Dict[str, SparkSession] = {}
        self._session_metadata: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._max_sessions = 2  # Reduced for 16GB server to prevent memory issues
        self._session_timeout = 600  # 10 minutes timeout for larger files
        self._cleanup_interval = 120  # 2 minutes cleanup interval
        self._last_cleanup = time.time()
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self._cleanup_thread.start()
    
    def _cleanup_worker(self):
        """Background thread to clean up expired sessions."""
        while True:
            try:
                time.sleep(self._cleanup_interval)
                self._cleanup_expired_sessions()
            except Exception as e:
                logger.error(f"Cleanup worker error: {e}")
    
    def _cleanup_expired_sessions(self):
        """Remove expired sessions."""
        current_time = time.time()
        with self._lock:
            expired_sessions = []
            for session_id, metadata in self._session_metadata.items():
                if current_time - metadata['last_used'] > self._session_timeout:
                    expired_sessions.append(session_id)
            
            for session_id in expired_sessions:
                self._stop_session(session_id)
                logger.info(f"Cleaned up expired session: {session_id}")
    
    def _create_session_config(self) -> Dict[str, str]:
        """Create Spark configuration."""
        # Use fixed JAR paths for Windows
        if os.name == 'nt':  # Windows
            jar_paths = [
                r"C:\spark-jars\spark-excel_2.12-3.3.1_0.18.7.jar",
                r"C:\spark-jars\poi-5.2.3.jar",
                r"C:\spark-jars\poi-ooxml-5.2.3.jar"
            ]
        else:  # Linux/Unix
            jar_paths = [
                "/opt/spark-jars/spark-excel_2.12-3.3.1_0.18.7.jar",
                "/opt/spark-jars/poi-5.2.3.jar",
                "/opt/spark-jars/poi-ooxml-5.2.3.jar"
            ]
        
        # Check JARs exist and log status
        missing_jars = []
        for jar in jar_paths:
            if not Path(jar).exists():
                missing_jars.append(jar)
                logger.warning(f"Missing JAR file: {jar}")
            else:
                logger.info(f"Found JAR file: {jar}")
        
        if missing_jars:
            logger.error(f"Missing required JAR files: {missing_jars}")
            # For now, continue without JARs - this will limit Excel functionality
            # but allow basic Spark operations
            jars_str = ""
            logger.warning("Continuing without Excel support JARs")
        else:
            jars_str = ",".join(jar_paths)
            logger.info(f"Using JAR files: {jars_str}")
        
        config = {
            "spark.driver.memory": "12g",  # Optimized for 16GB server (75% of available RAM)
            "spark.executor.memory": "12g",  # Optimized for 16GB server (75% of available RAM)
            "spark.driver.maxResultSize": "6g",  # Increased for larger datasets
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.hadoop.io.native.lib.available": "false",
            "spark.sql.shuffle.partitions": "16",  # Reduced for better memory management on 16GB server
            "spark.memory.fraction": "0.85",  # Increased memory fraction for better performance
            "spark.memory.storageFraction": "0.15",  # Reduced storage fraction to give more to execution
            "spark.ui.showConsoleProgress": "true",
            "spark.sql.sources.commitProtocolClass": "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
            "spark.driver.extraJavaOptions": "-Djava.net.preferIPv4Stack=true -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication",
            "spark.executor.extraJavaOptions": "-Djava.net.preferIPv4Stack=true -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "512m",  # Increased for better performance
            "spark.sql.files.maxPartitionBytes": "512MB",  # Increased for better performance
            "spark.sql.files.openCostInBytes": "16777216",  # Increased for better performance
            "spark.sql.files.minPartitionNum": "1",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer.max": "2047m",  # Maximum allowed for Kryo
            "spark.rpc.askTimeout": "600s",  # Increased from 300s
            "spark.rpc.lookupTimeout": "600s",  # Increased from 300s
            "spark.network.timeout": "600s",  # Increased from 300s
            "spark.executor.heartbeatInterval": "120s",  # Increased from 60s
            "spark.sql.broadcastTimeout": "600s",  # Increased from 300s
            "spark.sql.execution.timeout": "600s",  # Increased from 300s
            # Additional configurations for large files
            "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
            "spark.sql.adaptive.forceApplyShuffledHashJoin": "false",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "512MB",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "10",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "16",  # Reduced for 16GB server
            # Storage optimizations for 50GB storage
            "spark.local.dir": SPARK_LOCAL_DIRS,
            "spark.sql.warehouse.dir": os.path.join(SPARK_LOCAL_DIRS, "warehouse"),
            "spark.sql.execution.arrow.pyspark.selfDestruct.enabled": "true",
            "spark.sql.execution.arrow.pyspark.maxRecordsPerBatch": "5000"
        }
        
        # Only add JARs if they exist
        if jars_str:
            config["spark.jars"] = jars_str
            # Additional Excel-specific configurations
            config.update({
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
                "spark.sql.execution.arrow.pyspark.selfDestruct.enabled": "true",
                "spark.sql.execution.arrow.pyspark.maxRecordsPerBatch": "10000",
                # Excel reading optimizations
                "spark.sql.files.maxPartitionBytes": "256MB",
                "spark.sql.files.openCostInBytes": "8388608",
                "spark.sql.files.minPartitionNum": "1",
                "spark.sql.files.maxPartitionBytes": "256MB",
                # Memory management for large files
                "spark.memory.offHeap.enabled": "true",
                "spark.memory.offHeap.size": "4g",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.adaptive.localShuffleReader.enabled": "true"
            })
        
        return config

    
    def _create_spark_session(self, session_id: str) -> SparkSession:
        """Create a new Spark session."""
        try:
            config = self._create_session_config()
            
            builder = SparkSession.builder \
                .appName(f"ExcelProcessor-{session_id}") \
                .master("local[*]")
            
            # Apply all configurations
            for key, value in config.items():
                builder = builder.config(key, value)
            
            session = builder.getOrCreate()
            
            # Store session metadata
            self._session_metadata[session_id] = {
                'created_at': time.time(),
                'last_used': time.time(),
                'thread_id': threading.get_ident(),
                'status': 'active'
            }
            
            logger.info(f"Created new Spark session: {session_id} (version: {session.version})")
            return session
        except Exception as e:
            logger.error(f"Failed to create Spark session {session_id}: {e}")
            raise
    
    def _stop_session(self, session_id: str):
        """Stop a specific session."""
        if session_id in self._sessions:
            try:
                session = self._sessions[session_id]
                if session and not session._sc._jsc.sc().isStopped():
                    session.stop()
                    logger.info(f"Stopped Spark session: {session_id}")
            except Exception as e:
                logger.error(f"Error stopping session {session_id}: {e}")
            finally:
                del self._sessions[session_id]
                if session_id in self._session_metadata:
                    del self._session_metadata[session_id]
    
    def _validate_session(self, session: SparkSession) -> bool:
        """Validate if a session is still active."""
        try:
            # Check if SparkContext is stopped
            if session._sc._jsc.sc().isStopped():
                return False
            
            # Try a simple operation to test the session
            session._sc.parallelize([1]).count()
            return True
        except Exception as e:
            logger.warning(f"Session validation failed: {e}")
            return False
    
    def _get_session_id(self) -> str:
        """Generate a unique session ID."""
        return f"session_{threading.get_ident()}_{int(time.time() * 1000)}"
    
    def get_session(self) -> SparkSession:
        """Get a Spark session (create if needed)."""
        with self._lock:
            # Clean up expired sessions first
            self._cleanup_expired_sessions()
            
            # Check if we have too many sessions
            if len(self._sessions) >= self._max_sessions:
                # Find the oldest session to replace
                oldest_session = min(self._session_metadata.items(), 
                                   key=lambda x: x[1]['last_used'])
                self._stop_session(oldest_session[0])
                logger.info(f"Replaced oldest session due to pool limit: {oldest_session[0]}")
            
            # Create new session
            session_id = self._get_session_id()
            session = self._create_spark_session(session_id)
            self._sessions[session_id] = session
            
            return session
    
    def get_or_create_session(self) -> SparkSession:
        """Get existing session or create new one."""
        with self._lock:
            # Try to find an existing valid session
            for session_id, session in self._sessions.items():
                if self._validate_session(session):
                    # Update last used time
                    self._session_metadata[session_id]['last_used'] = time.time()
                    logger.info(f"Reusing existing session: {session_id}")
                    return session
            
            # No valid session found, create new one
            return self.get_session()
    
    def stop_all_sessions(self):
        """Stop all sessions (useful for cleanup)."""
        with self._lock:
            session_ids = list(self._sessions.keys())
            for session_id in session_ids:
                self._stop_session(session_id)
            logger.info("Stopped all Spark sessions")
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get information about current sessions."""
        with self._lock:
            info = {
                'total_sessions': len(self._sessions),
                'max_sessions': self._max_sessions,
                'sessions': {}
            }
            
            for session_id, metadata in self._session_metadata.items():
                info['sessions'][session_id] = {    
                    'created_at': metadata['created_at'],
                    'last_used': metadata['last_used'],
                    'age_seconds': time.time() - metadata['created_at'],
                    'idle_seconds': time.time() - metadata['last_used'],
                    'status': metadata['status']
                }
            
            return info

# Global session manager instance
_session_manager = SparkSessionManager()

def get_spark_session() -> SparkSession:
    """
    Get a Spark session with proper lifecycle management.
    This is the main function to use throughout the application.
    """
    try:
        return _session_manager.get_or_create_session()
    except Exception as e:
        logger.error(f"Failed to get Spark session: {e}")
        raise

def get_large_file_spark_session() -> SparkSession:
    """
    Get a Spark session specifically optimized for large file processing.
    This session has higher memory allocation and better resource management.
    """
    try:
        # Create a new session with large file optimizations
        config = _session_manager._create_session_config()
        
        # Override with large file specific settings optimized for 16GB server
        large_file_config = {
            "spark.driver.memory": "14g",  # Optimized for 16GB server (87.5% of available RAM)
            "spark.executor.memory": "14g",  # Optimized for 16GB server (87.5% of available RAM)
            "spark.driver.maxResultSize": "8g",  # Increased for very large datasets
            "spark.sql.files.maxPartitionBytes": "1GB",  # Larger partitions for better performance
            "spark.sql.files.openCostInBytes": "33554432",  # Higher open cost threshold
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1g",
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "8",  # Fewer initial partitions for 16GB server
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1GB",
            "spark.memory.offHeap.size": "8g",  # More off-heap memory for large files
            "spark.sql.execution.arrow.pyspark.maxRecordsPerBatch": "3000",  # Smaller batches for memory efficiency
            "spark.sql.adaptive.forceApplyShuffledHashJoin": "false",
            "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
            # Additional optimizations for large files on 16GB server
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.sql.adaptive.coalescePartitions.maxPartitionNum": "8",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",  # Reduced for memory efficiency
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.optimizeSkewedJoin.enabled": "true"
        }
        
        config.update(large_file_config)
        
        builder = SparkSession.builder \
            .appName("LargeFileProcessor") \
            .master("local[*]")
        
        # Apply all configurations
        for key, value in config.items():
            builder = builder.config(key, value)
        
        session = builder.getOrCreate()
        logger.info(f"Created large file optimized Spark session (version: {session.version})")
        return session
        
    except Exception as e:
        logger.error(f"Failed to create large file Spark session: {e}")
        # Fallback to regular session
        return get_spark_session()

@contextmanager
def large_file_spark_context():
    """
    Context manager for large file processing with automatic cleanup.
    
    Usage:
        with large_file_spark_context() as spark:
            df = spark.read.format("com.crealytics.spark.excel").load("large_file.xlsx")
            # ... process data
    """
    session = None
    try:
        session = get_large_file_spark_session()
        yield session
    except Exception as e:
        logger.error(f"Error in large file Spark session context: {e}")
        raise
    finally:
        if session:
            try:
                session.stop()
                logger.info("Stopped large file Spark session")
            except Exception as e:
                logger.warning(f"Error stopping large file session: {e}")

@contextmanager
def spark_session_context():
    """
    Context manager for Spark sessions.
    Automatically handles session cleanup.
    
    Usage:
        with spark_session_context() as spark:
            df = spark.read.csv("file.csv")
            # ... process data
    """
    session = None
    try:
        session = get_spark_session()
        yield session
    except Exception as e:
        logger.error(f"Error in Spark session context: {e}")
        raise
    finally:
        # Note: We don't stop the session here as it's managed by the pool
        pass

def stop_all_spark_sessions():
    """Stop all Spark sessions (useful for application shutdown)."""
    _session_manager.stop_all_sessions()

def get_spark_session_info() -> Dict[str, Any]:
    """Get information about current Spark sessions."""
    return _session_manager.get_session_info()

def validate_spark_session(spark: SparkSession) -> bool:
    """Validate if a Spark session is still active."""
    return _session_manager._validate_session(spark)

def process_excel_locally(file_path: str, sheet_name: str = None) -> Dict[str, Any]:
    """
    Process Excel file locally using Spark with Excel support.
    
    Args:
        file_path: Path to Excel file
        sheet_name: Specific sheet to process (optional)
        
    Returns:
        Processing results
    """
    try:
        # Get Spark session optimized for large files
        spark = get_large_file_spark_session()
        
        logger.info(f"Processing Excel file locally: {file_path}")
        
        # Read Excel file using Spark Excel format
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("dataAddress", sheet_name or "Sheet1") \
            .option("maxRowsInMemory", "10000") \
            .option("tempFileThreshold", "1000000") \
            .load(file_path)
        
        # Get basic info
        total_rows = df.count()
        total_columns = len(df.columns)
        
        # Show sample data (first 10 rows)
        sample_data = df.limit(10).toPandas().to_dict('records')
        
        # Get column types
        column_types = {}
        for field in df.schema.fields:
            column_types[field.name] = str(field.dataType)
        
        logger.info(f"Successfully processed Excel file: {total_rows} rows, {total_columns} columns")
        
        return {
            "status": "success",
            "service": "local_spark",
            "total_rows": total_rows,
            "total_columns": total_columns,
            "columns": df.columns,
            "column_types": column_types,
            "sample_data": sample_data,
            "file_path": file_path,
            "sheet_name": sheet_name or "Sheet1"
        }
        
    except Exception as e:
        logger.error(f"Local Excel processing failed: {e}")
        return {
            "status": "error",
            "service": "local_spark",
            "error": str(e),
            "file_path": file_path,
            "sheet_name": sheet_name or "Sheet1"
        }

def process_excel_with_fallback(file_path: str, sheet_name: str = None) -> Dict[str, Any]:
    """
    Process Excel file with fallback to pandas if Spark fails.
    
    Args:
        file_path: Path to Excel file
        sheet_name: Specific sheet to process (optional)
        
    Returns:
        Processing results
    """
    try:
        # First try Spark processing
        result = process_excel_locally(file_path, sheet_name)
        
        if result['status'] == 'success':
            return result
        
        # If Spark fails, fallback to pandas
        logger.info(f"Spark processing failed, falling back to pandas: {result['error']}")
        
        import pandas as pd
        
        # Read Excel file with pandas
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        else:
            df = pd.read_excel(file_path, dtype=str)
        
        # Get basic info
        total_rows = len(df)
        total_columns = len(df.columns)
        
        # Show sample data (first 10 rows)
        sample_data = df.head(10).to_dict('records')
        
        # Get column types
        column_types = {}
        for col in df.columns:
            column_types[col] = str(df[col].dtype)
        
        logger.info(f"Pandas fallback successful: {total_rows} rows, {total_columns} columns")
        
        return {
            "status": "success",
            "service": "pandas_fallback",
            "total_rows": total_rows,
            "total_columns": total_columns,
            "columns": df.columns.tolist(),
            "column_types": column_types,
            "sample_data": sample_data,
            "file_path": file_path,
            "sheet_name": sheet_name or "Sheet1",
            "fallback_reason": result['error']
        }
        
    except Exception as e:
        logger.error(f"All Excel processing methods failed: {e}")
        return {
            "status": "error",
            "service": "all_methods_failed",
            "error": str(e),
            "file_path": file_path,
            "sheet_name": sheet_name or "Sheet1"
        }

# Legacy function for backward compatibility
def get_spark_session_legacy() -> SparkSession:
    """
    Legacy function for backward compatibility.
    Use get_spark_session() instead.
    """
    logger.warning("Using legacy get_spark_session_legacy(). Use get_spark_session() instead.")
    return get_spark_session()