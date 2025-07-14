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
JAVA_HOME = r"C:\Users\harsh\java\jdk-17"
HADOOP_HOME = r"C:\hadoop"
PYSPARK_PYTHON = sys.executable

# # Set environment variables
# os.environ["JAVA_HOME"] = JAVA_HOME
# os.environ["HADOOP_HOME"] = HADOOP_HOME
# os.environ["PATH"] += f";{os.path.join(HADOOP_HOME, 'bin')}"
# os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
# os.environ["SPARK_LOCAL_IP"] = "localhost"

# Create local Spark temp dir if it doesn't exist
SPARK_LOCAL_DIRS = os.path.join(os.getcwd(), "spark-temp")
os.makedirs(SPARK_LOCAL_DIRS, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = SPARK_LOCAL_DIRS

# --- Linux/Server ---
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["HADOOP_HOME"] = "/opt/hadoop"
os.environ["SPARK_HOME"] = "/opt/spark"
os.environ["PATH"] += f":/opt/hadoop/bin:/opt/spark/bin"
os.environ["SPARK_LOCAL_DIRS"] = "/home/prashant/Dashboard-backend/spark-temp"
os.environ["PYSPARK_PYTHON"] = sys.executable

# Initialize findspark
findspark.init()

class SparkSessionManager:
    """
    Thread-safe Spark session manager with session pooling and lifecycle management.
    """
    
    def __init__(self):
        self._sessions: Dict[str, SparkSession] = {}
        self._session_metadata: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._max_sessions = 3  # Maximum concurrent sessions
        self._session_timeout = 300  # 5 minutes timeout
        self._cleanup_interval = 60  # 1 minute cleanup interval
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
        # jar_paths = [
        #     r"C:\spark-jars\spark-excel_2.12-3.3.1_0.18.7.jar",
        #     r"C:\spark-jars\poi-5.2.3.jar",
        #     r"C:\spark-jars\poi-ooxml-5.2.3.jar"
        # ]

        jar_paths = [
            "/opt/spark-jars/spark-excel_2.12-3.3.1_0.18.7.jar",
            "/opt/spark-jars/poi-5.2.3.jar",
            "/opt/spark-jars/poi-ooxml-5.2.3.jar"
        ]
        
        # Check JARs exist
        missing_jars = [jar for jar in jar_paths if not Path(jar).exists()]
        if missing_jars:
            raise FileNotFoundError(f"Missing required JAR files: {missing_jars}")
        
        jars_str = ",".join(jar_paths)
        
        return {
            "spark.jars": jars_str,
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.hadoop.io.native.lib.available": "false",
            "spark.sql.shuffle.partitions": "16",
            "spark.ui.showConsoleProgress": "true",
            "spark.sql.sources.commitProtocolClass": "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
            "spark.driver.extraJavaOptions": "-Djava.net.preferIPv4Stack=true -XX:+UseG1GC -XX:MaxGCPauseMillis=200",
            "spark.executor.extraJavaOptions": "-Djava.net.preferIPv4Stack=true -XX:+UseG1GC -XX:MaxGCPauseMillis=200",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
            "spark.sql.files.maxPartitionBytes": "128MB",
            "spark.sql.files.openCostInBytes": "4194304",
            "spark.sql.files.minPartitionNum": "1",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer.max": "1024m",
            # Add timeout configurations for large files
            "spark.rpc.askTimeout": "300s",
            "spark.rpc.lookupTimeout": "300s",
            "spark.network.timeout": "300s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.sql.broadcastTimeout": "300s",
            "spark.sql.execution.timeout": "300s"
        }
    
    def _create_spark_session(self, session_id: str) -> SparkSession:
        """Create a new Spark session."""
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
    return _session_manager.get_or_create_session()

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

# Legacy function for backward compatibility
def get_spark_session_legacy() -> SparkSession:
    """
    Legacy function for backward compatibility.
    Use get_spark_session() instead.
    """
    logger.warning("Using legacy get_spark_session_legacy(). Use get_spark_session() instead.")
    return get_spark_session()