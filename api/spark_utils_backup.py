import os
import sys
import logging
from pathlib import Path
import findspark
from pyspark.sql import SparkSession

# ─── Logger Setup ──────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ─── Environment Setup ─────────────────────────────────────────────────────────
JAVA_HOME = r"C:\Users\harsh\java\jdk-17"
HADOOP_HOME = r"C:\hadoop"
PYSPARK_PYTHON = sys.executable  # Use current interpreter

os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['HADOOP_HOME'] = HADOOP_HOME
os.environ['PATH'] += f";{os.path.join(HADOOP_HOME, 'bin')}"
os.environ['PYSPARK_PYTHON'] = PYSPARK_PYTHON

# Set Spark environment variables for Windows
os.environ['SPARK_LOCAL_IP'] = 'localhost'
os.environ['SPARK_LOCAL_DIRS'] = os.path.join(os.getcwd(), 'spark-temp')

findspark.init()

# ─── Singleton SparkSession Holder ─────────────────────────────────────────────
_spark_session = None

def get_spark_session() -> SparkSession:
    global _spark_session

    if _spark_session is not None:
        return _spark_session

    jar_paths = [
        r"C:\spark-jars\spark-excel_2.12-3.3.1_0.18.7.jar",
        r"C:\spark-jars\poi-5.2.3.jar",
        r"C:\spark-jars\poi-ooxml-5.2.3.jar"
    ]

    # Check JARs exist
    missing_jars = [jar for jar in jar_paths if not Path(jar).exists()]
    if missing_jars:
        raise FileNotFoundError(f"Missing required JAR files: {missing_jars}")

    jars_str = ",".join(jar_paths)

    _spark_session = SparkSession.builder \
        .appName("ExcelProcessor") \
        .master("local[*]") \
        .config("spark.jars", jars_str) \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.ui.showConsoleProgress", "true") \
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .getOrCreate()

    logger.info(f"✔ SparkSession created using Spark version: {_spark_session.version}")
    return _spark_session
