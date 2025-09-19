#!/usr/bin/env python3
"""
Test script to verify local Spark setup and Excel processing functionality.
Run this script to test if your local Spark installation is working correctly.
"""

import os
import sys
import logging

# Add the api directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'api'))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_spark_imports():
    """Test if Spark modules can be imported."""
    try:
        from pyspark.sql import SparkSession
        logger.info("✅ PySpark imports successful")
        return True
    except ImportError as e:
        logger.error(f"❌ PySpark import failed: {e}")
        return False

def test_spark_utils_imports():
    """Test if our custom Spark utilities can be imported."""
    try:
        from api.spark_utils import (
            get_spark_session, 
            get_large_file_spark_session,
            process_excel_locally,
            process_excel_with_fallback
        )
        logger.info("✅ Spark utilities imports successful")
        return True
    except ImportError as e:
        logger.error(f"❌ Spark utilities import failed: {e}")
        return False

def test_spark_session_creation():
    """Test if we can create a Spark session."""
    try:
        from api.spark_utils import get_spark_session
        
        logger.info("🚀 Creating Spark session...")
        spark = get_spark_session()
        
        if spark:
            logger.info(f"✅ Spark session created successfully")
            logger.info(f"   Spark version: {spark.version}")
            logger.info(f"   Spark UI: http://localhost:4040")
            
            # Test basic functionality
            test_df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
            count = test_df.count()
            logger.info(f"✅ Basic Spark functionality test passed: {count} rows")
            
            return True
        else:
            logger.error("❌ Failed to create Spark session")
            return False
            
    except Exception as e:
        logger.error(f"❌ Spark session creation failed: {e}")
        return False

def test_large_file_spark_session():
    """Test if we can create a large file optimized Spark session."""
    try:
        from api.spark_utils import get_large_file_spark_session
        
        logger.info("🚀 Creating large file optimized Spark session...")
        spark = get_large_file_spark_session()
        
        if spark:
            logger.info(f"✅ Large file Spark session created successfully")
            logger.info(f"   Spark version: {spark.version}")
            
            # Test basic functionality
            test_df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
            count = test_df.count()
            logger.info(f"✅ Large file Spark session test passed: {count} rows")
            
            return True
        else:
            logger.error("❌ Failed to create large file Spark session")
            return False
            
    except Exception as e:
        logger.error(f"❌ Large file Spark session creation failed: {e}")
        return False

def test_spark_configuration():
    """Test if Spark configuration is properly set."""
    try:
        from api.spark_utils import get_spark_session
        
        spark = get_spark_session()
        
        # Check some key configurations
        configs = spark.conf.getAll()
        
        logger.info("🔧 Checking Spark configuration...")
        
        # Check memory settings
        driver_memory = configs.get("spark.driver.memory", "Not set")
        executor_memory = configs.get("spark.executor.memory", "Not set")
        
        logger.info(f"   Driver memory: {driver_memory}")
        logger.info(f"   Executor memory: {executor_memory}")
        
        # Check if our custom configs are applied
        if "12g" in driver_memory or "14g" in driver_memory:
            logger.info("✅ Memory configuration looks good")
        else:
            logger.warning("⚠️ Memory configuration may not be optimal")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Configuration check failed: {e}")
        return False

def test_excel_processing_functions():
    """Test if Excel processing functions are available."""
    try:
        from api.spark_utils import process_excel_locally, process_excel_with_fallback
        
        logger.info("✅ Excel processing functions imported successfully")
        
        # Test function signatures
        import inspect
        
        local_sig = inspect.signature(process_excel_locally)
        fallback_sig = inspect.signature(process_excel_with_fallback)
        
        logger.info(f"   process_excel_locally signature: {local_sig}")
        logger.info(f"   process_excel_with_fallback signature: {fallback_sig}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Excel processing functions test failed: {e}")
        return False

def main():
    """Run all tests."""
    logger.info("🧪 Starting local Spark setup tests...")
    logger.info("=" * 60)
    
    tests = [
        ("PySpark Imports", test_spark_imports),
        ("Spark Utils Imports", test_spark_utils_imports),
        ("Spark Session Creation", test_spark_session_creation),
        ("Large File Spark Session", test_large_file_spark_session),
        ("Spark Configuration", test_spark_configuration),
        ("Excel Processing Functions", test_excel_processing_functions),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n🔍 Running test: {test_name}")
        try:
            if test_func():
                passed += 1
                logger.info(f"✅ {test_name} PASSED")
            else:
                logger.error(f"❌ {test_name} FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name} FAILED with exception: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Your local Spark setup is working correctly.")
        logger.info("\n🚀 You can now use local Spark processing instead of cloud services.")
    else:
        logger.error("❌ Some tests failed. Please check your Spark installation and configuration.")
        logger.info("\n💡 Common issues:")
        logger.info("   - Java not installed or JAVA_HOME not set")
        logger.info("   - Spark not installed or SPARK_HOME not set")
        logger.info("   - Missing JAR files for Excel processing")
        logger.info("   - Insufficient memory or disk space")

if __name__ == "__main__":
    main()
