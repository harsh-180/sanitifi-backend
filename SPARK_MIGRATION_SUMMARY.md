# Spark Migration Summary: Cloud to Local Processing

## Overview
This document summarizes the changes made to switch from using Databricks cloud Spark services to local Spark processing on your server.

## Changes Made

### 1. Updated `spark_utils.py`
- **Added new Excel processing functions:**
  - `process_excel_locally()`: Processes Excel files using local Spark with Excel support
  - `process_excel_with_fallback()`: Falls back to pandas if Spark fails
  - Both functions provide comprehensive error handling and data extraction

- **Optimized Spark configuration for your server (16GB RAM, 50GB storage):**
  - Driver memory: 12g (75% of available RAM)
  - Executor memory: 12g (75% of available RAM)
  - Large file processing: 14g memory (87.5% of available RAM)
  - Reduced partition counts for better memory management
  - Optimized storage settings for 50GB storage capacity
  - Enhanced garbage collection and memory management

- **Session management improvements:**
  - Reduced max concurrent sessions from 3 to 2 (prevents memory issues)
  - Increased session timeout to 10 minutes for large files
  - Better cleanup intervals

### 2. Updated `views.py`
- **Replaced cloud processing calls:**
  - Removed imports from `spark_utils_cloud`
  - Replaced `process_excel_always_cloud()` with `process_excel_with_fallback()`
  - Updated all Excel processing logic to use local Spark

- **Enhanced data handling:**
  - Better error handling and fallback mechanisms
  - Improved data extraction and processing
  - More detailed logging and status reporting

### 3. Created Test Script
- **`test_local_spark.py`**: Comprehensive test script to verify local Spark setup
- Tests all major components: imports, session creation, configuration, Excel processing
- Provides detailed feedback and troubleshooting information

## Benefits of Local Processing

### Performance
- **Faster processing**: No network latency or cloud queue delays
- **Better resource utilization**: Direct access to server resources
- **Optimized memory usage**: Tailored for your 16GB RAM configuration

### Cost
- **No cloud costs**: Eliminates Databricks usage charges
- **Predictable expenses**: Only server hosting costs
- **Better ROI**: Full control over resource allocation

### Control
- **Data privacy**: Files never leave your server
- **Customization**: Full control over Spark configuration
- **Reliability**: No dependency on external services

## Server Requirements

### Hardware
- **RAM**: 16GB (minimum 12GB available for Spark)
- **Storage**: 50GB (minimum 20GB available for temporary files)
- **CPU**: Multi-core recommended for parallel processing

### Software
- **Java**: JDK 17 (already configured)
- **Spark**: Local installation with Excel support
- **Python**: PySpark and required dependencies

## Configuration Details

### Memory Allocation
```
Regular Spark Session:
- Driver: 12g (75% of 16GB)
- Executor: 12g (75% of 16GB)
- Max Result: 6g

Large File Processing:
- Driver: 14g (87.5% of 16GB)
- Executor: 14g (87.5% of 16GB)
- Max Result: 8g
- Off-heap: 8g
```

### Storage Optimization
```
- Local temp directory: Dashboard-backend/spark-temp
- Warehouse directory: Dashboard-backend/spark-temp/warehouse
- Partition size: 512MB (regular) / 1GB (large files)
- Adaptive query execution: Enabled
```

## Usage

### Basic Excel Processing
```python
from api.spark_utils import process_excel_with_fallback

# Process Excel file with automatic fallback
result = process_excel_with_fallback('file.xlsx', 'Sheet1')

if result['status'] == 'success':
    print(f"Processed {result['total_rows']} rows, {result['total_columns']} columns")
    print(f"Service used: {result['service']}")
```

### Large File Processing
```python
from api.spark_utils import get_large_file_spark_session

# Get optimized session for large files
spark = get_large_file_spark_session()

# Process large Excel files
df = spark.read.format("com.crealytics.spark.excel").load("large_file.xlsx")
```

## Testing

### Run the Test Script
```bash
cd Dashboard-backend
python test_local_spark.py
```

### Expected Output
- ✅ All tests should pass
- Spark session creation successful
- Memory configuration optimized
- Excel processing functions available

## Troubleshooting

### Common Issues

1. **Java not found**
   - Verify JAVA_HOME is set correctly
   - Check Java version (JDK 17 required)

2. **Memory issues**
   - Reduce Spark memory settings if needed
   - Close other applications to free memory
   - Check available RAM with `free -h`

3. **Storage issues**
   - Ensure sufficient disk space
   - Check permissions on spark-temp directory
   - Clean up old temporary files

4. **Excel processing fails**
   - Verify JAR files are present
   - Check file permissions
   - Use pandas fallback as backup

### Performance Tuning

1. **For very large files (>1GB)**
   - Use `get_large_file_spark_session()`
   - Increase partition sizes
   - Reduce batch sizes

2. **For memory-constrained situations**
   - Reduce driver/executor memory
   - Increase cleanup frequency
   - Use smaller partition sizes

## Migration Checklist

- [x] Updated `spark_utils.py` with local processing functions
- [x] Optimized configuration for 16GB RAM / 50GB storage
- [x] Updated `views.py` to use local processing
- [x] Removed cloud processing dependencies
- [x] Created comprehensive test script
- [x] Updated session management settings
- [x] Enhanced error handling and fallbacks

## Next Steps

1. **Test the setup**: Run `test_local_spark.py`
2. **Process sample files**: Try uploading Excel files to verify functionality
3. **Monitor performance**: Check memory usage and processing times
4. **Optimize further**: Adjust settings based on actual usage patterns

## Support

If you encounter issues:
1. Check the test script output for specific error messages
2. Review server logs for detailed error information
3. Verify Java and Spark installations
4. Check available system resources

---

**Note**: This migration maintains backward compatibility while providing significant performance improvements and cost savings through local processing.
