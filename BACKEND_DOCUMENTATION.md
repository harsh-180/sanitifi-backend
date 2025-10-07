# Sanitify Backend Documentation

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture & Structure](#architecture--structure)
3. [Features & Functionality](#features--functionality)
4. [Database Models](#database-models)
5. [API Documentation](#api-documentation)
6. [Configuration & Settings](#configuration--settings)
7. [Hardcoded Data & Values](#hardcoded-data--values)
8. [Control Flow Graph](#control-flow-graph)

## Project Overview

**Sanitify Backend** is a Django REST Framework-based API server that provides comprehensive data analytics, processing, and visualization capabilities. It serves as the backend for the Sanitify data analytics platform, handling user authentication, data processing, visualization generation, and project management.

### Technology Stack
- **Framework**: Django 5.1.5 + Django REST Framework
- **Database**: SQLite (development), PostgreSQL (production ready)
- **Authentication**: JWT (JSON Web Tokens)
- **Data Processing**: Apache Spark (PySpark), Pandas, NumPy
- **Visualization**: Plotly, Matplotlib, Seaborn
- **File Processing**: OpenPyXL, Python-PPTX
- **Cloud Integration**: Google Sheets API, OneDrive API
- **Caching**: Django cache framework

## Architecture & Structure

### Directory Structure
```
Dashboard-backend/
├── api/                    # Main application
│   ├── models.py          # Database models
│   ├── views.py           # API views (main)
│   ├── apis.py            # Additional API views
│   ├── urls.py            # URL routing
│   ├── serializers.py     # Data serializers
│   ├── spark_utils.py     # Spark session management
│   ├── auth.py            # Authentication utilities
│   ├── middleware.py      # Custom middleware
│   ├── log_utils.py       # Logging utilities
│   └── migrations/        # Database migrations
├── dashboard/             # Django project settings
│   ├── settings.py        # Main settings
│   ├── urls.py           # Root URL configuration
│   ├── wsgi.py           # WSGI configuration
│   └── asgi.py           # ASGI configuration
├── media/                 # File storage
├── spark-temp/           # Spark temporary files
├── artifacts/            # Spark session artifacts
└── requirements.txt      # Python dependencies
```

### Core Components
1. **API Layer**: RESTful endpoints for all operations
2. **Data Processing Engine**: Spark-based data transformation
3. **Authentication System**: JWT-based user management
4. **File Management**: Upload, storage, and processing
5. **Visualization Engine**: Chart and plot generation
6. **Project Management**: User projects and sharing
7. **Logging System**: Comprehensive API and user action logging

## Features & Functionality

### 1. Authentication & User Management
- **User Registration**: Email-based signup with OTP verification
- **User Login**: JWT token-based authentication
- **OTP System**: Time-based OTP for email verification
- **Password Management**: Secure password hashing
- **Session Management**: Token-based sessions

### 2. Project & File Management
- **Project Creation**: User-specific project organization
- **File Upload**: Support for Excel (.xlsx), CSV files
- **File Organization**: KPI and Media file categorization
- **File Processing**: Automatic sheet detection and processing
- **File Storage**: Organized media storage structure
- **File Deletion**: Secure file removal with cleanup

### 3. Data Processing & Transformation
- **Data Cleaning**:
  - Remove spaces and special characters
  - Case conversion (lowercase/uppercase)
  - Missing value handling
  - Data type detection and conversion
- **Data Melting**: Reshape data from wide to long format
- **Data Mapping**: Map columns between datasets
- **Data Merging**: Combine multiple datasets
- **Custom Scripts**: Python-based custom transformations
- **Concatenation**: Merge multiple sheets/files

### 4. Advanced Data Analytics
- **Exploratory Data Analysis (EDA)**:
  - Statistical summaries
  - Correlation analysis
  - Distribution analysis
  - Missing data analysis
  - Data profiling
- **Pivot Tables**: Dynamic pivot table generation
- **Statistical Analysis**: Descriptive statistics, hypothesis testing
- **Data Profiling**: Comprehensive data quality assessment

### 5. Visualization & Charting
- **Chart Generation**:
  - Bar charts, line charts, pie charts
  - Scatter plots, heatmaps, box plots
  - Histograms, distribution plots
  - Correlation matrices
- **Interactive Charts**: Plotly-based interactive visualizations
- **Chart Customization**: Colors, themes, annotations
- **Chart Export**: PNG, SVG, PDF formats
- **Chart Management**: Save, load, update, delete charts

### 6. Reporting & Export
- **PowerPoint Reports**: Automated PPT generation
- **Excel Export**: Data and charts to Excel
- **PDF Generation**: Report generation in PDF format
- **Image Downloads**: Individual chart downloads
- **Template Support**: Customizable report templates

### 7. Project Sharing & Collaboration
- **Project Sharing**: Share projects with other users
- **Permission Management**: View, edit, admin permissions
- **File-Level Sharing**: Share specific files within projects
- **Access Control**: User-based access restrictions
- **Collaboration Features**: Multi-user project access

### 8. Version Control & History
- **Transformation History**: Track all data transformations
- **Commit System**: Save transformation states
- **Undo/Redo**: Revert or reapply transformations
- **Data Lineage**: Track data transformation paths
- **State Management**: Maintain data processing states

### 9. Cloud Integration
- **Google Sheets**: Create and sync with Google Sheets
- **OneDrive**: Integration with Microsoft OneDrive
- **OAuth Authentication**: Secure cloud service authentication
- **Real-time Sync**: Bidirectional data synchronization

### 10. Monitoring & Logging
- **API Logging**: Comprehensive request/response logging
- **User Action Tracking**: Track user activities
- **Performance Monitoring**: API response time tracking
- **Error Logging**: Detailed error tracking and reporting
- **Audit Trail**: Complete user action audit trail

## Database Models

### Core Models

#### 1. User Model
```python
class User(models.Model):
    email = models.EmailField(unique=True)
    username = models.CharField(max_length=255, blank=True)
    password = models.CharField(max_length=128)
```

#### 2. OTPToken Model
```python
class OTPToken(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    otp = models.CharField(max_length=6)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    is_used = models.BooleanField(default=False)
```

#### 3. Projects Model
```python
class Projects(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    name = models.CharField(max_length=255)
    kpi_id = models.IntegerField(unique=True, null=True, blank=True)
    kpi_file = models.JSONField(default=list, blank=True)
    media_id = models.IntegerField(unique=True, null=True, blank=True)
    media_file = models.JSONField(default=list, blank=True)
    concatenated_file = models.JSONField(default=list, blank=True)
    google_sheet_ids = models.JSONField(default=dict, blank=True)
```

#### 4. SavedScript Model
```python
class SavedScript(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    title = models.CharField(max_length=255)
    script_content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True, null=True)
```

#### 5. SavedPlot Model
```python
class SavedPlot(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    project = models.ForeignKey('Projects', on_delete=models.CASCADE)
    file_type = models.CharField(max_length=50)
    file_name = models.CharField(max_length=255)
    sheet_name = models.CharField(max_length=255)
    plot_name = models.CharField(max_length=255, default='Default Plot')
    plot_config = models.JSONField()
    chart_data = models.JSONField()
    chart_options = models.JSONField()
```

#### 6. EDAPlot Model
```python
class EDAPlot(models.Model):
    PLOT_TYPES = [
        ('histogram', 'Histogram'),
        ('boxplot', 'Box Plot'),
        ('scatter', 'Scatter Plot'),
        ('correlation', 'Correlation Matrix'),
        ('missing_values', 'Missing Values'),
        ('distribution', 'Distribution Plot'),
        ('heatmap', 'Heatmap'),
        ('bar', 'Bar Chart'),
        ('line', 'Line Chart'),
        ('pie', 'Pie Chart'),
        ('custom', 'Custom Plot'),
    ]
    
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    project = models.ForeignKey('Projects', on_delete=models.CASCADE)
    plot_name = models.CharField(max_length=255, default='EDA Plot')
    plot_type = models.CharField(max_length=20, choices=PLOT_TYPES, default='custom')
    file_type = models.CharField(max_length=50, blank=True, null=True)
    file_name = models.CharField(max_length=255, blank=True, null=True)
    sheet_name = models.CharField(max_length=255, blank=True, null=True)
    plot_config = models.JSONField(default=dict)
    chart_data = models.JSONField(default=dict)
    chart_options = models.JSONField(default=dict)
    eda_analysis_type = models.CharField(max_length=100, blank=True, null=True)
    columns_analyzed = models.JSONField(default=list)
    data_summary = models.JSONField(default=dict)
    description = models.TextField(blank=True, null=True)
    tags = models.JSONField(default=list)
```

#### 7. ProjectShare Model
```python
class ProjectShare(models.Model):
    SHARE_TYPES = [
        ('project', 'Entire Project'),
        ('file', 'Specific File'),
    ]
    
    PERMISSION_LEVELS = [
        ('view', 'View Only'),
        ('edit', 'Edit'),
        ('admin', 'Admin'),
    ]
    
    project = models.ForeignKey('Projects', on_delete=models.CASCADE, related_name='shares')
    shared_by = models.ForeignKey('User', on_delete=models.CASCADE, related_name='shared_projects')
    shared_with = models.ForeignKey('User', on_delete=models.CASCADE, related_name='received_shares')
    share_type = models.CharField(max_length=10, choices=SHARE_TYPES, default='project')
    permission_level = models.CharField(max_length=10, choices=PERMISSION_LEVELS, default='view')
    file_type = models.CharField(max_length=10, blank=True, null=True)
    file_name = models.CharField(max_length=255, blank=True, null=True)
    sheet_name = models.CharField(max_length=255, blank=True, null=True)
    is_active = models.BooleanField(default=True)
```

#### 8. APILog Model
```python
class APILog(models.Model):
    user = models.ForeignKey('User', on_delete=models.SET_NULL, null=True, blank=True)
    endpoint = models.CharField(max_length=255)
    method = models.CharField(max_length=10)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True, null=True)
    request_payload = models.JSONField(default=dict, blank=True)
    response_status = models.IntegerField(null=True, blank=True)
    response_data = models.JSONField(default=dict, blank=True)
    execution_time = models.FloatField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
```

## API Documentation

### Authentication APIs

#### 1. User Signup
```http
POST /api/auth/signup/
```
**Payload:**
```json
{
  "email": "user@example.com",
  "password": "password123",
  "confirm_password": "password123"
}
```
**Response:**
```json
{
  "message": "User created successfully. OTP sent to email.",
  "user_id": 123
}
```

#### 2. User Login
```http
POST /api/auth/signin/
```
**Payload:**
```json
{
  "email": "user@example.com",
  "password": "password123"
}
```
**Response:**
```json
{
  "message": "Login successful",
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "user_id": 123
}
```

#### 3. OTP Verification
```http
POST /api/auth/verify-otp/
```
**Payload:**
```json
{
  "email": "user@example.com",
  "otp": "123456"
}
```
**Response:**
```json
{
  "message": "OTP verified successfully",
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
}
```

### Project Management APIs

#### 4. Create Project
```http
POST /api/auth/upload/
```
**Payload:**
```json
{
  "project_name": "My Data Project",
  "user_id": 123
}
```
**Response:**
```json
{
  "message": "Project created successfully",
  "project_id": 456,
  "project_name": "My Data Project"
}
```

#### 5. Get User Projects
```http
GET /api/auth/projects/
```
**Query Parameters:**
- `user_id`: User identifier
- `page`: Page number (pagination)

**Response:**
```json
{
  "projects": [
    {
      "id": 456,
      "name": "My Data Project",
      "kpi_files": ["file1.xlsx", "file2.csv"],
      "media_files": ["image1.jpg"],
      "created_at": "2024-01-15T10:30:00Z"
    }
  ],
  "total_pages": 5,
  "current_page": 1
}
```

#### 6. Delete Project
```http
POST /api/auth/projects/delete
```
**Payload:**
```json
{
  "project_id": 456,
  "user_id": 123
}
```

### File Management APIs

#### 7. File Upload
```http
POST /api/upload/
```
**Payload:**
```json
{
  "file_type": "kpi|media",
  "file_name": "data.xlsx",
  "file_id": "file123",
  "project_id": 456,
  "user_id": 123,
  "permission_level": "admin",
  "is_shared": false,
  "share_id": null
}
```
**Response:**
```json
{
  "message": "File uploaded successfully",
  "sheets_data": [
    {"name": "Sheet1", "rows": 1000, "columns": 15},
    {"name": "Sheet2", "rows": 500, "columns": 8}
  ],
  "file_id": "file123"
}
```

#### 8. Get Sheet Data (Paginated)
```http
POST /api/sheet-page/
```
**Payload:**
```json
{
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123,
  "page": 1,
  "page_size": 1000
}
```
**Response:**
```json
{
  "data": [
    {"column1": "value1", "column2": "value2"},
    {"column1": "value3", "column2": "value4"}
  ],
  "total_rows": 1000,
  "current_page": 1,
  "total_pages": 10,
  "columns": ["column1", "column2"]
}
```

#### 9. Get Complete Sheet Data
```http
POST /api/sheet-complete/
```
**Payload:**
```json
{
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123
}
```

### Data Processing APIs

#### 10. Data Cleaning
```http
POST /api/cleaning/
```
**Payload:**
```json
{
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123,
  "cleaning_config": {
    "columns_to_lowercase": ["name", "city"],
    "columns_to_remove_spaces": ["address"],
    "special_chars_to_remove": {
      "column_name": ["@", "#", "$"]
    }
  }
}
```

#### 11. Data Melting
```http
POST /api/melting/
```
**Payload:**
```json
{
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123,
  "melt_config": {
    "unchanged_columns": ["id", "name"],
    "columns_to_melt": ["jan", "feb", "mar"],
    "variable_column_name": "month",
    "value_column_name": "sales"
  }
}
```

#### 12. Data Mapping
```http
POST /api/mapping/
```
**Payload:**
```json
{
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123,
  "mapping_file": "mapping.xlsx",
  "column_mappings": {
    "source_column": "target_column"
  }
}
```

#### 13. Custom Script Execution
```http
POST /api/custom-script/
```
**Payload:**
```json
{
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123,
  "script": "import pandas as pd\n# Custom Python code here",
  "action": "preview|save"
}
```

### Visualization APIs

#### 14. Save EDA Plot
```http
POST /api/save-eda-plot/
```
**Payload:**
```json
{
  "plot_name": "Sales Analysis",
  "plot_type": "bar",
  "plot_config": {
    "xAxes": ["category"],
    "yAxes": ["sales"],
    "chartType": "bar",
    "aggregationMethod": "sum"
  },
  "chart_data": {...},
  "chart_options": {...},
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "sheet_name": "Sheet1",
  "project_id": 456,
  "user_id": 123,
  "tags": ["sales", "analysis"]
}
```

#### 15. Fetch EDA Plots
```http
GET /api/fetch-eda-plots/
```
**Query Parameters:**
- `user_id`: User identifier
- `project_id`: Project identifier (optional)
- `plot_type`: Filter by plot type (optional)

#### 16. Generate Pivot Table
```http
POST /api/pivot-table/
```
**Payload:**
```json
{
  "pivot_config": {
    "rows": ["category", "subcategory"],
    "columns": ["year"],
    "values": ["sales", "profit"],
    "aggregations": {
      "sales": ["sum"],
      "profit": ["sum", "avg"]
    },
    "filters": {
      "region": ["north", "south"]
    }
  },
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "sheet_name": "Sheet1",
  "project_id": 456,
  "user_id": 123
}
```

#### 17. Chart Builder
```http
POST /api/chart-builder/
```
**Payload:**
```json
{
  "chart_config": {
    "chart_type": "bar",
    "x_axis": "category",
    "y_axis": "value",
    "title": "My Chart",
    "colors": ["#ff6b6b", "#4ecdc4"],
    "aggregation": "sum"
  },
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "sheet_name": "Sheet1",
  "project_id": 456,
  "user_id": 123
}
```

### Sharing & Collaboration APIs

#### 18. Share Project
```http
POST /api/share-project/
```
**Payload:**
```json
{
  "project_id": 456,
  "shared_with_email": "collaborator@example.com",
  "permission_level": "edit",
  "files_to_share": ["file1.xlsx", "file2.csv"]
}
```

#### 19. Get Shared Projects
```http
GET /api/get-shared-projects/
```
**Query Parameters:**
- `user_id`: User identifier

#### 20. Remove Project Share
```http
POST /api/remove-project-share/
```
**Payload:**
```json
{
  "share_id": 789,
  "user_id": 123
}
```

### Version Control APIs

#### 21. Get Commits
```http
POST /api/getCommits/
```
**Payload:**
```json
{
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123
}
```

#### 22. Undo/Redo Operations
```http
POST /api/undo-redo/
```
**Payload:**
```json
{
  "action": "undo|redo",
  "sheet_name": "Sheet1",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "user_id": 123,
  "commit_hash": "abc123"
}
```

### Cloud Integration APIs

#### 23. Create Google Sheet
```http
POST /api/create_google_sheet/
```
**Payload:**
```json
{
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "sheet_name": "Sheet1"
}
```

#### 24. Update from Google Sheet
```http
POST /api/update_from_google_sheet/
```
**Payload:**
```json
{
  "google_sheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
  "file_type": "kpi",
  "file_name": "data.xlsx",
  "project_id": 456,
  "sheet_name": "Sheet1"
}
```

### Logging & Monitoring APIs

#### 25. Get API Logs
```http
GET /api/logs/api/
```
**Query Parameters:**
- `level`: user|project|sheet
- `user_id`: User identifier
- `project_id`: Project identifier
- `page`: Page number

#### 26. Get API Log Statistics
```http
GET /api/logs/api/stats/
```
**Response:**
```json
{
  "total_requests": 1000,
  "average_response_time": 0.5,
  "error_rate": 0.02,
  "most_used_endpoints": [
    {"endpoint": "/api/sheet-page/", "count": 200},
    {"endpoint": "/api/save-eda-plot/", "count": 150}
  ]
}
```

## Configuration & Settings

### Environment Variables
```python
# Database
DATABASE_URL = "sqlite:///db.sqlite3"  # or PostgreSQL URL

# Security
SECRET_KEY = "django-insecure-..."
DEBUG = True
ALLOWED_HOSTS = ['*']

# File Upload
DATA_UPLOAD_MAX_MEMORY_SIZE = 1024 * 1024 * 1024  # 1 GB
FILE_UPLOAD_MAX_MEMORY_SIZE = 1024 * 1024 * 1024  # 1 GB

# CORS
CORS_ALLOW_ALL_ORIGINS = True

# Media Files
MEDIA_URL = '/media/'
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')

# Spark Configuration
JAVA_HOME = r"C:\Users\harsh\java\jdk-17"
HADOOP_HOME = r"C:\hadoop"
SPARK_LOCAL_DIRS = "C:/Users/harsh/Documents/skewb/dashboard/Dashboard-backend/spark-temp"

# Google API
GOOGLE_SERVICE_ACCOUNT_INFO = "{...}"  # JSON string
GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# OneDrive API
ONEDRIVE_CLIENT_ID = "..."
ONEDRIVE_CLIENT_SECRET = "..."
ONEDRIVE_TENANT_ID = "..."
```

### Django Settings
```python
# REST Framework
REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework_simplejwt.authentication.JWTAuthentication',
    ),
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.IsAuthenticated',
    ),
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 20
}

# JWT Settings
SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(minutes=60),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=7),
    'ROTATE_REFRESH_TOKENS': True,
}

# CORS Settings
CORS_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

# Cache Settings
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
    }
}
```

## Hardcoded Data & Values

### 1. Spark Configuration
```python
# Environment paths (hardcoded for Windows development)
JAVA_HOME = r"C:\Users\harsh\java\jdk-17"
HADOOP_HOME = r"C:\hadoop"
SPARK_LOCAL_DIRS = "C:/Users/harsh/Documents/skewb/dashboard/Dashboard-backend/spark-temp"

# Spark session configuration
SPARK_CONFIG = {
    "spark.app.name": "SanitifyDataProcessing",
    "spark.master": "local[*]",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "2g"
}
```

### 2. File Processing Constants
```python
# Supported file types
SUPPORTED_FILE_TYPES = ['xlsx', 'xls', 'csv']
SUPPORTED_MIME_TYPES = [
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.ms-excel',
    'text/csv'
]

# File size limits
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
MAX_UPLOAD_SIZE = 1024 * 1024 * 1024  # 1 GB

# Pagination defaults
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 5000
```

### 3. Chart Configuration
```python
# Default chart colors
DEFAULT_CHART_COLORS = [
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
    '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9'
]

# Chart types
SUPPORTED_CHART_TYPES = [
    'bar', 'line', 'pie', 'scatter', 'area', 'heatmap',
    'box', 'histogram', 'correlation', 'distribution'
]

# Default chart dimensions
DEFAULT_CHART_WIDTH = 800
DEFAULT_CHART_HEIGHT = 400
```

### 4. Data Processing Defaults
```python
# Data type mapping
DATA_TYPE_MAPPING = {
    'string': 'object',
    'numeric': 'float64',
    'datetime': 'datetime64[ns]',
    'boolean': 'bool'
}

# Aggregation methods
AGGREGATION_METHODS = [
    'sum', 'mean', 'count', 'min', 'max', 'median', 'std', 'var'
]

# Date grouping options
DATE_GROUPING_OPTIONS = [
    'raw', 'year', 'month', 'quarter', 'week', 'day', 'hour'
]
```

### 5. Security Constants
```python
# OTP configuration
OTP_LENGTH = 6
OTP_EXPIRY_MINUTES = 10
OTP_ATTEMPTS_LIMIT = 3

# Password requirements
MIN_PASSWORD_LENGTH = 8
PASSWORD_REQUIREMENTS = {
    'min_length': 8,
    'require_uppercase': True,
    'require_lowercase': True,
    'require_numbers': True,
    'require_special_chars': False
}

# Session timeout
SESSION_TIMEOUT_MINUTES = 60
REFRESH_TOKEN_LIFETIME_DAYS = 7
```

### 6. API Rate Limiting
```python
# Rate limiting configuration
RATE_LIMITS = {
    'login': '5/minute',
    'upload': '10/hour',
    'processing': '20/hour',
    'general': '100/minute'
}

# Request size limits
MAX_REQUEST_SIZE = 50 * 1024 * 1024  # 50 MB
MAX_RESPONSE_SIZE = 100 * 1024 * 1024  # 100 MB
```

### 7. Logging Configuration
```python
# Log levels
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

# Log retention
LOG_RETENTION_DAYS = 30
MAX_LOG_ENTRIES = 10000

# Excluded endpoints from logging
EXCLUDED_ENDPOINTS = [
    'getCommits', 'getCommitsArray', 'auth', 'projects',
    'get-shared-projects', 'save-pivot', 'save-plots',
    'fetch-pivot-plots', 'update-sheet-data', 'upload'
]
```

## Control Flow Graph

```javascript
// Backend Control Flow Graph Code
class SanitifyBackendControlFlow {
  constructor() {
    this.nodes = [];
    this.edges = [];
    this.initializeFlow();
  }

  initializeFlow() {
    // Request Entry Points
    this.addNode("Request", "HTTP Request Entry", "entry");
    this.addNode("Authentication", "JWT Authentication", "auth");
    this.addNode("Authorization", "Permission Check", "auth");
    
    // Core API Endpoints
    this.addNode("AuthAPI", "Authentication APIs", "api");
    this.addNode("ProjectAPI", "Project Management APIs", "api");
    this.addNode("FileAPI", "File Management APIs", "api");
    this.addNode("DataAPI", "Data Processing APIs", "api");
    this.addNode("VizAPI", "Visualization APIs", "api");
    this.addNode("ShareAPI", "Sharing APIs", "api");
    this.addNode("CloudAPI", "Cloud Integration APIs", "api");
    this.addNode("LogAPI", "Logging APIs", "api");
    
    // Data Processing Flow
    this.addNode("SparkSession", "Spark Session Management", "processing");
    this.addNode("DataCleaning", "Data Cleaning Engine", "processing");
    this.addNode("DataMelting", "Data Melting Engine", "processing");
    this.addNode("DataMapping", "Data Mapping Engine", "processing");
    this.addNode("CustomScript", "Custom Script Engine", "processing");
    this.addNode("PivotTable", "Pivot Table Generator", "processing");
    
    // Visualization Flow
    this.addNode("ChartGenerator", "Chart Generation Engine", "viz");
    this.addNode("EDAAnalysis", "EDA Analysis Engine", "viz");
    this.addNode("PlotlyEngine", "Plotly Visualization", "viz");
    this.addNode("ExportEngine", "Export Generation", "viz");
    
    // Database Operations
    this.addNode("Database", "SQLite/PostgreSQL", "database");
    this.addNode("Models", "Django Models", "database");
    this.addNode("Serializers", "Data Serialization", "database");
    
    // File System Operations
    this.addNode("FileSystem", "File Storage", "storage");
    this.addNode("MediaStorage", "Media Files", "storage");
    this.addNode("SparkTemp", "Spark Temporary Files", "storage");
    
    // Cloud Services
    this.addNode("GoogleSheets", "Google Sheets API", "cloud");
    this.addNode("OneDrive", "OneDrive API", "cloud");
    this.addNode("OAuth", "OAuth Authentication", "cloud");
    
    // Response Generation
    this.addNode("Response", "HTTP Response", "response");
    this.addNode("ErrorHandler", "Error Handling", "response");
    this.addNode("Logging", "API Logging", "response");
    
    this.createEdges();
  }

  addNode(id, label, category) {
    this.nodes.push({
      id,
      label,
      category,
      type: this.getNodeType(category)
    });
  }

  getNodeType(category) {
    const types = {
      entry: "circle",
      auth: "diamond",
      api: "rectangle",
      processing: "hexagon",
      viz: "triangle",
      database: "square",
      storage: "square",
      cloud: "star",
      response: "octagon"
    };
    return types[category] || "rectangle";
  }

  createEdges() {
    // Request Flow
    this.addEdge("Request", "Authentication", "Validate JWT token");
    this.addEdge("Authentication", "Authorization", "Check permissions");
    this.addEdge("Authorization", "AuthAPI", "Authentication requests");
    this.addEdge("Authorization", "ProjectAPI", "Project requests");
    this.addEdge("Authorization", "FileAPI", "File requests");
    this.addEdge("Authorization", "DataAPI", "Data processing requests");
    this.addEdge("Authorization", "VizAPI", "Visualization requests");
    this.addEdge("Authorization", "ShareAPI", "Sharing requests");
    this.addEdge("Authorization", "CloudAPI", "Cloud integration requests");
    this.addEdge("Authorization", "LogAPI", "Logging requests");
    
    // Data Processing Flow
    this.addEdge("DataAPI", "SparkSession", "Initialize Spark session");
    this.addEdge("SparkSession", "DataCleaning", "Data cleaning requests");
    this.addEdge("SparkSession", "DataMelting", "Data melting requests");
    this.addEdge("SparkSession", "DataMapping", "Data mapping requests");
    this.addEdge("SparkSession", "CustomScript", "Custom script execution");
    this.addEdge("SparkSession", "PivotTable", "Pivot table generation");
    
    // Visualization Flow
    this.addEdge("VizAPI", "ChartGenerator", "Chart generation requests");
    this.addEdge("VizAPI", "EDAAnalysis", "EDA analysis requests");
    this.addEdge("ChartGenerator", "PlotlyEngine", "Interactive charts");
    this.addEdge("EDAAnalysis", "PlotlyEngine", "Statistical plots");
    this.addEdge("PlotlyEngine", "ExportEngine", "Export requests");
    
    // Database Operations
    this.addEdge("AuthAPI", "Models", "User operations");
    this.addEdge("ProjectAPI", "Models", "Project operations");
    this.addEdge("FileAPI", "Models", "File operations");
    this.addEdge("Models", "Database", "CRUD operations");
    this.addEdge("Database", "Serializers", "Data serialization");
    
    // File System Operations
    this.addEdge("FileAPI", "FileSystem", "File storage");
    this.addEdge("FileSystem", "MediaStorage", "Media file storage");
    this.addEdge("SparkSession", "SparkTemp", "Temporary file storage");
    
    // Cloud Integration
    this.addEdge("CloudAPI", "OAuth", "OAuth authentication");
    this.addEdge("OAuth", "GoogleSheets", "Google Sheets integration");
    this.addEdge("OAuth", "OneDrive", "OneDrive integration");
    
    // Response Flow
    this.addEdge("AuthAPI", "Response", "Authentication response");
    this.addEdge("ProjectAPI", "Response", "Project response");
    this.addEdge("FileAPI", "Response", "File response");
    this.addEdge("DataCleaning", "Response", "Cleaning response");
    this.addEdge("DataMelting", "Response", "Melting response");
    this.addEdge("DataMapping", "Response", "Mapping response");
    this.addEdge("CustomScript", "Response", "Script response");
    this.addEdge("PivotTable", "Response", "Pivot response");
    this.addEdge("ChartGenerator", "Response", "Chart response");
    this.addEdge("EDAAnalysis", "Response", "EDA response");
    this.addEdge("ExportEngine", "Response", "Export response");
    this.addEdge("ShareAPI", "Response", "Sharing response");
    this.addEdge("CloudAPI", "Response", "Cloud response");
    this.addEdge("LogAPI", "Response", "Log response");
    
    // Error Handling
    this.addEdge("Authentication", "ErrorHandler", "Authentication failed");
    this.addEdge("Authorization", "ErrorHandler", "Authorization failed");
    this.addEdge("DataCleaning", "ErrorHandler", "Processing error");
    this.addEdge("DataMelting", "ErrorHandler", "Processing error");
    this.addEdge("DataMapping", "ErrorHandler", "Processing error");
    this.addEdge("CustomScript", "ErrorHandler", "Script error");
    this.addEdge("ChartGenerator", "ErrorHandler", "Chart error");
    this.addEdge("EDAAnalysis", "ErrorHandler", "Analysis error");
    this.addEdge("Database", "ErrorHandler", "Database error");
    this.addEdge("FileSystem", "ErrorHandler", "File system error");
    this.addEdge("GoogleSheets", "ErrorHandler", "Google API error");
    this.addEdge("OneDrive", "ErrorHandler", "OneDrive API error");
    
    // Logging
    this.addEdge("Request", "Logging", "Log request");
    this.addEdge("Response", "Logging", "Log response");
    this.addEdge("ErrorHandler", "Logging", "Log error");
  }

  addEdge(from, to, label) {
    this.edges.push({
      from,
      to,
      label,
      arrows: "to"
    });
  }

  // Generate Mermaid.js diagram code
  generateMermaidDiagram() {
    let diagram = "graph TD\n";
    
    // Add nodes
    this.nodes.forEach(node => {
      const shape = this.getMermaidShape(node.type);
      diagram += `  ${node.id}${shape}"${node.label}"\n`;
    });
    
    diagram += "\n";
    
    // Add edges
    this.edges.forEach(edge => {
      diagram += `  ${edge.from} -->|"${edge.label}"| ${edge.to}\n`;
    });
    
    return diagram;
  }

  getMermaidShape(type) {
    const shapes = {
      circle: "(((",
      diamond: "{{",
      rectangle: "[",
      square: "[",
      hexagon: "{{",
      triangle: ">",
      star: "*",
      octagon: "{{"
    };
    return shapes[type] || "[";
  }

  // Generate detailed flow description
  generateFlowDescription() {
    return {
      entryPoints: ["Request"],
      authenticationFlow: ["Authentication", "Authorization"],
      apiEndpoints: {
        auth: ["AuthAPI"],
        project: ["ProjectAPI"],
        file: ["FileAPI"],
        data: ["DataAPI"],
        visualization: ["VizAPI"],
        sharing: ["ShareAPI"],
        cloud: ["CloudAPI"],
        logging: ["LogAPI"]
      },
      dataProcessingFlow: {
        spark: ["SparkSession"],
        cleaning: ["DataCleaning"],
        melting: ["DataMelting"],
        mapping: ["DataMapping"],
        scripts: ["CustomScript"],
        pivot: ["PivotTable"]
      },
      visualizationFlow: {
        charts: ["ChartGenerator"],
        eda: ["EDAAnalysis"],
        plotly: ["PlotlyEngine"],
        export: ["ExportEngine"]
      },
      dataLayer: {
        database: ["Database"],
        models: ["Models"],
        serializers: ["Serializers"]
      },
      storageLayer: {
        files: ["FileSystem"],
        media: ["MediaStorage"],
        temp: ["SparkTemp"]
      },
      cloudIntegration: {
        oauth: ["OAuth"],
        google: ["GoogleSheets"],
        onedrive: ["OneDrive"]
      },
      responseHandling: {
        success: ["Response"],
        error: ["ErrorHandler"],
        logging: ["Logging"]
      }
    };
  }
}

// Usage example:
const backendFlow = new SanitifyBackendControlFlow();
const mermaidDiagram = backendFlow.generateMermaidDiagram();
const flowDescription = backendFlow.generateFlowDescription();

console.log("Backend Mermaid Diagram:");
console.log(mermaidDiagram);
console.log("\nBackend Flow Description:");
console.log(JSON.stringify(flowDescription, null, 2));
```

### Key Backend Control Flow Patterns

1. **Request Processing Flow**:
   - HTTP Request → Authentication → Authorization → API Endpoints
   - JWT token validation at entry point
   - Permission-based routing to specific APIs

2. **Data Processing Flow**:
   - API Request → Spark Session → Data Processing Engine → Response
   - Spark session management for distributed processing
   - Multiple processing engines for different operations

3. **Visualization Flow**:
   - Visualization API → Chart/EDA Engine → Plotly → Export → Response
   - Interactive chart generation using Plotly
   - Multiple export formats supported

4. **Database Flow**:
   - API Request → Models → Database → Serializers → Response
   - Django ORM for database operations
   - JSON serialization for API responses

5. **Error Handling Flow**:
   - Any processing step → Error Handler → Logging → Error Response
   - Comprehensive error handling at all levels
   - Detailed error logging for debugging

6. **Cloud Integration Flow**:
   - Cloud API → OAuth → Cloud Service → Response
   - OAuth-based authentication for cloud services
   - Bidirectional data synchronization

This documentation provides a comprehensive overview of the Sanitify backend application, including its architecture, features, database models, API endpoints, configuration, hardcoded values, and control flow patterns.
