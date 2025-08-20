# api/models.py
# from django.contrib.auth.models import AbstractUser
from django.db import models
from django.contrib.auth.hashers import make_password
import os
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone
from datetime import timedelta
import random
import string

class User(models.Model):
    email = models.EmailField(unique=True)
    username= models.CharField(max_length=255,blank=True)
    password = models.CharField(max_length=128)

    def set_password(self, raw_password):
        self.password = make_password(raw_password)
    
    def check_password(self, raw_password):
        # return check_password(raw_password, self.password)
        return raw_password==self.password

    def __str__(self):
        return self.username

class OTPToken(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    otp = models.CharField(max_length=6)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    is_used = models.BooleanField(default=False)
    
    def __str__(self):
        return f"OTP for {self.user.username} - {self.otp}"
    
    def is_expired(self):
        return timezone.now() > self.expires_at
    
    def is_valid(self):
        return not self.is_used and not self.is_expired()
    
    @classmethod
    def generate_otp(cls):
        """Generate a 6-digit OTP"""
        return ''.join(random.choices(string.digits, k=6))
    
    @classmethod
    def create_for_user(cls, user):
        """Create a new OTP token for a user"""
        # Delete any existing unused OTPs for this user
        cls.objects.filter(user=user, is_used=False).delete()
        
        # Create new OTP
        otp = cls.generate_otp()
        expires_at = timezone.now() + timedelta(minutes=10)  # OTP expires in 10 minutes
        
        return cls.objects.create(
            user=user,
            otp=otp,
            expires_at=expires_at
        )


class Projects(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    name = models.CharField(max_length=255)

    kpi_id = models.IntegerField(unique=True, null=True, blank=True)
    kpi_file = models.JSONField(default=list, blank=True, encoder=DjangoJSONEncoder)

    media_id = models.IntegerField(unique=True, null=True, blank=True)
    media_file = models.JSONField(default=list, blank=True, encoder=DjangoJSONEncoder)

    # Field to store concatenated file references
    concatenated_file = models.JSONField(default=list, blank=True, encoder=DjangoJSONEncoder)

    # Mapping: (file_type, file_name, sheet_name) -> google_sheet_id
    google_sheet_ids = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)

    def save(self, *args, **kwargs):
        """Assign unique incremental IDs for kpi_id and media_id and set proper file paths."""

        if self.kpi_file and self.kpi_id is None:
            last_kpi = Projects.objects.order_by('-kpi_id').first()
            self.kpi_id = (last_kpi.kpi_id + 1) if last_kpi and last_kpi.kpi_id else 1
        
        if self.media_file and self.media_id is None:
            last_media = Projects.objects.order_by('-media_id').first()
            self.media_id = (last_media.media_id + 1) if last_media and last_media.media_id else 1

        # Ensure kpi_file and media_file remain JSON-compatible
        project_folder = f"user_{self.user.id}/project_{self.id}"

        if isinstance(self.kpi_file, list) and len(self.kpi_file) > 0:
            self.kpi_file = [
                os.path.join(f"{project_folder}/kpi", os.path.basename(f)) for f in self.kpi_file
            ]

        if isinstance(self.media_file, list) and len(self.media_file) > 0:
            self.media_file = [
                os.path.join(f"{project_folder}/media", os.path.basename(f)) for f in self.media_file
            ]

        super().save(*args, **kwargs)

class SavedScript(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    title = models.CharField(max_length=255)
    script_content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ['-updated_at']

    def __str__(self):
        return f"{self.title} - {self.user.username}"

class SavedPlot(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    project = models.ForeignKey('Projects', on_delete=models.CASCADE)
    file_type = models.CharField(max_length=50)  # 'kpi' or 'media'
    file_name = models.CharField(max_length=255)
    sheet_name = models.CharField(max_length=255)
    plot_name = models.CharField(max_length=255, default='Default Plot')  # Added default value
    plot_config = models.JSONField()  # Stores plot configuration
    chart_data = models.JSONField()  # Stores chart data
    chart_options = models.JSONField()  # Stores chart options
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-updated_at']
        # Removed unique_together constraint to allow multiple plots per file/sheet

    def __str__(self):
        return f"{self.plot_name} - {self.file_name} - {self.sheet_name} - {self.user.username}"

class SavedPivot(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    project = models.ForeignKey('Projects', on_delete=models.CASCADE)
    pivot_name = models.CharField(max_length=255)
    file_type = models.CharField(max_length=10)  # 'media' or 'kpi'
    file_name = models.CharField(max_length=255)
    sheet_name = models.CharField(max_length=255)
    pivot_config = models.JSONField()  # Store rows, columns, values, filters
    pivot_data = models.JSONField()  # Store the actual pivot table data
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('user', 'project', 'pivot_name')

    def __str__(self):
        return f"{self.pivot_name} - {self.project.name}"

class SavedPivotPlot(models.Model):
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    project = models.ForeignKey('Projects', on_delete=models.CASCADE)
    pivot = models.ForeignKey('SavedPivot', on_delete=models.CASCADE)
    plot_name = models.CharField(max_length=255, default='Default Pivot Plot')
    plot_config = models.JSONField()  # Stores plot configuration
    chart_data = models.JSONField()  # Stores chart data
    chart_options = models.JSONField()  # Stores chart options
    active_filters = models.JSONField(default=dict, blank=True)  # Stores active filters separately
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-updated_at']
        unique_together = ('user', 'project', 'pivot', 'plot_name')

    def __str__(self):
        return f"{self.plot_name} - {self.pivot.pivot_name} - {self.user.username}"

class EDAPlot(models.Model):
    """
    Model to store EDA (Exploratory Data Analysis) plots
    """
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
    
    # Data source information
    file_type = models.CharField(max_length=50, blank=True, null=True)  # 'kpi', 'media', 'concatenated'
    file_name = models.CharField(max_length=255, blank=True, null=True)
    sheet_name = models.CharField(max_length=255, blank=True, null=True)
    
    # Plot configuration and data
    plot_config = models.JSONField(default=dict)  # Stores plot configuration (xAxes, yAxis, yAxes, chartType, etc.)
    chart_data = models.JSONField(default=dict)  # Stores the actual chart data
    chart_options = models.JSONField(default=dict)  # Stores chart styling and options
    
    # EDA-specific metadata
    eda_analysis_type = models.CharField(max_length=100, blank=True, null=True)  # e.g., 'numerical_analysis', 'categorical_analysis'
    columns_analyzed = models.JSONField(default=list)  # List of columns used in the analysis
    data_summary = models.JSONField(default=dict)  # Summary statistics of the data used
    
    # Additional metadata
    description = models.TextField(blank=True, null=True)
    tags = models.JSONField(default=list)  # For categorizing plots
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-updated_at']
        unique_together = ('user', 'project', 'plot_name')

    def __str__(self):
        return f"{self.plot_name} - {self.plot_type} - {self.user.username}"
    
    def get_y_axes(self):
        """
        Get Y-axes configuration, supporting both old (yAxis) and new (yAxes) formats
        """
        plot_config = self.plot_config or {}
        
        # Check for new format first (yAxes array)
        if 'yAxes' in plot_config and plot_config['yAxes']:
            return plot_config['yAxes']
        
        # Fall back to old format (single yAxis)
        if 'yAxis' in plot_config and plot_config['yAxis']:
            return [plot_config['yAxis']]
        
        return []
    
    def get_x_axes(self):
        """
        Get X-axes configuration
        """
        plot_config = self.plot_config or {}
        return plot_config.get('xAxes', [])
    
    def get_chart_type(self):
        """
        Get chart type
        """
        plot_config = self.plot_config or {}
        return plot_config.get('chartType', 'bar')
    
    def get_aggregation_method(self):
        """
        Get aggregation method
        """
        plot_config = self.plot_config or {}
        return plot_config.get('aggregationMethod', 'sum')
    
    def get_date_grouping(self):
        """
        Get date grouping configuration
        """
        plot_config = self.plot_config or {}
        return plot_config.get('dateGrouping', 'raw')

# Project Sharing Models
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
    
    # For file-specific sharing
    file_type = models.CharField(max_length=10, blank=True, null=True)  # 'kpi' or 'media'
    file_name = models.CharField(max_length=255, blank=True, null=True)
    sheet_name = models.CharField(max_length=255, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        unique_together = [
            ('project', 'shared_with', 'share_type', 'file_type', 'file_name', 'sheet_name')
        ]
        ordering = ['-created_at']
    
    def __str__(self):
        if self.share_type == 'project':
            return f"{self.project.name} shared with {self.shared_with.username}"
        else:
            return f"{self.project.name} - {self.file_name} shared with {self.shared_with.username}"
    
    def clean(self):
        from django.core.exceptions import ValidationError
        if self.share_type == 'file' and not all([self.file_type, self.file_name]):
            raise ValidationError("File type and file name are required for file-specific sharing")
        
        if self.share_type == 'project' and any([self.file_type, self.file_name, self.sheet_name]):
            raise ValidationError("File-specific fields should not be set for project-level sharing")
    
    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

class UserActionLog(models.Model):
    user = models.ForeignKey('User', on_delete=models.SET_NULL, null=True, blank=True)
    action = models.CharField(max_length=255)
    details = models.TextField(blank=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)

    def __str__(self):
        return f"{self.user} - {self.action} at {self.timestamp}"

class APILog(models.Model):
    """
    Comprehensive API logging model to store request and response information
    """
    # Basic request information
    user = models.ForeignKey('User', on_delete=models.SET_NULL, null=True, blank=True)
    endpoint = models.CharField(max_length=255)  # API endpoint path
    method = models.CharField(max_length=10)  # HTTP method (GET, POST, etc.)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True, null=True)
    
    # Request details
    request_payload = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
    request_headers = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
    request_params = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
    request_files = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
    request_body = models.TextField(blank=True, null=True)  # Raw request body
    request_content_type = models.CharField(max_length=100, blank=True, null=True)  # Content type
    
    # Response details
    response_status = models.IntegerField()  # HTTP status code
    response_data = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
    response_headers = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
    
    # Timing and metadata
    request_timestamp = models.DateTimeField(auto_now_add=True)
    response_timestamp = models.DateTimeField(null=True, blank=True)
    duration_ms = models.IntegerField(null=True, blank=True)  # Response time in milliseconds
    
    # Error information
    error_message = models.TextField(blank=True, null=True)
    error_traceback = models.TextField(blank=True, null=True)
    
    # Additional context
    project_id = models.IntegerField(null=True, blank=True)  # If applicable
    file_type = models.CharField(max_length=50, blank=True, null=True)  # If applicable
    file_name = models.CharField(max_length=255, blank=True, null=True)  # If applicable
    sheet_name = models.CharField(max_length=255, blank=True, null=True)  # If applicable
    
    class Meta:
        ordering = ['-request_timestamp']
        indexes = [
            models.Index(fields=['user', 'request_timestamp']),
            models.Index(fields=['endpoint', 'method']),
            models.Index(fields=['response_status']),
            models.Index(fields=['project_id']),
        ]
    
    def __str__(self):
        return f"{self.method} {self.endpoint} - {self.user} - {self.response_status} - {self.request_timestamp}"
    
    @property
    def is_success(self):
        """Check if the API call was successful (2xx status codes)"""
        return 200 <= self.response_status < 300
    
    @property
    def is_error(self):
        """Check if the API call resulted in an error (4xx or 5xx status codes)"""
        return self.response_status >= 400


class EDAFormat(models.Model):
    """
    Model to store EDA (Exploratory Data Analysis) format/flow templates
    """
    FORMAT_TYPES = [
        ('single_plot', 'Single Plot'),
        ('multi_plot', 'Multi Plot'),
        ('dashboard', 'Dashboard'),
        ('analysis_flow', 'Analysis Flow'),
    ]
    
    user = models.ForeignKey('User', on_delete=models.CASCADE)
    format_name = models.CharField(max_length=255)
    format_type = models.CharField(max_length=20, choices=FORMAT_TYPES, default='single_plot')
    
    # Format configuration
    format_config = models.JSONField(default=dict)  # Stores plot templates and layout
    required_columns = models.JSONField(default=list)  # Columns required for this format
    optional_columns = models.JSONField(default=list)  # Optional columns
    column_patterns = models.JSONField(default=dict)  # Flexible column matching patterns
    sample_data_structure = models.JSONField(default=dict)  # Sample data structure for validation
    
    # Metadata
    description = models.TextField(blank=True, null=True)
    tags = models.JSONField(default=list)  # For categorizing formats
    category = models.CharField(max_length=100, blank=True, null=True)
    
    # Usage tracking
    usage_count = models.IntegerField(default=0)
    last_used = models.DateTimeField(null=True, blank=True)
    
    # Version and status
    version = models.CharField(max_length=20, default='1.0')
    is_active = models.BooleanField(default=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-updated_at']
        unique_together = ('user', 'format_name')
    
    def __str__(self):
        return f"{self.format_name} ({self.format_type}) - {self.user.username}"
    
    def check_column_compatibility(self, available_columns):
        """
        Check if the available columns are compatible with this format
        Returns: (is_compatible, missing_columns, matched_columns)
        """
        if not self.required_columns:
            return True, [], available_columns
        
        missing_columns = []
        matched_columns = []
        
        for required_col in self.required_columns:
            # Try exact match first
            if required_col in available_columns:
                matched_columns.append(required_col)
                continue
            
            # Try pattern matching
            if required_col in self.column_patterns:
                pattern_config = self.column_patterns[required_col]
                pattern_type = pattern_config.get('type', 'exact')
                pattern_value = pattern_config.get('value', '')
                
                found_match = False
                for col in available_columns:
                    if pattern_type == 'contains' and pattern_value.lower() in col.lower():
                        matched_columns.append(col)
                        found_match = True
                        break
                    elif pattern_type == 'regex':
                        import re
                        try:
                            regex = re.compile(pattern_value, re.IGNORECASE)
                            if regex.search(col):
                                matched_columns.append(col)
                                found_match = True
                                break
                        except re.error:
                            continue
                
                if not found_match:
                    missing_columns.append(required_col)
            else:
                missing_columns.append(required_col)
        
        is_compatible = len(missing_columns) == 0
        return is_compatible, missing_columns, matched_columns
    
    def increment_usage(self):
        """Increment usage count and update last used timestamp"""
        self.usage_count += 1
        self.last_used = timezone.now()
        self.save(update_fields=['usage_count', 'last_used'])
    
    def get_plot_configs(self):
        """
        Get plot configurations from format_config, supporting multiple Y-axes
        """
        format_config = self.format_config or {}
        plots = format_config.get('plots', [])
        
        # Ensure each plot has both yAxis and yAxes for compatibility
        for plot in plots:
            plot_config = plot.get('plot_config', {})
            
            # Handle Y-axes configuration
            if 'yAxes' in plot_config and plot_config['yAxes']:
                # New format with multiple Y-axes
                y_axes = plot_config['yAxes']
                y_axis = y_axes[0] if y_axes else None
            elif 'yAxis' in plot_config and plot_config['yAxis']:
                # Old format with single Y-axis
                y_axis = plot_config['yAxis']
                y_axes = [y_axis]
            else:
                # No Y-axis specified
                y_axis = None
                y_axes = []
            
            # Update plot_config to ensure both formats are available
            plot_config['yAxis'] = y_axis
            plot_config['yAxes'] = y_axes
            plot['plot_config'] = plot_config
        
        return plots