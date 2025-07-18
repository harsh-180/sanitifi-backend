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