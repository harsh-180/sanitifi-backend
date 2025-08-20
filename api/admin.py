from django.contrib import admin
from .models import User, Projects, ProjectShare, UserActionLog, APILog, OTPToken  # Import your model
import json

class userModel(admin.ModelAdmin):
    list_display=['id','username','email','password']

class UserModel(admin.ModelAdmin):
    list_display=['id','name','user']

class ProjectShareModel(admin.ModelAdmin):
    list_display=['id','project','shared_by','shared_with','share_type','permission_level','is_active','created_at']
    list_filter=['share_type','permission_level','is_active','created_at']
    search_fields=['project__name','shared_by__username','shared_by__email','shared_with__username','shared_with__email']

class UserActionLogModel(admin.ModelAdmin):
    list_display=['id','user','action','timestamp','ip_address']
    list_filter=['action','timestamp']
    search_fields=['user__username','user__email','action','details']
    readonly_fields=['timestamp']
    actions = ['delete_selected']  # Enables the default delete action
    list_per_page = 200

class APILogModel(admin.ModelAdmin):
    list_display=['id','method','endpoint','user_display','response_status','request_timestamp','duration_ms']
    list_filter=['method','response_status','request_timestamp']
    search_fields=['user__username','user__email','endpoint','error_message']
    readonly_fields=['request_timestamp','response_timestamp','duration_ms']
    
    def user_display(self, obj):
        """Display user information in a more readable format"""
        if obj.user:
            return f"{obj.user.username or obj.user.email} (ID: {obj.user.id})"
        return "No user"
    user_display.short_description = 'User'
    
    # Add fieldsets for better organization
    fieldsets = (
        ('Basic Information', {
            'fields': ('user', 'endpoint', 'method', 'ip_address', 'user_agent')
        }),
        ('Request Details', {
            'fields': ('request_payload', 'request_headers', 'request_params', 'request_files'),
            'classes': ('collapse',)
        }),
        ('Response Details', {
            'fields': ('response_status', 'response_data', 'response_headers'),
            'classes': ('collapse',)
        }),
        ('Timing & Context', {
            'fields': ('request_timestamp', 'response_timestamp', 'duration_ms', 'project_id', 'file_type', 'file_name', 'sheet_name')
        }),
        ('Error Information', {
            'fields': ('error_message', 'error_traceback'),
            'classes': ('collapse',)
        }),
    )
    
    # Make the model read-only in admin
    def has_add_permission(self, request):
        return False
    
    def has_change_permission(self, request, obj=None):
        return False
    
    def has_delete_permission(self, request, obj=None):
        return True  # Allow deletion for cleanup

class OTPTokenAdmin(admin.ModelAdmin):
    list_display = ['user', 'otp', 'created_at', 'expires_at', 'is_used', 'is_valid']
    list_filter = ['is_used', 'created_at']
    search_fields = ['user__username', 'user__email']
    readonly_fields = ['created_at']

admin.site.register(User, userModel)
admin.site.register(Projects, UserModel)
admin.site.register(ProjectShare, ProjectShareModel)
admin.site.register(UserActionLog, UserActionLogModel)
admin.site.register(APILog, APILogModel)
admin.site.register(OTPToken, OTPTokenAdmin)


