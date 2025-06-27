from django.contrib import admin
from .models import User, Projects, ProjectShare, UserActionLog  # Import your model

class userModel(admin.ModelAdmin):
    list_display=['id','username','email','password']

class UserModel(admin.ModelAdmin):
    list_display=['id','name','user']

class ProjectShareModel(admin.ModelAdmin):
    list_display=['id','project','shared_by','shared_with','share_type','permission_level','is_active','created_at']
    list_filter=['share_type','permission_level','is_active','created_at']
    search_fields=['project__name','shared_by__username','shared_by__email','shared_with__username','shared_with__email']

admin.site.register(User, userModel)
admin.site.register(Projects, UserModel)
admin.site.register(ProjectShare, ProjectShareModel)
admin.site.register(UserActionLog)


