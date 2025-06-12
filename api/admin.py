from django.contrib import admin
from .models import User,Projects  # Import your model

class userModel(admin.ModelAdmin):
    list_display=['id','username','email','password']
admin.site.register(User, userModel)


class UserModel(admin.ModelAdmin):
    list_display = ['id', 'user', 'name', 'kpi_id', 'kpi_file', 'media_id', 'media_file']

admin.site.register(Projects, UserModel)


