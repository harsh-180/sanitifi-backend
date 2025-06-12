from django.contrib.auth.backends import BaseBackend
from .models import User

class CustomAuthBackend(BaseBackend):
    def authenticate( username=None, password=None):
        try:
            user = User.objects.get(username=username)  # You can also check by email
            print(user)
            if user.check_password(password):
               
                return user
        except User.DoesNotExist:
            return None

   
