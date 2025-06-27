from .models import UserActionLog

def log_user_action(user, action, details=None, ip_address=None):
    UserActionLog.objects.create(
        user=user,
        action=action,
        details=details,
        ip_address=ip_address
    ) 