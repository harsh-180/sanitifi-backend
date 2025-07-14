from .models import UserActionLog, APILog, User
import json
import time
from django.utils import timezone
from django.http import JsonResponse
from rest_framework.response import Response
import traceback

def log_user_action(user, action, details=None, ip_address=None):
    UserActionLog.objects.create(
        user=user,
        action=action,
        details=details,
        ip_address=ip_address
    )


def sanitize_data_for_logging(data):
    """
    Sanitize sensitive data before logging
    """
    if not isinstance(data, dict):
        return data
    
    # Create a copy to avoid modifying the original
    sanitized = data.copy()
    
    # List of sensitive fields to mask
    sensitive_fields = [
        'password', 'token', 'access', 'refresh', 'secret', 'key', 
        'authorization', 'auth', 'credential', 'private'
    ]
    
    def mask_sensitive_values(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if any(sensitive in key.lower() for sensitive in sensitive_fields):
                    obj[key] = '***MASKED***'
                elif isinstance(value, (dict, list)):
                    mask_sensitive_values(value)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    mask_sensitive_values(item)
    
    mask_sensitive_values(sanitized)
    return sanitized


def extract_request_data(request):
    """
    Extract and sanitize request data for logging
    """
    print(f"=== extract_request_data called ===")
    print(f"Request path: {request.path}")
    print(f"Request method: {request.method}")
    print(f"Content-Type: {request.META.get('CONTENT_TYPE', 'N/A')}")
    
    request_data = {
        'payload': {},
        'headers': {},
        'params': {},
        'files': {},
        'body': '',
        'content_type': ''
    }
    
    # Extract content type
    request_data['content_type'] = request.META.get('CONTENT_TYPE', '')
    print(f"Extracted content type: {request_data['content_type']}")
    
    # Extract raw request body
    try:
        if hasattr(request, 'body'):
            # For DRF requests, body might be bytes
            if isinstance(request.body, bytes):
                request_data['body'] = request.body.decode('utf-8', errors='ignore')
            else:
                request_data['body'] = str(request.body)
            print(f"Raw body length: {len(request_data['body'])}")
            print(f"Raw body preview: {request_data['body'][:200]}...")
        else:
            print("Request has no body attribute")
    except Exception as e:
        request_data['body'] = f"Error reading body: {str(e)}"
        print(f"Error reading body: {str(e)}")
    
    # Extract request payload (JSON data)
    if hasattr(request, 'data') and request.data:
        print("Using request.data for payload")
        request_data['payload'] = sanitize_data_for_logging(request.data)
    else:
        print("request.data is empty, trying to parse raw body")
        # If request.data is empty, try to parse the raw body as JSON
        # This happens when middleware runs before DRF parses the request
        try:
            if request_data['body'] and request_data['content_type'] and 'application/json' in request_data['content_type']:
                print("Attempting to parse raw body as JSON")
                parsed_body = json.loads(request_data['body'])
                request_data['payload'] = sanitize_data_for_logging(parsed_body)
                print(f"Successfully parsed JSON payload with keys: {list(request_data['payload'].keys())}")
            else:
                print(f"Not parsing as JSON - body: {bool(request_data['body'])}, content_type: {request_data['content_type']}")
        except (json.JSONDecodeError, ValueError) as e:
            # If JSON parsing fails, that's okay - just log the raw body
            print(f"Could not parse request body as JSON: {str(e)}")
    
    # Extract POST data (form data)
    if hasattr(request, 'POST') and request.POST:
        post_data = dict(request.POST)
        # Remove sensitive fields from POST data
        sensitive_fields = ['password', 'token', 'access', 'refresh', 'secret', 'key']
        for field in sensitive_fields:
            post_data.pop(field, None)
        request_data['payload'].update(sanitize_data_for_logging(post_data))
    
    # Extract query parameters
    if hasattr(request, 'query_params') and request.query_params:
        request_data['params'] = dict(request.query_params)
    elif hasattr(request, 'GET') and request.GET:
        request_data['params'] = dict(request.GET)
    
    # Extract file information (without the actual files)
    if hasattr(request, 'FILES') and request.FILES:
        files_info = {}
        for field_name, file_obj in request.FILES.items():
            files_info[field_name] = {
                'name': file_obj.name,
                'size': file_obj.size,
                'content_type': getattr(file_obj, 'content_type', 'unknown')
            }
        request_data['files'] = files_info
    
    # Extract headers (sanitize sensitive ones)
    if hasattr(request, 'headers'):
        headers = dict(request.headers)
        # Remove sensitive headers
        sensitive_headers = ['authorization', 'cookie', 'x-csrftoken', 'x-api-key']
        for header in sensitive_headers:
            headers.pop(header, None)
        request_data['headers'] = headers
    
    print(f"Final payload keys: {list(request_data['payload'].keys())}")
    print(f"=== extract_request_data completed ===")
    
    return request_data


def extract_response_data(response):
    """
    Extract response data for logging
    """
    response_data = {
        'data': {},
        'headers': {}
    }
    
    # Extract response data
    if hasattr(response, 'data'):
        response_data['data'] = sanitize_data_for_logging(response.data)
    
    # Extract response headers
    if hasattr(response, 'headers'):
        response_data['headers'] = dict(response.headers)
    
    return response_data


def log_api_request(request, response=None, error=None, start_time=None):
    """
    Log comprehensive API request and response information
    
    Args:
        request: Django request object
        response: Django response object (optional)
        error: Exception object if an error occurred (optional)
        start_time: Timestamp when request started (optional)
    """
    try:
        print(f"=== API Logging Started ===")
        print(f"Endpoint: {request.path}")
        print(f"Method: {request.method}")
        print(f"Content-Type: {request.META.get('CONTENT_TYPE', 'N/A')}")
        
        # Extract request data first to get user_id from payload
        request_data = extract_request_data(request)
        
        # Priority 1: Try to get user from user_id in payload
        user = None
        user_id_from_payload = request_data['payload'].get('user_id')
        if user_id_from_payload:
            try:
                user = User.objects.get(id=user_id_from_payload)
                print(f"API Log - Found user from payload user_id: {user}")
            except User.DoesNotExist:
                print(f"API Log - User not found with ID from payload: {user_id_from_payload}")
            except Exception as e:
                print(f"API Log - Error getting user from payload user_id: {e}")
        
        # Priority 2: If no user from payload, use the existing get_logging_user function
        if user is None:
            from .views import get_logging_user
            user = get_logging_user(request, None)
            print(f"API Log - User detection from get_logging_user: user={user}, type={type(user)}")
        
        # Resolve SimpleLazyObject if present
        if user and hasattr(user, '_wrapped'):
            try:
                user = user._wrapped
                print(f"API Log - Resolved SimpleLazyObject to: {user}")
            except Exception as e:
                print(f"API Log - Error resolving SimpleLazyObject: {e}")
                user = None
        elif user and str(type(user)).find('SimpleLazyObject') != -1:
            # This is a SimpleLazyObject, try to resolve it
            try:
                # Force evaluation by accessing a property
                user_id = user.id if hasattr(user, 'id') else None
                if user_id:
                    user = User.objects.get(id=user_id)
                    print(f"API Log - Resolved SimpleLazyObject from database: {user}")
                else:
                    print(f"API Log - SimpleLazyObject has no id, setting user to None")
                    user = None
            except Exception as e:
                print(f"API Log - Error resolving SimpleLazyObject from database: {e}")
                user = None
        
        # Final validation - ensure user is a proper User instance
        if user and not isinstance(user, User):
            try:
                # If user is not a User instance, try to get it from database
                if hasattr(user, 'id'):
                    user = User.objects.get(id=user.id)
                    print(f"API Log - Converted to User instance: {user}")
                else:
                    print(f"API Log - User object has no id, setting to None")
                    user = None
            except Exception as e:
                print(f"API Log - Error converting to User instance: {e}")
                user = None
        
        # Priority 3: If still None, try to get from project (same pattern as existing views)
        if user is None and request_data['payload']:
            project_id = request_data['payload'].get('project_id')
            if project_id:
                from .models import Projects
                try:
                    project = Projects.objects.get(id=project_id)
                    user = project.user  # Use project.user as fallback (same as existing views)
                    print(f"API Log - Found user from project: {user}")
                except Projects.DoesNotExist:
                    print(f"API Log - Project not found: {project_id}")
                    pass
        
        # Debug logging for payload extraction
        print(f"API Log - Payload extraction debug:")
        print(f"  - Content-Type: {request_data['content_type']}")
        print(f"  - Raw body length: {len(request_data['body'])}")
        print(f"  - Raw body preview: {request_data['body'][:200]}...")
        print(f"  - Payload keys: {list(request_data['payload'].keys()) if request_data['payload'] else 'empty'}")
        print(f"  - User ID from payload: {user_id_from_payload}")
        print(f"  - Project ID from payload: {request_data['payload'].get('project_id')}")
        print(f"  - File type from payload: {request_data['payload'].get('file_type')}")
        print(f"  - File name from payload: {request_data['payload'].get('file_name')}")
        print(f"  - Sheet name from payload: {request_data['payload'].get('sheet_name')}")
        
        # Determine response status and data
        response_status = 500  # Default to 500 if error
        response_data = {}
        
        if error:
            error_message = str(error)
            error_traceback = traceback.format_exc()
        else:
            error_message = None
            error_traceback = None
            if response:
                response_status = response.status_code
                response_data = extract_response_data(response)
        
        # Calculate duration
        duration_ms = None
        if start_time:
            duration_ms = int((timezone.now() - start_time).total_seconds() * 1000)
        
        # Extract additional context from request data
        project_id = request_data['payload'].get('project_id')
        file_type = request_data['payload'].get('file_type')
        file_name = request_data['payload'].get('file_name')
        sheet_name = request_data['payload'].get('sheet_name')
        
        # Final check - ensure user is a valid User instance or None
        if user is not None and not isinstance(user, User):
            print(f"API Log - Final check failed: user is {type(user)}, setting to None")
            user = None
        
        print(f"API Log - Final values:")
        print(f"  - User: {user}")
        print(f"  - Project ID: {project_id}")
        print(f"  - File Type: {file_type}")
        print(f"  - File Name: {file_name}")
        print(f"  - Sheet Name: {sheet_name}")
        print(f"  - Response Status: {response_status}")
        print(f"  - Duration: {duration_ms}ms")
        
        # Create API log entry
        api_log = APILog.objects.create(
            user=user,
            endpoint=request.path,
            method=request.method,
            ip_address=request.META.get('REMOTE_ADDR'),
            user_agent=request.META.get('HTTP_USER_AGENT', ''),
            request_payload=request_data['payload'],
            request_headers=request_data['headers'],
            request_params=request_data['params'],
            request_files=request_data['files'],
            request_body=request_data['body'],
            request_content_type=request_data['content_type'],
            response_status=response_status,
            response_data=response_data['data'],
            response_headers=response_data['headers'],
            response_timestamp=timezone.now(),
            duration_ms=duration_ms,
            error_message=error_message,
            error_traceback=error_traceback,
            project_id=project_id,
            file_type=file_type,
            file_name=file_name,
            sheet_name=sheet_name
        )
        
        print(f"API Log created successfully: {api_log.id}")
        print(f"=== API Logging Completed ===")
        
        return api_log
        
    except Exception as e:
        # If logging fails, don't break the application
        print(f"Failed to log API request: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return None


def api_logging_middleware(get_response):
    """
    Django middleware for automatic API logging
    """
    def middleware(request):
        # Record start time
        start_time = timezone.now()
        
        # Process the request
        try:
            response = get_response(request)
            # Log successful request
            log_api_request(request, response, start_time=start_time)
            return response
        except Exception as e:
            # Log failed request
            log_api_request(request, error=e, start_time=start_time)
            raise
    
    return middleware


def log_api_call_with_context(request, response, context=None):
    """
    Enhanced API logging with additional context
    
    Args:
        request: Django request object
        response: Django response object
        context: Additional context dictionary (optional)
    """
    try:
        api_log = log_api_request(request, response)
        
        if api_log and context:
            # Update the log with additional context
            for key, value in context.items():
                if hasattr(api_log, key):
                    setattr(api_log, key, value)
            api_log.save()
        
        return api_log
        
    except Exception as e:
        print(f"Failed to log API call with context: {str(e)}")
        return None


def api_logging_decorator(func):
    """
    Decorator to manually log API calls for specific views
    """
    def wrapper(self, request, *args, **kwargs):
        start_time = timezone.now()
        
        try:
            response = func(self, request, *args, **kwargs)
            # Log successful request
            log_api_request_from_view(request, response, start_time=start_time)
            return response
        except Exception as e:
            # Log failed request
            log_api_request_from_view(request, None, start_time=start_time)
            raise
    
    return wrapper


def ensure_payload_logging(request, response=None, error=None):
    """
    Utility function to ensure payload logging is working
    """
    try:
        # Extract and log request data
        request_data = extract_request_data(request)
        
        print(f"=== API Payload Logging Debug ===")
        print(f"Endpoint: {request.path}")
        print(f"Method: {request.method}")
        print(f"Content-Type: {request_data['content_type']}")
        print(f"Payload: {json.dumps(request_data['payload'], indent=2)}")
        print(f"Body: {request_data['body'][:500]}...")  # First 500 chars
        print(f"Files: {request_data['files']}")
        print(f"=================================")
        
        # Log the actual API call
        return log_api_request(request, response, error)
        
    except Exception as e:
        print(f"Error in ensure_payload_logging: {str(e)}")
        return None 


def log_api_request_from_view(request, response, start_time=None, context=None, error=None):
    """
    Log API request from within a view where we have access to parsed request data
    
    Args:
        request: Django request object (with parsed data)
        response: Django response object
        start_time: Timestamp when request started (optional)
        context: Additional context dictionary (optional)
        error: Exception object if an error occurred (optional)
    """
    try:
        # Extract request data first to get user_id from payload
        request_data = extract_request_data(request)
        
        # Priority 1: Try to get user from user_id in payload
        user = None
        user_id_from_payload = request_data['payload'].get('user_id')
        if user_id_from_payload:
            try:
                user = User.objects.get(id=user_id_from_payload)
                print(f"API Log (view) - Found user from payload user_id: {user}")
            except User.DoesNotExist:
                print(f"API Log (view) - User not found with ID from payload: {user_id_from_payload}")
            except Exception as e:
                print(f"API Log (view) - Error getting user from payload user_id: {e}")
        
        # Priority 2: If no user from payload, use the existing get_logging_user function
        if user is None:
            from .views import get_logging_user
            user = get_logging_user(request, None)
            print(f"API Log (view) - User detection from get_logging_user: user={user}")
        
        # Extract request information
        endpoint = request.path
        method = request.method
        ip_address = request.META.get('REMOTE_ADDR')
        user_agent = request.META.get('HTTP_USER_AGENT', '')
        
        # Determine response status and data
        if error:
            response_status = 500
            response_data = {'data': {}, 'headers': {}}
            error_message = str(error)
            error_traceback = traceback.format_exc()
        else:
            response_status = response.status_code if response else 500
            response_data = extract_response_data(response) if response else {'data': {}, 'headers': {}}
            error_message = None
            error_traceback = None
        
        # Calculate duration
        duration_ms = None
        if start_time:
            duration_ms = int((timezone.now() - start_time).total_seconds() * 1000)
        
        # Extract additional context from request data
        project_id = request_data['payload'].get('project_id')
        file_type = request_data['payload'].get('file_type')
        file_name = request_data['payload'].get('file_name')
        sheet_name = request_data['payload'].get('sheet_name')
        
        # Apply additional context if provided
        if context:
            project_id = context.get('project_id', project_id)
            file_type = context.get('file_type', file_type)
            file_name = context.get('file_name', file_name)
            sheet_name = context.get('sheet_name', sheet_name)
        
        # Create API log entry
        api_log = APILog.objects.create(
            user=user,
            endpoint=endpoint,
            method=method,
            ip_address=ip_address,
            user_agent=user_agent,
            request_payload=request_data['payload'],
            request_headers=request_data['headers'],
            request_params=request_data['params'],
            request_files=request_data['files'],
            request_body=request_data['body'],
            request_content_type=request_data['content_type'],
            response_status=response_status,
            response_data=response_data['data'],
            response_headers=response_data['headers'],
            response_timestamp=timezone.now(),
            duration_ms=duration_ms,
            error_message=error_message,
            error_traceback=error_traceback,
            project_id=project_id,
            file_type=file_type,
            file_name=file_name,
            sheet_name=sheet_name
        )
        
        print(f"API Log created from view: {api_log.id} - {method} {endpoint} - Status: {response_status}")
        return api_log
        
    except Exception as e:
        # If logging fails, don't break the application
        print(f"Failed to log API request from view: {str(e)}")
        return None 