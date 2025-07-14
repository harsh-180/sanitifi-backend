from django.utils import timezone
from .log_utils import log_api_request, log_api_request_from_view
import traceback


class APILoggingMiddleware:
    """
    Middleware to automatically log all API requests and responses
    """
    
    def __init__(self, get_response):
        self.get_response = get_response
    
    def __call__(self, request):
        # Record start time
        start_time = timezone.now()
        
        # Debug logging for middleware
        print(f"=== Middleware called for {request.method} {request.path} ===")
        print(f"Content-Type: {request.META.get('CONTENT_TYPE', 'N/A')}")
        
        # Ensure request body is read (this is important for middleware)
        if hasattr(request, 'body'):
            try:
                # Force read the body if it hasn't been read yet
                if hasattr(request, '_body') and request._body is None:
                    request._body = request.read()
                print(f"Request body length: {len(request.body) if request.body else 0}")
                
                # For JSON requests, ensure the body is properly accessible
                if request.META.get('CONTENT_TYPE', '').startswith('application/json'):
                    try:
                        # Try to access the body as a string
                        if isinstance(request.body, bytes):
                            body_str = request.body.decode('utf-8')
                            print(f"JSON body preview: {body_str[:200]}...")
                        else:
                            print(f"Body type: {type(request.body)}")
                    except Exception as e:
                        print(f"Error accessing body content: {str(e)}")
                        
            except Exception as e:
                print(f"Error reading request body in middleware: {str(e)}")
        
        # Process the request
        try:
            response = self.get_response(request)
            # Log successful request
            print(f"Middleware: Logging successful request")
            
            # Try to use the view-based logging first (which has access to parsed data)
            try:
                log_api_request_from_view(request, response, start_time=start_time)
                print("Used view-based logging successfully")
            except Exception as e:
                print(f"View-based logging failed: {str(e)}, falling back to middleware logging")
                # Fallback to middleware-based logging
                log_api_request(request, response, start_time=start_time)
            
            return response
        except Exception as e:
            # Log failed request
            print(f"Middleware: Logging failed request with error: {str(e)}")
            try:
                log_api_request_from_view(request, None, start_time=start_time)
            except:
                log_api_request(request, error=e, start_time=start_time)
            raise
    
    def process_exception(self, request, exception):
        """
        Handle exceptions that occur during request processing
        """
        try:
            print(f"Middleware: Processing exception: {str(exception)}")
            try:
                log_api_request_from_view(request, None, error=exception)
            except:
                log_api_request(request, error=exception)
        except Exception as e:
            # If logging fails, don't break the application
            print(f"Failed to log API exception: {str(e)}")
        
        return None 