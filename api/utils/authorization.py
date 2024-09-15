from flask import request, abort
from functools import wraps

def check_access(f):
    """
        Decorator function acting as an authorization checker.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_token = request.headers.get('Authorization')
        if auth_token != 'Bearer AuthToken':
            abort(401)  # Unauthorized
        return f(*args, **kwargs)
    return decorated_function