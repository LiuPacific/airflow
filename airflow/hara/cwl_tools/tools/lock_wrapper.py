import fcntl
from functools import wraps

def resource_lock_decorator(lock_file_path):
    def ha_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            lock_file = open(lock_file_path, 'w')
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                print(f'lock acquired for {func.__name__}')
                result = func(*args, **kwargs)
                return result
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()
                print(f'lock released for {func.__name__}')
        return wrapper
    return ha_decorator




