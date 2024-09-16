import fcntl
from functools import wraps
from airflow.hara.cwl_tools.tools import cwl_log

hara_lock_count_map: dict = {}


def resource_lock_decorator(lock_file_path):
    def ha_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if (hara_lock_count_map.get(lock_file_path) is None
                or hara_lock_count_map.get(lock_file_path) == 0):
                with open(lock_file_path, 'w') as lock_file:
                    try:
                        fcntl.flock(lock_file, fcntl.LOCK_EX)
                        hara_lock_count_map[lock_file_path] = 1 # if it managed to acquire the lock, add one reference to the counter.
                        cwl_log.get_cwl_logger().info('lock acquired for %s, now counter 1', {func.__name__})
                        result = func(*args, **kwargs)
                        return result
                    finally:
                        hara_lock_count_map[lock_file_path] = 0
                        fcntl.flock(lock_file, fcntl.LOCK_UN)
                        cwl_log.get_cwl_logger().info('lock released for %s, now counter 0', {func.__name__})
            else: # inside the stack which starts from the above `func()` call, there might be another method with the decorator. So we will support reentrant lock here.
                reference_counter = hara_lock_count_map[lock_file_path]
                reference_counter = reference_counter + 1
                hara_lock_count_map[lock_file_path] = reference_counter
                cwl_log.get_cwl_logger().info('lock acquired for %s, now counter %d', {func.__name__}, reference_counter)
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    reference_counter = reference_counter - 1
                    hara_lock_count_map[lock_file_path] = reference_counter
                    cwl_log.get_cwl_logger().info('lock released for %s, now counter %d', {func.__name__}, reference_counter)
        return wrapper

    return ha_decorator

