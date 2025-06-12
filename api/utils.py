import os

def is_large_file(file_path, threshold_mb=100):
    return os.path.getsize(file_path) > threshold_mb * 1024 * 1024