from typing import Any
import os


def log(*args: Any, **kwargs: Any):
    if not DEBUG: return
    print(*args, **kwargs)


DEBUG = 'FR_ASYNC_DEBUG' in os.environ
