from typing import Any
import sys

def log(*args: Any, **kwargs: Any):
    if not DEBUG: return
    print(*args, **kwargs)


DEBUG = '-d' in sys.argv
