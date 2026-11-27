import sys
from typing import Any


def log(*args: Any, **kwargs: Any):
    if not DEBUG: return
    print(*args, **kwargs)


DEBUG = '-d' in sys.argv
