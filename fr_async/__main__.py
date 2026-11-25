
import os

from .typing import ReturnsAfter, LiteralType
from .scheduler import EEVDFScheduler
from .classes import Yieldable, Task, File
from .loop import ExecutionLoop
from .messages import *

from typing import Any, Generator, Literal
import time

def main() -> Any:
    yield Task(print, 'a')

loop: ExecutionLoop = ExecutionLoop(EEVDFScheduler)
loop.run(Yieldable(main))
