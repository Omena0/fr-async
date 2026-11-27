
import os

from .typing import ReturnsAfter, LiteralType
from .scheduler import EEVDFScheduler
from .classes import Yieldable, Task, File
from .loop import ExecutionLoop
from .messages import *

from typing import Any, Generator, Literal
import time

def gen():
    yield Sleep(1)
    print('1')
    yield Sleep(1)
    print('2')
    yield Sleep(1)
    print('3')

def main() -> Any:
    yield Task(gen(), background=True)
    print('a')
    yield Sleep(1)
    print('b')
    yield Sleep(1)
    print('c')
    yield Sleep(1)
    print('d')

loop: ExecutionLoop = ExecutionLoop(EEVDFScheduler)
loop.run(Yieldable(main))
