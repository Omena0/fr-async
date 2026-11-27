

from typing import Any

from .classes import Task, Yieldable
from .loop import ExecutionLoop
from .messages import *
from .scheduler import EEVDFScheduler


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
