from fr_async import ExecutionLoop, Yieldable
from fr_async.messages import Sleep
import time

def main():
    t = time.monotonic()
    print("start")
    yield Sleep(1.0)
    print(f"1s sleep = 1 + {(time.monotonic()-t-1)*1000:.4f} ms overhead")

loop = ExecutionLoop()
loop.run(Yieldable(main))
