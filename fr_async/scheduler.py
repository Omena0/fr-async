from dataclasses import dataclass
from math import exp, isfinite
from itertools import count
from typing import Any
import time

lazy from fr_async.classes import Task

class BaseScheduler:
    __slots__ = ()

    def __bool__(self) -> bool: ...
    def __contains__(self, item: Any) -> bool: ...
    def __len__(self) -> int: ...
    def add(self, task: Task, *, immediate = False): ...
    def pop(self) -> Task: ...
    def remove(self, task: Task) -> None: ...
    def account(self, task: Task, runtime: float) -> None: ...


NICE_0_LOAD = 1024.0
@dataclass(slots=True)
class _Entity:
    weight: float
    time_slice: float

    vruntime: float
    deadline: float
    lag: float = 0.0

    sequence: int = 0
    immediate: bool = False
    inactive_since: float | None = None

class EEVDFScheduler(BaseScheduler):
    __slots__ = (
        '_entities',
        '_immediate',
        '_queued',
        '_queued_weight',
        '_queued_weighted_runtime',
        '_sequence',
        '_vtime',
        'default_slice',
        'sleep_decay',
    )

    def __init__(
        self,
        *,
        default_slice: float = 0.004,
        sleep_decay: float | None = 1.0,
    ):
        if not isfinite(default_slice) or default_slice <= 0:
            raise ValueError("default_slice must be finite and positive")

        if sleep_decay is not None:
            if not isfinite(sleep_decay) or sleep_decay <= 0:
                raise ValueError("sleep_decay must be finite and positive")

        self.default_slice = default_slice
        self.sleep_decay = sleep_decay

        # All known tasks and currently queued tasks.
        self._entities: dict[Task, _Entity] = {}
        self._queued: dict[Task, _Entity] = {}

        self._sequence = count()
        self._vtime = 0.0
        self._queued_weight = 0.0
        self._queued_weighted_runtime = 0.0
        self._immediate: list[tuple[int, Task]] = []

    def __contains__(self, task: Task) -> bool:
        return task in self._queued

    def __bool__(self) -> bool:
        return bool(self._queued)

    def __len__(self) -> int:
        return len(self._queued)

    @staticmethod
    def get_data(task: Task, name: str, default: Any):
        try:
            value = getattr(task.data, name)
        except (AttributeError, KeyError):
            value = default

        if value is None:
            value = default

        setattr(task.data, name, value)
        return value

    def _average(
        self,
        extra: _Entity | None = None,
    ) -> tuple[float, float]:
        weighted_runtime = self._queued_weighted_runtime
        total_weight = self._queued_weight

        if extra is not None:
            weighted_runtime += extra.vruntime * extra.weight
            total_weight += extra.weight

        if not total_weight:
            return self._vtime, 0.0

        return weighted_runtime / total_weight, total_weight

    def add(self, task: Task, *, immediate=False):
        entry = self._entities.get(task)

        if entry is None:
            data = task.data

            weight = float(self.get_data(task, "weight", NICE_0_LOAD))
            time_slice = float(
                self.get_data(task, "time_slice", self.default_slice)
            )

            data.weight = weight
            data.time_slice = time_slice

            if not isfinite(weight) or weight <= 0:
                raise ValueError("Task weight must be finite and positive")

            if not isfinite(time_slice) or time_slice <= 0:
                raise ValueError("Task time_slice must be finite and positive")

            vruntime, _ = self._average()

            entry = _Entity(
                weight=weight,
                time_slice=time_slice,
                vruntime=vruntime,
                deadline=vruntime + time_slice * NICE_0_LOAD / weight,
            )

            self._entities[task] = entry

        elif task in self._queued:
            # Already queued. An immediate enqueue moves it to the front.
            if immediate:
                entry.immediate = True
                entry.sequence = next(self._sequence)
                self._immediate.append((entry.sequence, task))
            return

        # Reinsert an entity while preserving its lag.
        elif entry.inactive_since is not None:
            now = time.monotonic()
            inactive_for = max(0.0, now - entry.inactive_since)

            lag = entry.lag

            if self.sleep_decay is not None:
                lag *= exp(-inactive_for / self.sleep_decay)

            vruntime, total_weight = self._average()
            remaining = max(0.0, entry.deadline - entry.vruntime)

            if total_weight:
                entry.vruntime = (
                    vruntime
                    - lag * (total_weight + entry.weight) / total_weight
                )
            else:
                entry.vruntime = vruntime
                lag = 0.0

            entry.lag = lag

            if remaining:
                entry.deadline = entry.vruntime + remaining
            else:
                entry.deadline = (
                    entry.vruntime
                    + entry.time_slice * NICE_0_LOAD / entry.weight
                )

            entry.inactive_since = None

        entry.sequence = next(self._sequence)
        entry.immediate = immediate

        self._queued[task] = entry
        self._queued_weight += entry.weight
        self._queued_weighted_runtime += entry.vruntime * entry.weight
        if immediate:
            self._immediate.append((entry.sequence, task))

    def pop(self) -> Task:
        if not self._queued:
            raise IndexError("pop from empty scheduler")

        urgent_task: Task | None = None
        while self._immediate:
            sequence, queued_task = self._immediate[-1]
            entry = self._queued.get(queued_task)
            if (
                entry is None
                or not entry.immediate
                or entry.sequence != sequence
            ):
                self._immediate.pop()
                continue

            self._immediate.pop()
            urgent_task = queued_task
            break

        if urgent_task is not None:
            task = urgent_task
        else:
            vruntime, _ = self._average()
            eligible_task: Task | None = None
            eligible_deadline = 0.0
            eligible_sequence = 0
            fallback_task: Task | None = None
            fallback_deadline = 0.0
            fallback_sequence = 0

            for queued_task, entry in list(self._queued.items()):
                if fallback_task is None or (
                    entry.deadline < fallback_deadline
                    or (
                        entry.deadline == fallback_deadline
                        and entry.sequence < fallback_sequence
                    )
                ):
                    fallback_task = queued_task
                    fallback_deadline = entry.deadline
                    fallback_sequence = entry.sequence

                if entry.vruntime <= vruntime and (
                    eligible_task is None
                    or entry.deadline < eligible_deadline
                    or (
                        entry.deadline == eligible_deadline
                        and entry.sequence < eligible_sequence
                    )
                ):
                    eligible_task = queued_task
                    eligible_deadline = entry.deadline
                    eligible_sequence = entry.sequence

            task = eligible_task or fallback_task
        assert task is not None

        entry = self._queued.pop(task)
        self._queued_weight -= entry.weight
        self._queued_weighted_runtime -= entry.vruntime * entry.weight
        entry.immediate = False

        return task

    def remove(self, task: Task):
        entry = self._queued.get(task)

        if entry is None:
            raise ValueError("Task is not queued")

        vruntime, _ = self._average()
        entry.lag = vruntime - entry.vruntime
        entry.inactive_since = time.monotonic()

        self._vtime = vruntime
        del self._queued[task]
        self._queued_weight -= entry.weight
        self._queued_weighted_runtime -= entry.vruntime * entry.weight

    def account(self, task: Task, runtime: float):
        """Account for the time a task spent executing."""

        entry = self._entities.get(task)

        if entry is None:
            return

        runtime = max(0.0, runtime)
        entry.vruntime += runtime * NICE_0_LOAD / entry.weight

        vruntime, _ = self._average(extra=entry)

        self._vtime = vruntime
        entry.lag = vruntime - entry.vruntime

        if task.done:
            del self._entities[task]

            if self._queued:
                self._vtime, _ = self._average()
