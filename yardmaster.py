"""Yardmaster — a dependency-aware job yard.

Jobs carry a priority, dependencies, and a payload. The yard hands out
ready jobs (deps satisfied) in priority order, assigns them to worker
nodes round-robin or least-loaded, and requeues failures up to
max_attempts before dead-lettering them.

Pure stdlib. Synchronous. No network.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class UnknownJobError(KeyError):
    """Raised when operating on a job id the yard has never seen."""


class DuplicateJobError(ValueError):
    """Raised when submitting a job id that already exists."""


class UnknownNodeError(KeyError):
    """Raised when assigning to a node that was never registered."""


@dataclass
class Job:
    """A unit of yard work.

    priority: higher number = scheduled earlier among ready jobs.
    deps: ids of jobs that must be done before this one is ready.
    payload: anything — a callable, a dict, a string. The yard never
        executes it; it only routes it.
    max_attempts: total tries allowed before dead-letter.
    """

    id: str
    priority: int = 0
    deps: tuple[str, ...] = ()
    payload: Any = None
    max_attempts: int = 3
    attempts: int = 0
    state: str = "queued"  # queued | running | done | dead
    node: Optional[str] = None
    seq: int = 0  # submission order, tie-breaker for stable priority order

    @property
    def failed_out(self) -> bool:
        return self.state == "dead"


@dataclass
class Node:
    """A worker node. load = jobs currently assigned."""

    name: str
    load: int = 0


class Yard:
    """Priority queue with dependencies, node assignment, and requeue."""

    def __init__(self, strategy: str = "round-robin") -> None:
        if strategy not in ("round-robin", "least-loaded"):
            raise ValueError(f"unknown strategy: {strategy!r}")
        self.strategy = strategy
        self._jobs: dict[str, Job] = {}
        self._nodes: dict[str, Node] = {}
        self._rr = itertools.count()
        self._seq = itertools.count()
        self.dead_letter: list[Job] = []

    # -- registration ------------------------------------------------

    def submit(self, job: Job) -> Job:
        if job.id in self._jobs:
            raise DuplicateJobError(f"job {job.id!r} already submitted")
        job.seq = next(self._seq)
        job.state = "queued"
        self._jobs[job.id] = job
        return job

    def job(self, job_id: str) -> Job:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise UnknownJobError(job_id) from None

    def register_node(self, name: str) -> Node:
        node = Node(name)
        self._nodes[name] = node
        return node

    def node(self, name: str) -> Node:
        try:
            return self._nodes[name]
        except KeyError:
            raise UnknownNodeError(name) from None

    # -- scheduling ----------------------------------------------------

    def ready(self) -> list[Job]:
        """Queued jobs whose deps are all done, highest priority first.

        Ties break by submission order (stable). Jobs that exhausted
        their attempts are dead and never come back.
        """
        out = []
        for job in self._jobs.values():
            if job.state != "queued":
                continue
            if any(
                dep not in self._jobs or self._jobs[dep].state != "done"
                for dep in job.deps
            ):
                continue
            out.append(job)
        out.sort(key=lambda j: (-j.priority, j.seq))
        return out

    def schedule(self, count: int = 1) -> list[Job]:
        """Pop up to `count` ready jobs, assign each to a node, mark running."""
        scheduled = []
        for job in self.ready()[:count]:
            job.node = self._pick_node()
            job.state = "running"
            if job.node is not None:
                self.node(job.node).load += 1
            scheduled.append(job)
        return scheduled

    def _pick_node(self) -> Optional[str]:
        if not self._nodes:
            return None  # nodeless yard: jobs still schedule, unassigned
        if self.strategy == "least-loaded":
            name = min(self._nodes.values(), key=lambda n: (n.load, n.name)).name
        else:
            names = sorted(self._nodes)
            name = names[next(self._rr) % len(names)]
        return name

    # -- completion ----------------------------------------------------

    def mark_done(self, job_id: str) -> None:
        job = self.job(job_id)
        job.state = "done"
        if job.node is not None:
            self.node(job.node).load = max(0, self.node(job.node).load - 1)
            job.node = None

    def mark_failed(self, job_id: str) -> Job:
        """Requeue the job; dead-letter it once attempts hit max_attempts."""
        job = self.job(job_id)
        job.attempts += 1
        if job.node is not None:
            self.node(job.node).load = max(0, self.node(job.node).load - 1)
            job.node = None
        if job.attempts >= job.max_attempts:
            job.state = "dead"
            self.dead_letter.append(job)
        else:
            job.state = "queued"
        return job

    # -- introspection ---------------------------------------------------

    def pending_count(self) -> int:
        return sum(1 for j in self._jobs.values() if j.state == "queued")

    def running_count(self) -> int:
        return sum(1 for j in self._jobs.values() if j.state == "running")
