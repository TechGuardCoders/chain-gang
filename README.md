# Chain Gang

A dependency-aware job yard for GPU-adjacent work: a priority queue where
job B can wait on job A, workers pull from nodes, and failures requeue
until they hit max attempts — then dead-letter instead of looping forever.

Pure Python stdlib. Synchronous. No network, no dependencies beyond pytest.

## Why

Serving stacks already have autoscalers and routers. What they don't have
is a small, inspectable scheduler for the *around* work — preprocessing
runs, eval sweeps, checkpoint uploads — where you need ordering guarantees
and failure semantics you can actually reason about in a unit test.

## Architecture

```
Job(id, priority, deps, payload, max_attempts)
        │ submit
        ▼
     Yard ──ready()──► deps done? ──► priority order (stable)
        │
        ├── schedule(count) ──► node assignment (round-robin | least-loaded)
        ├── mark_done(id) ──► releases node load
        └── mark_failed(id) ──► requeue until attempts == max_attempts
                                  └── else dead_letter[]
```

- **Dependencies**: a job is ready only when every `deps` id is `done`.
  Deps on unknown or dead jobs never become ready — no silent skips.
- **Priority**: higher number first; ties break by submission order, so
  ordering is deterministic across runs.
- **Failure**: `mark_failed` increments attempts and requeues. At
  `max_attempts` the job goes to `dead_letter` and never schedules again.
- **Nodes**: register any number; assignment is round-robin or
  least-loaded, with load released on done/fail.

## Usage

```python
from yardmaster import Job, Yard

yard = Yard(strategy="least-loaded")
yard.register_node("spark-1")
yard.register_node("spark-2")

yard.submit(Job(id="tokenize", priority=10))
yard.submit(Job(id="eval-sweep", priority=5, deps=("tokenize",),
                payload={"model": "glm", "n": 200}))

yard.schedule(count=1)      # -> [tokenize], assigned to a node
yard.mark_done("tokenize")
ready = yard.schedule(count=1)  # -> [eval-sweep]
yard.mark_failed(ready[0].id)   # requeued, attempts=1
```

## Results

- 21 tests, all passing (`pytest -q`), covering deps, priority stability,
  requeue-to-dead-letter, node strategies, and load accounting.
- Failure path verified: 3-attempt job requeues twice, dead-letters on
  the third, and is excluded from `ready()` afterward.

## CI

GitHub Actions runs `pytest` on push (Python 3.12). Badge:

[![tests](https://github.com/TechGuardCoders/chain-gang/actions/workflows/ci.yml/badge.svg)](https://github.com/TechGuardCoders/chain-gang/actions/workflows/ci.yml)
