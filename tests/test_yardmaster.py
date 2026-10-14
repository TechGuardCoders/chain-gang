"""Tests for yardmaster: deps, priority, requeue, dead-letter, nodes."""

import pytest

from yardmaster import (
    DuplicateJobError,
    Job,
    UnknownJobError,
    UnknownNodeError,
    Yard,
)


def make_job(id, **kw):
    return Job(id=id, **kw)


# --- submission -------------------------------------------------------


def test_submit_and_lookup():
    y = Yard()
    y.submit(make_job("a"))
    assert y.job("a").id == "a"
    assert y.job("a").state == "queued"


def test_duplicate_submit_rejected():
    y = Yard()
    y.submit(make_job("a"))
    with pytest.raises(DuplicateJobError):
        y.submit(make_job("a"))


def test_unknown_job_raises():
    with pytest.raises(UnknownJobError):
        Yard().job("nope")


# --- dependencies -----------------------------------------------------


def test_dep_blocks_readiness():
    y = Yard()
    y.submit(make_job("a"))
    y.submit(make_job("b", deps=("a",)))
    assert [j.id for j in y.ready()] == ["a"]
    y.schedule()
    y.mark_done("a")
    assert [j.id for j in y.ready()] == ["b"]


def test_chain_of_deps():
    y = Yard()
    y.submit(make_job("a"))
    y.submit(make_job("b", deps=("a",)))
    y.submit(make_job("c", deps=("b",)))
    assert [j.id for j in y.ready()] == ["a"]
    y.schedule()
    y.mark_done("a")
    assert [j.id for j in y.ready()] == ["b"]
    y.schedule()
    y.mark_done("b")
    assert [j.id for j in y.ready()] == ["c"]


def test_dep_on_unknown_job_never_ready():
    y = Yard()
    y.submit(make_job("b", deps=("ghost",)))
    assert y.ready() == []


def test_failed_dep_blocks():
    y = Yard()
    y.submit(make_job("a", max_attempts=1))
    y.submit(make_job("b", deps=("a",)))
    y.schedule()
    y.mark_failed("a")  # dead
    assert [j.id for j in y.ready()] == []


# --- priority order -----------------------------------------------------


def test_priority_order_high_first():
    y = Yard()
    for pid, pri in [("low", 1), ("high", 10), ("mid", 5)]:
        y.submit(make_job(pid, priority=pri))
    assert [j.id for j in y.ready()] == ["high", "mid", "low"]


def test_ties_break_by_submission_order():
    y = Yard()
    for pid in ["first", "second", "third"]:
        y.submit(make_job(pid, priority=7))
    assert [j.id for j in y.ready()] == ["first", "second", "third"]


def test_schedule_returns_priority_order():
    y = Yard()
    y.submit(make_job("a", priority=1))
    y.submit(make_job("b", priority=9))
    y.submit(make_job("c", priority=5))
    got = y.schedule(count=3)
    assert [j.id for j in got] == ["b", "c", "a"]
    assert all(j.state == "running" for j in got)


# --- failure / requeue / dead-letter ------------------------------------


def test_failure_requeues_until_max_attempts():
    y = Yard()
    y.submit(make_job("a", max_attempts=3))
    y.schedule()
    y.mark_failed("a")
    assert y.job("a").state == "queued"
    assert y.job("a").attempts == 1
    y.schedule()
    y.mark_failed("a")
    assert y.job("a").state == "queued"
    y.schedule()
    y.mark_failed("a")
    assert y.job("a").state == "dead"
    assert [j.id for j in y.dead_letter] == ["a"]


def test_dead_job_not_rescheduled():
    y = Yard()
    y.submit(make_job("a", max_attempts=1))
    y.schedule()
    y.mark_failed("a")
    assert y.ready() == []
    assert y.schedule() == []


def test_success_on_retry():
    y = Yard()
    y.submit(make_job("a", max_attempts=3))
    y.schedule()
    y.mark_failed("a")
    y.schedule()
    y.mark_done("a")
    assert y.job("a").state == "done"
    assert y.dead_letter == []


# --- node assignment ------------------------------------------------------


def test_round_robin_assignment():
    y = Yard(strategy="round-robin")
    y.register_node("n1")
    y.register_node("n2")
    y.submit(make_job("a"))
    y.submit(make_job("b"))
    y.submit(make_job("c"))
    got = y.schedule(count=3)
    assert [j.node for j in got] == ["n1", "n2", "n1"]


def test_least_loaded_assignment():
    y = Yard(strategy="least-loaded")
    y.register_node("n1")
    y.register_node("n2")
    y.submit(make_job("a"))
    y.submit(make_job("b"))
    y.schedule(count=1)  # a -> n1 (tie, alphabetical)
    y.schedule(count=1)  # b -> n2 (n1 loaded)
    assert y.node("n1").load == 1
    assert y.node("n2").load == 1


def test_load_released_on_done():
    y = Yard(strategy="least-loaded")
    y.register_node("n1")
    y.register_node("n2")
    y.submit(make_job("a"))
    y.submit(make_job("b"))
    y.schedule(count=1)
    y.mark_done("a")
    assert y.node("n1").load == 0


def test_nodeless_yard_schedules_unassigned():
    y = Yard()
    y.submit(make_job("a"))
    got = y.schedule()
    assert got[0].node is None
    assert got[0].state == "running"


def test_unknown_node_raises():
    with pytest.raises(UnknownNodeError):
        Yard().node("ghost")


# --- payload / introspection ----------------------------------------------


def test_payload_is_opaque():
    fn = lambda: "ran"  # noqa: E731
    y = Yard()
    y.submit(make_job("a", payload=fn))
    y.submit(make_job("b", payload={"cmd": "bake", "gpu": 0}))
    assert y.ready()[0].payload() == "ran"
    assert y.ready()[1].payload["cmd"] == "bake"


def test_counts():
    y = Yard()
    y.register_node("n1")
    y.submit(make_job("a"))
    y.submit(make_job("b"))
    y.schedule(count=1)
    assert y.running_count() == 1
    assert y.pending_count() == 1
    y.mark_done("a")
    assert y.running_count() == 0
    assert y.pending_count() == 1


def test_invalid_strategy_rejected():
    with pytest.raises(ValueError):
        Yard(strategy="chaos")
