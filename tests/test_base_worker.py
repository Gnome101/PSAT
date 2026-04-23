"""Tests for workers.base — BaseWorker loop, recovery, and signal handling.

These are pure unit tests that mock all DB dependencies.
"""

from __future__ import annotations

import signal
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from db.models import JobStage, JobStatus
from workers.base import BaseWorker, JobHandledDirectly

# ---------------------------------------------------------------------------
# Concrete subclass for testing
# ---------------------------------------------------------------------------


class _TestWorker(BaseWorker):
    """Minimal concrete worker for testing."""

    stage = JobStage.discovery
    next_stage = JobStage.static
    poll_interval = 0  # no real sleeping in tests

    def process(self, session, job):
        """No-op — individual tests override via mock."""
        pass


class _DoneWorker(BaseWorker):
    """Worker whose next_stage is done (triggers complete_job)."""

    stage = JobStage.policy
    next_stage = JobStage.done
    poll_interval = 0

    def process(self, session, job):
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job(**overrides):
    """Return a lightweight mock Job object."""
    defaults = dict(
        id=uuid.uuid4(),
        address="0x" + "a" * 40,
        name="test-job",
        status=JobStatus.processing,
        stage=JobStage.discovery,
        updated_at=datetime.now(timezone.utc),
        worker_id="some-worker",
        detail=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# Tests: __init__
# ---------------------------------------------------------------------------


@patch("workers.base.signal.signal")
def test_init_sets_worker_id_with_classname_and_pid(mock_signal):
    """worker_id contains the class name and the PID."""
    with patch("workers.base.os.getpid", return_value=12345):
        w = _TestWorker()
    assert w.worker_id.startswith("_TestWorker-12345-")
    assert len(w.worker_id.split("-")) == 3
    assert w._running is True


@patch("workers.base.signal.signal")
def test_init_registers_signal_handlers(mock_signal):
    """__init__ registers SIGTERM and SIGINT handlers."""
    _TestWorker()
    calls = mock_signal.call_args_list
    sig_nums = {c[0][0] for c in calls}
    assert signal.SIGTERM in sig_nums
    assert signal.SIGINT in sig_nums


# ---------------------------------------------------------------------------
# Tests: _handle_sigterm
# ---------------------------------------------------------------------------


@patch("workers.base.signal.signal")
def test_handle_sigterm_sets_running_false(mock_signal):
    w = _TestWorker()
    assert w._running is True
    w._handle_sigterm(signal.SIGTERM, None)
    assert w._running is False


# ---------------------------------------------------------------------------
# Tests: process() (base class)
# ---------------------------------------------------------------------------


def test_base_process_raises_not_implemented():
    """BaseWorker.process() must raise NotImplementedError."""
    with patch("workers.base.signal.signal"):
        w = BaseWorker()
    with pytest.raises(NotImplementedError):
        w.process(MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Tests: _recover_stale_jobs
# ---------------------------------------------------------------------------


@patch("workers.base.signal.signal")
def test_recover_stale_jobs_requeues(mock_signal):
    """Stale jobs are set to queued with worker_id cleared."""
    w = _TestWorker()
    stale_job = _make_job(
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=300),
    )
    mock_session = MagicMock()
    mock_session.execute.return_value.scalars.return_value.all.return_value = [stale_job]

    w._recover_stale_jobs(mock_session)

    assert stale_job.status == JobStatus.queued
    assert stale_job.worker_id is None
    assert stale_job.detail == "Re-queued after stale processing timeout"
    mock_session.commit.assert_called_once()


@patch("workers.base.signal.signal")
def test_recover_stale_jobs_no_stale(mock_signal):
    """When no stale jobs exist, session.commit is not called."""
    w = _TestWorker()
    mock_session = MagicMock()
    mock_session.execute.return_value.scalars.return_value.all.return_value = []

    w._recover_stale_jobs(mock_session)

    mock_session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: run_loop
# ---------------------------------------------------------------------------


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
@patch("workers.base.advance_job")
def test_run_loop_claims_processes_and_advances(mock_advance, mock_claim, mock_session_cls, mock_signal):
    """Happy path: claim a job, process it, advance to next_stage."""
    job = _make_job()
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return job
        # Stop the loop on second iteration
        w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    w = _TestWorker()
    w.process = MagicMock()
    w.run_loop()

    w.process.assert_called_once_with(mock_session, job)
    mock_advance.assert_called_once_with(mock_session, job.id, JobStage.static, "Completed discovery")


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
@patch("workers.base.advance_job")
def test_run_loop_job_handled_directly_skips_advance(mock_advance, mock_claim, mock_session_cls, mock_signal):
    """When process() raises JobHandledDirectly, advance_job is NOT called."""
    job = _make_job()
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return job
        w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    w = _TestWorker()
    w.process = MagicMock(side_effect=JobHandledDirectly())
    w.run_loop()

    mock_advance.assert_not_called()


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
@patch("workers.base.fail_job")
def test_run_loop_process_exception_calls_fail_job(mock_fail, mock_claim, mock_session_cls, mock_signal):
    """When process() raises an unexpected exception, fail_job is called."""
    job = _make_job()
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return job
        w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    w = _TestWorker()
    w.process = MagicMock(side_effect=RuntimeError("boom"))
    w.run_loop()

    mock_fail.assert_called_once()
    args = mock_fail.call_args[0]
    assert args[0] is mock_session  # session
    assert args[1] == job.id  # job_id
    assert "boom" in args[2]  # error traceback contains "boom"
    # rollback fires at least once from the exception handler; the empty-
    # sweep branch of ``reclaim_stuck_jobs`` may also call it.
    mock_session.rollback.assert_called()


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job", return_value=None)
@patch("workers.base.time.sleep")
def test_run_loop_no_job_sleeps(mock_sleep, mock_claim, mock_session_cls, mock_signal):
    """When no job is available, the loop sleeps for poll_interval."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    w = _TestWorker()
    w.poll_interval = 2.0

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect
    w.run_loop()

    mock_sleep.assert_called_with(2.0)


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
def test_run_loop_running_false_exits(mock_claim, mock_session_cls, mock_signal):
    """Setting _running=False before starting causes immediate exit."""
    w = _TestWorker()
    w._running = False
    w.run_loop()
    mock_claim.assert_not_called()


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
@patch("db.queue.complete_job")
def test_run_loop_next_stage_done_calls_complete_job(mock_complete, mock_claim, mock_session_cls, mock_signal):
    """When next_stage is done, complete_job is called instead of advance_job."""
    job = _make_job(stage=JobStage.policy)
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return job
        w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    w = _DoneWorker()
    w.process = MagicMock()
    w.run_loop()

    mock_complete.assert_called_once_with(mock_session, job.id)


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job", return_value=None)
@patch("workers.base.time.sleep")
def test_run_loop_stale_recovery_every_30_cycles(mock_sleep, mock_claim, mock_session_cls, mock_signal):
    """Stale job recovery runs on the 30th poll cycle."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    w = _TestWorker()

    cycle = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal cycle
        cycle += 1
        if cycle >= 31:
            w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    with patch.object(w, "_recover_stale_jobs") as mock_recover:
        w.run_loop()
        mock_recover.assert_called_once_with(mock_session)


# ---------------------------------------------------------------------------
# Tests: _claim_job sweeps stuck processing rows before claiming
# ---------------------------------------------------------------------------


@patch("workers.base.signal.signal")
@patch("workers.base.reclaim_stuck_jobs")
@patch("workers.base.claim_job")
def test_claim_job_sweeps_stuck_rows_before_claiming(mock_claim, mock_reclaim, mock_signal):
    """``_claim_job`` invokes ``reclaim_stuck_jobs`` first so crashed workers'
    jobs can be picked up on the very next poll instead of waiting 30 cycles
    for the stage-filtered recovery sweep."""
    w = _TestWorker()
    mock_session = MagicMock()
    mock_reclaim.return_value = []
    mock_claim.return_value = None

    w._claim_job(mock_session)

    mock_reclaim.assert_called_once_with(mock_session)
    mock_claim.assert_called_once_with(mock_session, JobStage.discovery, w.worker_id)


# ---------------------------------------------------------------------------
# Tests: update_detail
# ---------------------------------------------------------------------------


@patch("workers.base.signal.signal")
@patch("workers.base.update_job_detail")
def test_update_detail_delegates_to_queue(mock_update, mock_signal):
    """update_detail() delegates to db.queue.update_job_detail."""
    w = _TestWorker()
    mock_session = MagicMock()
    mock_job = _make_job()
    w.update_detail(mock_session, cast(Any, mock_job), "50% done")
    mock_update.assert_called_once_with(mock_session, mock_job.id, "50% done")


# ---------------------------------------------------------------------------
# Tests: error handling edge cases in run_loop
# ---------------------------------------------------------------------------


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
@patch("workers.base.fail_job")
def test_run_loop_fail_job_exception_retries_with_fresh_session(mock_fail, mock_claim, mock_session_cls, mock_signal):
    """When fail_job raises, the loop retries with a fresh session."""
    job = _make_job()
    mock_session = MagicMock()
    fresh_session = MagicMock()

    session_call_count = 0

    def _session_factory():
        nonlocal session_call_count
        session_call_count += 1
        if session_call_count == 1:
            return mock_session
        return fresh_session

    mock_session_cls.side_effect = _session_factory

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return job
        w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    # First fail_job raises, second (fresh session) succeeds
    mock_fail.side_effect = [Exception("db gone"), None]

    w = _TestWorker()
    w.process = MagicMock(side_effect=RuntimeError("boom"))
    w.run_loop()

    assert mock_fail.call_count == 2
    # Second call should use fresh_session
    second_call_args = mock_fail.call_args_list[1][0]
    assert second_call_args[0] is fresh_session
    # fresh_session.close is called in the retry block; it may also be called
    # when SessionLocal returns it as the main loop session in later iterations.
    assert fresh_session.close.call_count >= 1


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
@patch("workers.base.fail_job")
def test_run_loop_both_fail_job_attempts_fail_gracefully(mock_fail, mock_claim, mock_session_cls, mock_signal):
    """When both fail_job attempts raise, the loop continues without crashing."""
    job = _make_job()
    mock_session = MagicMock()
    fresh_session = MagicMock()

    session_call_count = 0

    def _session_factory():
        nonlocal session_call_count
        session_call_count += 1
        if session_call_count == 1:
            return mock_session
        return fresh_session

    mock_session_cls.side_effect = _session_factory

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return job
        w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    # Both fail_job calls raise
    mock_fail.side_effect = [Exception("db gone"), Exception("still gone")]

    w = _TestWorker()
    w.process = MagicMock(side_effect=RuntimeError("boom"))
    # Should not raise — loop handles both failures gracefully
    w.run_loop()

    assert mock_fail.call_count == 2


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job")
def test_run_loop_outer_exception_does_not_crash(mock_claim, mock_session_cls, mock_signal):
    """An exception in the outer try block (e.g. claim_job) is caught and logged."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    call_count = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("unexpected db error")
        w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect

    w = _TestWorker()
    # Should not raise — the outer except catches it
    w.run_loop()


@patch("workers.base.signal.signal")
@patch("workers.base.SessionLocal")
@patch("workers.base.claim_job", return_value=None)
@patch("workers.base.time.sleep")
def test_run_loop_session_closed_when_no_job(mock_sleep, mock_claim, mock_session_cls, mock_signal):
    """When no job is claimed, the session is still closed."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session

    w = _TestWorker()

    cycle = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal cycle
        cycle += 1
        if cycle >= 2:
            w._running = False
        return None

    mock_claim.side_effect = _claim_side_effect
    w.run_loop()

    # session.close called in the continue branch and in finally
    assert mock_session.close.call_count >= 2


# ---------------------------------------------------------------------------
# Tests: process() watchdog (SIGALRM-based)
# ---------------------------------------------------------------------------


def test_on_process_timeout_raises_process_budget_exceeded():
    """The SIGALRM handler must raise ProcessBudgetExceeded so the main
    loop's except clause can log + fail the job uniformly."""
    from workers.base import ProcessBudgetExceeded

    with patch("workers.base.signal.signal"):
        w = _TestWorker()
    with pytest.raises(ProcessBudgetExceeded) as excinfo:
        w._on_process_timeout(signal.SIGALRM, None)
    # Error message must include the stage so Grafana alerts can fan out.
    assert w.stage.value in str(excinfo.value)


@patch("workers.base.fail_job")
@patch("workers.base.advance_job")
@patch("workers.base.claim_job")
@patch("workers.base.SessionLocal")
@patch("workers.base.signal.alarm")
@patch("workers.base.signal.signal")
def test_run_loop_arms_and_disarms_alarm_on_success(
    mock_signal, mock_alarm, mock_session_cls, mock_claim, mock_advance, mock_fail
):
    """Watchdog is armed before process() and cleared after success."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    job = _make_job()
    w = _TestWorker()

    cycle = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal cycle
        cycle += 1
        if cycle >= 2:
            w._running = False
            return None
        return job

    mock_claim.side_effect = _claim_side_effect
    w.run_loop()

    # The budget arm, then a zero-disarm in finally.
    alarm_values = [c.args[0] for c in mock_alarm.call_args_list]
    assert w.process_budget_seconds in alarm_values
    assert 0 in alarm_values
    # Last alarm() call in the finally should be a disarm.
    assert alarm_values[-1] == 0


@patch("workers.base.fail_job")
@patch("workers.base.claim_job")
@patch("workers.base.SessionLocal")
@patch("workers.base.signal.alarm")
@patch("workers.base.signal.signal")
def test_run_loop_budget_trip_fails_job_and_continues(mock_signal, mock_alarm, mock_session_cls, mock_claim, mock_fail):
    """A ProcessBudgetExceeded in process() must fail the job and let
    the loop continue — not crash the whole worker."""
    from workers.base import ProcessBudgetExceeded

    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    job = _make_job()

    class _TimeoutWorker(_TestWorker):
        def process(self, session, job):
            raise ProcessBudgetExceeded("budget exceeded for stage=discovery")

    w = _TimeoutWorker()

    cycle = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal cycle
        cycle += 1
        if cycle >= 2:
            w._running = False
            return None
        return job

    mock_claim.side_effect = _claim_side_effect
    w.run_loop()

    # Job was failed exactly once with a budget-exceeded reason.
    assert mock_fail.call_count == 1
    call = mock_fail.call_args
    assert call.args[1] == job.id
    assert "budget" in call.args[2].lower()
    # Watchdog was disarmed despite the exception.
    alarm_values = [c.args[0] for c in mock_alarm.call_args_list]
    assert 0 in alarm_values


@patch("workers.base.fail_job")
@patch("workers.base.claim_job")
@patch("workers.base.SessionLocal")
@patch("workers.base.signal.alarm")
@patch("workers.base.signal.signal")
def test_run_loop_budget_zero_disables_watchdog(mock_signal, mock_alarm, mock_session_cls, mock_claim, mock_fail):
    """Setting process_budget_seconds = 0 must skip arming the alarm."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    job = _make_job()

    class _NoBudgetWorker(_TestWorker):
        process_budget_seconds = 0

    w = _NoBudgetWorker()

    cycle = 0

    def _claim_side_effect(session, stage, worker_id):
        nonlocal cycle
        cycle += 1
        if cycle >= 2:
            w._running = False
            return None
        return job

    mock_claim.side_effect = _claim_side_effect
    w.run_loop()

    # No non-zero alarm() call should have been made.
    non_zero_alarms = [c.args[0] for c in mock_alarm.call_args_list if c.args[0] != 0]
    assert non_zero_alarms == []
