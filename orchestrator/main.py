"""Central orchestration entry point for Zantra Medical workflows."""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

if __package__ is None or __package__ == "":  # pragma: no cover - runtime safety for script execution
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from orchestrator.agents.appointment import AppointmentAgent
from orchestrator.agents.billing import BillingAgent
from orchestrator.agents.recall import RecallAgent

LOG_PATH = Path(__file__).resolve().parent / "task_log.json"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _format_timestamp(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


class TaskLogger:
    """Persists orchestration events into a JSON log."""

    def __init__(self, log_path: Path) -> None:
        self._log_path = log_path
        self._lock = threading.Lock()
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        task_name: str,
        status: str,
        *,
        start_time: Optional[datetime] = None,
        message: Optional[str] = None,
        details: Optional[Dict[str, object]] = None,
    ) -> None:
        completed_at = _utc_now()
        started_at = start_time or completed_at
        entry: Dict[str, object] = {
            "task": task_name,
            "status": status,
            "started_at": _format_timestamp(started_at),
            "completed_at": _format_timestamp(completed_at),
        }
        if message:
            entry["message"] = message
        if details is not None:
            entry["details"] = details

        with self._lock:
            history = self._read_history()
            history.append(entry)
            serialized = json.dumps(history, indent=2)
            self._log_path.write_text(f"{serialized}\n", encoding="utf-8")

    def _read_history(self) -> List[Dict[str, object]]:
        if not self._log_path.exists():
            return []
        raw_content = self._log_path.read_text(encoding="utf-8").strip()
        if not raw_content:
            return []
        try:
            data = json.loads(raw_content)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Task log is corrupted and cannot be parsed: {exc.msg}"
            ) from exc
        if not isinstance(data, list):
            raise ValueError("Task log must contain a JSON list of entries.")
        return data


@dataclass
class ScheduledTask:
    """Represents a task scheduled to run once every day."""

    name: str
    time_of_day: dtime
    action: Callable[[], Optional[Dict[str, object]]]
    next_run: datetime = field(init=False)

    def __post_init__(self) -> None:
        self._schedule_next()

    def _schedule_next(self) -> None:
        now = datetime.now()
        candidate = datetime.combine(now.date(), self.time_of_day)
        if candidate <= now:
            candidate += timedelta(days=1)
        self.next_run = candidate

    def mark_executed(self) -> None:
        self._schedule_next()


class DailyTaskScheduler:
    """Lightweight daily scheduler that polls for tasks to run."""

    def __init__(self, logger: TaskLogger, poll_interval_seconds: int = 60) -> None:
        self._logger = logger
        self._poll_interval_seconds = max(1, poll_interval_seconds)
        self._tasks: List[ScheduledTask] = []
        self._stop_event = threading.Event()

    def add_daily_task(
        self, name: str, run_time: dtime, action: Callable[[], Optional[Dict[str, object]]]
    ) -> None:
        self._tasks.append(ScheduledTask(name=name, time_of_day=run_time, action=action))

    def start(self) -> None:
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._handle_stop_signal)
            signal.signal(signal.SIGTERM, self._handle_stop_signal)

        try:
            while not self._stop_event.is_set():
                now = datetime.now()
                for task in self._tasks:
                    if now >= task.next_run:
                        try:
                            execute_with_logging(task.name, task.action, self._logger)
                        except Exception:
                            # Failure is logged; continue running other tasks.
                            pass
                        finally:
                            task.mark_executed()
                time.sleep(self._poll_interval_seconds)
        finally:
            self._stop_event.set()

    def _handle_stop_signal(self, signum: int, frame) -> None:  # type: ignore[override]
        self._stop_event.set()


def execute_with_logging(
    task_name: str, action: Callable[[], Optional[Dict[str, object]]], logger: TaskLogger
) -> Optional[Dict[str, object]]:
    """Run ``action`` while emitting structured log entries."""

    start_time = _utc_now()
    details: Optional[Dict[str, object]] = None
    status = "success"
    message: Optional[str] = None

    try:
        result = action()
        if isinstance(result, dict):
            details = result
        return result
    except Exception as exc:
        status = "failed"
        message = str(exc)
        raise
    finally:
        logger.log(
            task_name,
            status,
            start_time=start_time,
            message=message,
            details=details,
        )


def run_recalls() -> Dict[str, object]:
    """Run the recall workflow and return a structured summary."""

    appointment_agent = AppointmentAgent()
    recall_agent = RecallAgent(appointment_agent)
    return recall_agent.schedule_recalls()


def _load_pending_claims() -> Iterable[Dict[str, object]]:
    claims_path = Path(__file__).resolve().parent / "data" / "claims.json"
    if not claims_path.exists():
        return []
    raw_content = claims_path.read_text(encoding="utf-8").strip()
    if not raw_content:
        return []
    try:
        payload = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid claims JSON data: {exc.msg}") from exc
    if not isinstance(payload, list):
        raise ValueError("Claims file must contain a list of claim objects.")
    return payload


def run_claims() -> Dict[str, object]:
    """Run the claim submission workflow and return a structured summary."""

    billing_agent = BillingAgent()
    for claim in _load_pending_claims():
        if not isinstance(claim, dict):
            raise ValueError("Each claim entry must be a JSON object.")
        billing_agent.queue_claim({str(k): v for k, v in claim.items()})
    return billing_agent.submit_claims()


def run_scheduler(logger: TaskLogger) -> None:
    scheduler = DailyTaskScheduler(logger=logger)
    scheduler.add_daily_task("daily_recalls", dtime(hour=9, minute=0), run_recalls)
    scheduler.add_daily_task("daily_claims", dtime(hour=17, minute=0), run_claims)
    logger.log("scheduler", "started", message="Daily scheduler started.")
    try:
        scheduler.start()
    finally:
        logger.log("scheduler", "stopped", message="Daily scheduler stopped.")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Zantra Medical orchestration controller")
    parser.add_argument(
        "command",
        nargs="?",
        choices=("run_scheduler", "run_recalls", "run_claims"),
        default="run_scheduler",
        help="Command to execute",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logger = TaskLogger(LOG_PATH)

    if args.command == "run_recalls":
        execute_with_logging("daily_recalls", run_recalls, logger)
    elif args.command == "run_claims":
        execute_with_logging("daily_claims", run_claims, logger)
    else:
        run_scheduler(logger)
    return 0


if __name__ == "__main__":
    sys.exit(main())
