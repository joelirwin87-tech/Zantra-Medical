"""Compliance reporting agent for Zantra Medical.

This module aggregates data from appointments, recalls, and claims datasets
and produces a weekly compliance report as a PDF document. The report includes
key metrics that help stakeholders monitor operational compliance:

* Recall completion percentage.
* Claim rejection rate.
* Average patient wait time.

The module is designed to be resilient when datasets are missing or partially
complete. It supports JSON and CSV inputs and provides informative logging to
help diagnose integration issues.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.pdfgen import canvas
except ImportError as exc:  # pragma: no cover - defensive guard for missing dependency
    raise ImportError(
        "ReportLab is required to generate compliance reports. Install it with 'pip install reportlab'."
    ) from exc


LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


@dataclass
class MetricSummary:
    """Container for computed compliance metrics."""

    recall_completion_rate: float
    claim_rejection_rate: float
    average_wait_time_minutes: float
    total_recalls: int
    completed_recalls: int
    total_claims: int
    rejected_claims: int
    total_appointments: int
    appointments_with_wait_time: int


def _project_root() -> Path:
    """Return the project root directory."""

    return Path(__file__).resolve().parents[1]


def _data_dir() -> Path:
    """Return the configured data directory.

    The directory can be overridden via the ``DATA_DIR`` environment variable.
    """

    override = os.getenv("DATA_DIR")
    return Path(override) if override else _project_root() / "data"


def _reports_dir() -> Path:
    """Return the reports output directory, creating it if necessary."""

    override = os.getenv("COMPLIANCE_REPORT_DIR")
    reports_path = Path(override) if override else _project_root() / "reports"
    reports_path.mkdir(parents=True, exist_ok=True)
    return reports_path


def _find_dataset_path(dataset_name: str) -> Optional[Path]:
    """Locate the dataset file for the given name.

    Supports CSV and JSON formats. Returns ``None`` if no file is found.
    """

    base_path = _data_dir()
    candidates = (
        base_path / f"{dataset_name}.json",
        base_path / f"{dataset_name}.csv",
    )
    for path in candidates:
        if path.exists():
            LOGGER.info("Found %s dataset at %s", dataset_name, path)
            return path
    LOGGER.warning("No dataset found for %s in %s", dataset_name, base_path)
    return None


def _load_json(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        # Some feeds wrap results in a top-level key such as "data" or "results".
        for key in ("data", "results", "items"):
            if key in data and isinstance(data[key], list):
                return [record for record in data[key] if isinstance(record, dict)]
        LOGGER.error("JSON dataset %s must be a list of objects", path)
        return []
    if not isinstance(data, list):
        LOGGER.error("JSON dataset %s is not a list", path)
        return []
    return [record for record in data if isinstance(record, dict)]


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [row for row in reader]


def load_dataset(
    dataset_name: str, required_fields: Iterable[str]
) -> List[Dict[str, Any]]:
    """Load a dataset, filtering out records that do not contain required fields."""

    dataset_path = _find_dataset_path(dataset_name)
    if not dataset_path:
        return []

    try:
        if dataset_path.suffix.lower() == ".json":
            records = _load_json(dataset_path)
        elif dataset_path.suffix.lower() == ".csv":
            records = _load_csv(dataset_path)
        else:
            LOGGER.error("Unsupported file extension for dataset %s", dataset_path)
            return []
    except (OSError, json.JSONDecodeError, csv.Error) as exc:
        LOGGER.exception("Failed to load dataset %s: %s", dataset_name, exc)
        return []

    filtered_records: List[Dict[str, Any]] = []
    for record in records:
        if all(_has_value(record, field) for field in required_fields):
            filtered_records.append(record)
        else:
            LOGGER.debug(
                "Skipping %s record missing required fields: %s", dataset_name, record
            )
    LOGGER.info("Loaded %d %s records", len(filtered_records), dataset_name)
    return filtered_records


def _has_value(record: Dict[str, Any], field: str) -> bool:
    value = record.get(field)
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def calculate_recall_completion_rate(
    recalls: Iterable[Dict[str, Any]],
) -> tuple[float, int, int]:
    total = 0
    completed = 0
    for recall in recalls:
        total += 1
        if _is_recall_completed(recall):
            completed += 1
    if total == 0:
        return 0.0, total, completed
    return (completed / total) * 100.0, total, completed


def _is_recall_completed(recall: Dict[str, Any]) -> bool:
    status = str(recall.get("status", "")).strip().lower()
    if status in {"completed", "complete", "done", "closed", "fulfilled"}:
        return True
    if status in {"scheduled", "pending", "in-progress", "open"}:
        return False
    if isinstance(recall.get("completed"), bool):
        return bool(recall["completed"])
    completed_at = recall.get("completed_at") or recall.get("completion_date")
    if completed_at:
        return True
    return False


def calculate_claim_rejection_rate(
    claims: Iterable[Dict[str, Any]],
) -> tuple[float, int, int]:
    total = 0
    rejected = 0
    for claim in claims:
        total += 1
        if _is_claim_rejected(claim):
            rejected += 1
    if total == 0:
        return 0.0, total, rejected
    return (rejected / total) * 100.0, total, rejected


def _is_claim_rejected(claim: Dict[str, Any]) -> bool:
    status = str(claim.get("status", "")).strip().lower()
    if status in {"rejected", "denied", "declined"}:
        return True
    if status in {"accepted", "approved", "paid", "submitted", "processing"}:
        return False
    rejection_flag = claim.get("rejected")
    if isinstance(rejection_flag, bool):
        return rejection_flag
    rejection_reason = claim.get("rejection_reason")
    return bool(rejection_reason)


def calculate_average_wait_time(
    appointments: Iterable[Dict[str, Any]],
) -> tuple[float, int, int]:
    total_wait_time = 0.0
    counted = 0
    total = 0
    for appointment in appointments:
        total += 1
        wait_time = _extract_wait_time_minutes(appointment)
        if wait_time is None:
            continue
        counted += 1
        total_wait_time += wait_time
    if counted == 0:
        return 0.0, total, counted
    return total_wait_time / counted, total, counted


def _extract_wait_time_minutes(appointment: Dict[str, Any]) -> Optional[float]:
    if "wait_time_minutes" in appointment and appointment["wait_time_minutes"] not in (
        None,
        "",
    ):
        try:
            return float(appointment["wait_time_minutes"])
        except (TypeError, ValueError):
            LOGGER.debug(
                "Invalid wait_time_minutes value: %s", appointment["wait_time_minutes"]
            )

    check_in = appointment.get("check_in_time") or appointment.get("check_in")
    start = (
        appointment.get("appointment_start_time")
        or appointment.get("start_time")
        or appointment.get("started_at")
    )
    if check_in and start:
        check_in_dt = _parse_datetime(check_in)
        start_dt = _parse_datetime(start)
        if check_in_dt and start_dt and start_dt >= check_in_dt:
            delta = start_dt - check_in_dt
            return delta.total_seconds() / 60.0
        LOGGER.debug("Invalid datetime ordering for appointment: %s", appointment)
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    cleaned = cleaned.replace("Z", "+00:00")
    formats = (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y %H:%M",
    )
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        LOGGER.debug("Failed to parse datetime value: %s", value)
        return None


def generate_metric_summary() -> MetricSummary:
    recalls = load_dataset("recalls", required_fields=["id", "status"])
    claims = load_dataset("claims", required_fields=["id", "status"])
    appointments = load_dataset(
        "appointments",
        required_fields=["id", "patient_id"],
    )

    recall_completion_rate, total_recalls, completed_recalls = (
        calculate_recall_completion_rate(recalls)
    )
    claim_rejection_rate, total_claims, rejected_claims = (
        calculate_claim_rejection_rate(claims)
    )
    average_wait_time, total_appointments, counted_wait_times = (
        calculate_average_wait_time(appointments)
    )

    LOGGER.info(
        "Computed metrics - Recall completion: %.2f%%, Claim rejection: %.2f%%, Average wait: %.2f minutes",
        recall_completion_rate,
        claim_rejection_rate,
        average_wait_time,
    )

    return MetricSummary(
        recall_completion_rate=recall_completion_rate,
        claim_rejection_rate=claim_rejection_rate,
        average_wait_time_minutes=average_wait_time,
        total_recalls=total_recalls,
        completed_recalls=completed_recalls,
        total_claims=total_claims,
        rejected_claims=rejected_claims,
        total_appointments=total_appointments,
        appointments_with_wait_time=counted_wait_times,
    )


def _current_week_range(
    reference: Optional[datetime] = None,
) -> tuple[datetime, datetime]:
    reference = reference or datetime.now()
    start_of_week = reference - timedelta(days=reference.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
    return start_of_week, end_of_week


def _report_filename(start_of_week: datetime) -> Path:
    reports_dir = _reports_dir()
    iso_week = start_of_week.isocalendar()
    filename = f"compliance_report_{iso_week.year}-W{iso_week.week:02d}.pdf"
    return reports_dir / filename


def _draw_header(pdf: canvas.Canvas, title: str, generated_at: datetime) -> None:
    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(1 * inch, 10.5 * inch, title)
    pdf.setFont("Helvetica", 10)
    pdf.drawString(
        1 * inch,
        10.1 * inch,
        f"Generated on: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
    )


def _draw_metrics(
    pdf: canvas.Canvas,
    metrics: MetricSummary,
    start_of_week: datetime,
    end_of_week: datetime,
) -> None:
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(1 * inch, 9.5 * inch, "Reporting Period")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(
        1 * inch,
        9.2 * inch,
        f"{start_of_week.strftime('%Y-%m-%d')} to {end_of_week.strftime('%Y-%m-%d')}",
    )

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(1 * inch, 8.6 * inch, "Key Metrics")

    pdf.setFont("Helvetica", 11)
    pdf.drawString(
        1.2 * inch,
        8.2 * inch,
        f"Recall completion rate: {metrics.recall_completion_rate:.2f}%",
    )
    pdf.drawString(
        1.2 * inch,
        7.9 * inch,
        f"Claim rejection rate: {metrics.claim_rejection_rate:.2f}%",
    )
    pdf.drawString(
        1.2 * inch,
        7.6 * inch,
        f"Average patient wait time: {metrics.average_wait_time_minutes:.2f} minutes",
    )

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(1 * inch, 7.0 * inch, "Data Summary")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(
        1.2 * inch,
        6.6 * inch,
        f"Recalls completed: {metrics.completed_recalls}/{metrics.total_recalls}",
    )
    pdf.drawString(
        1.2 * inch,
        6.3 * inch,
        f"Claims rejected: {metrics.rejected_claims}/{metrics.total_claims}",
    )
    pdf.drawString(
        1.2 * inch,
        6.0 * inch,
        f"Appointments with wait time data: {metrics.appointments_with_wait_time}/{metrics.total_appointments}",
    )

    pdf.setFont("Helvetica", 10)
    pdf.drawString(
        1 * inch,
        5.4 * inch,
        "Notes: Metrics are calculated using available data. Missing or invalid records are excluded.",
    )


def create_compliance_report(
    metrics: MetricSummary, start_of_week: datetime, end_of_week: datetime
) -> Path:
    generated_at = datetime.now()
    report_path = _report_filename(start_of_week)
    pdf = canvas.Canvas(str(report_path), pagesize=letter)
    _draw_header(pdf, "Weekly Compliance Report", generated_at)
    _draw_metrics(pdf, metrics, start_of_week, end_of_week)
    pdf.showPage()
    pdf.save()
    LOGGER.info("Compliance report created at %s", report_path)
    return report_path


def generate_weekly_compliance_report(reference: Optional[datetime] = None) -> Path:
    metrics = generate_metric_summary()
    start_of_week, end_of_week = _current_week_range(reference)
    return create_compliance_report(metrics, start_of_week, end_of_week)


def main() -> None:
    """Entry point for command-line usage."""

    report_path = generate_weekly_compliance_report()
    LOGGER.info("Weekly compliance report is ready: %s", report_path)


if __name__ == "__main__":
    main()
