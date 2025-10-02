"""Appointment agent responsible for appointment data retrieval."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class AppointmentRecord:
    """Represents an appointment entry used for recall decisions."""

    patient_id: str
    patient_name: str
    appointment_date: date
    needs_recall: bool = False


class AppointmentAgent:
    """Loads appointment information that downstream agents rely on."""

    def __init__(self, source_path: Optional[Path] = None) -> None:
        base_dir = Path(__file__).resolve().parents[1]
        default_path = base_dir / "data" / "appointments.json"
        self._source_path = source_path or default_path

    def load_appointments(self) -> List[AppointmentRecord]:
        """Load appointment records from disk if available."""

        if not self._source_path.exists():
            return []

        try:
            raw_data = json.loads(self._source_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid appointment data in {self._source_path}: {exc.msg}"
            ) from exc

        if not isinstance(raw_data, Iterable):
            raise ValueError("Appointment data must be a list of records.")

        records: List[AppointmentRecord] = []
        for entry in raw_data:
            record = self._parse_record(entry)
            records.append(record)
        return records

    def get_patients_due_for_recall(
        self, as_of: Optional[date] = None
    ) -> List[AppointmentRecord]:
        """Return appointments that require recall outreach."""

        as_of = as_of or date.today()
        due_records: List[AppointmentRecord] = []
        for record in self.load_appointments():
            if record.needs_recall:
                due_records.append(record)
            elif record.appointment_date <= as_of:
                due_records.append(record)
        return due_records

    def _parse_record(self, entry: dict) -> AppointmentRecord:
        if not isinstance(entry, dict):
            raise ValueError("Each appointment entry must be a dictionary.")

        try:
            patient_id = str(entry["patient_id"])
            patient_name = str(entry.get("patient_name", ""))
            needs_recall = bool(entry.get("needs_recall", False))
            appointment_raw = entry.get("appointment_date")
        except KeyError as exc:
            raise ValueError("Missing required appointment field") from exc

        appointment_date = self._coerce_date(appointment_raw)
        return AppointmentRecord(
            patient_id=patient_id,
            patient_name=patient_name,
            appointment_date=appointment_date,
            needs_recall=needs_recall,
        )

    @staticmethod
    def _coerce_date(value: object) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).date()
            except ValueError as exc:
                raise ValueError("Appointment dates must be ISO formatted") from exc
        raise ValueError("Unsupported appointment date format")
