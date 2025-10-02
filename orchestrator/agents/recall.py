"""Recall agent orchestrates patient recall outreach."""
from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional

from .appointment import AppointmentAgent, AppointmentRecord


class RecallAgent:
    """Coordinates the scheduling of patient recalls."""

    def __init__(self, appointment_agent: AppointmentAgent) -> None:
        self._appointment_agent = appointment_agent

    def schedule_recalls(self, as_of: Optional[date] = None) -> Dict[str, object]:
        """Determine which patients require recall notifications."""

        as_of = as_of or date.today()
        due_records: List[AppointmentRecord] = self._appointment_agent.get_patients_due_for_recall(as_of)
        recalls: List[Dict[str, str]] = []
        for record in due_records:
            recalls.append(
                {
                    "patient_id": record.patient_id,
                    "patient_name": record.patient_name,
                    "scheduled_for": as_of.isoformat(),
                }
            )
        return {
            "as_of": as_of.isoformat(),
            "scheduled_count": len(recalls),
            "recalls": recalls,
        }
