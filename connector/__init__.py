"""Connector interfaces for the Zantra Medical platform."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List


@dataclass
class AppointmentRecord:
    """Data model for stored appointments."""

    appointment_id: str
    patient_id: str
    provider_id: str
    scheduled_time: datetime


class HaloFHIRClient:
    """Minimal FHIR client abstraction for patient management."""

    def __init__(self) -> None:
        self._patients: set[str] = set()

    def register_patient(self, patient_id: str) -> None:
        if not patient_id:
            raise ValueError("patient_id must be provided")
        self._patients.add(patient_id)

    def patient_exists(self, patient_id: str) -> bool:
        return patient_id in self._patients


class HaloSQLClient:
    """In-memory SQL client simulator for appointment persistence."""

    def __init__(self) -> None:
        self._appointments: Dict[str, AppointmentRecord] = {}
        self._sequence: int = 1

    def is_slot_available(self, provider_id: str, scheduled_time: datetime) -> bool:
        for record in self._appointments.values():
            if (
                record.provider_id == provider_id
                and record.scheduled_time == scheduled_time
            ):
                return False
        return True

    def create_appointment(
        self, patient_id: str, provider_id: str, scheduled_time: datetime
    ) -> AppointmentRecord:
        appointment_id = str(self._sequence)
        self._sequence += 1
        record = AppointmentRecord(
            appointment_id=appointment_id,
            patient_id=patient_id,
            provider_id=provider_id,
            scheduled_time=scheduled_time,
        )
        self._appointments[appointment_id] = record
        return record

    def cancel_appointment(self, appointment_id: str) -> bool:
        if appointment_id in self._appointments:
            del self._appointments[appointment_id]
            return True
        return False

    def get_patient_schedule(self, patient_id: str) -> List[AppointmentRecord]:
        appointments = [
            record
            for record in self._appointments.values()
            if record.patient_id == patient_id
        ]
        return sorted(appointments, key=lambda record: record.scheduled_time)
