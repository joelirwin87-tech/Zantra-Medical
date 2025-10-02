"""Appointment agent providing scheduling operations."""

from __future__ import annotations

from datetime import datetime as DateTime
from typing import Dict, Optional

from connector import HaloFHIRClient, HaloSQLClient

SQL_CLIENT = HaloSQLClient()
FHIR_CLIENT = HaloFHIRClient()


def send_notification(recipient_id: str, message: str) -> None:
    """Stub notification sender for email/SMS reminders."""

    # Intentionally left as a stub for integration with messaging services.
    return None


def _validate_identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value.strip()


def _require_datetime(appointment_datetime: DateTime) -> DateTime:
    if not isinstance(appointment_datetime, DateTime):
        raise TypeError("datetime must be a datetime instance")
    return appointment_datetime


def book_appointment(
    patient_id: str,
    provider_id: str,
    datetime: DateTime,
    *,
    sql_client: Optional[HaloSQLClient] = None,
    fhir_client: Optional[HaloFHIRClient] = None,
) -> Dict[str, object]:
    """Book an appointment if the slot is available and notify the patient."""

    patient_id = _validate_identifier(patient_id, "patient_id")
    provider_id = _validate_identifier(provider_id, "provider_id")
    appointment_datetime = _require_datetime(datetime)

    sql_client = sql_client or SQL_CLIENT
    fhir_client = fhir_client or FHIR_CLIENT

    if not fhir_client.patient_exists(patient_id):
        raise ValueError(f"Patient '{patient_id}' is not registered")

    if not sql_client.is_slot_available(provider_id, appointment_datetime):
        raise ValueError("Requested time slot is unavailable")

    record = sql_client.create_appointment(
        patient_id, provider_id, appointment_datetime
    )

    message = (
        f"Appointment confirmed with provider {provider_id} "
        f"on {appointment_datetime.isoformat()}"
    )
    send_notification(patient_id, message)

    return {
        "appointment_id": record.appointment_id,
        "patient_id": record.patient_id,
        "provider_id": record.provider_id,
        "datetime": record.scheduled_time,
    }


def cancel_appointment(
    appointment_id: str,
    *,
    sql_client: Optional[HaloSQLClient] = None,
) -> None:
    """Cancel an existing appointment."""

    appointment_id = _validate_identifier(appointment_id, "appointment_id")
    sql_client = sql_client or SQL_CLIENT

    if not sql_client.cancel_appointment(appointment_id):
        raise ValueError(f"Appointment '{appointment_id}' does not exist")


def get_patient_schedule(
    patient_id: str,
    *,
    sql_client: Optional[HaloSQLClient] = None,
) -> list:
    """Retrieve the patient's scheduled appointments."""

    patient_id = _validate_identifier(patient_id, "patient_id")
    sql_client = sql_client or SQL_CLIENT

    records = sql_client.get_patient_schedule(patient_id)
    return [
        {
            "appointment_id": record.appointment_id,
            "patient_id": record.patient_id,
            "provider_id": record.provider_id,
            "datetime": record.scheduled_time,
        }
        for record in records
    ]
