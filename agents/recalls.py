"""Recall automation agent for notifying patients of upcoming appointments."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence

logger = logging.getLogger(__name__)


class HaloSQLClientProtocol(Protocol):
    """Protocol describing the minimal interface required from a SQL client."""

    def fetch_all(self, query: str, params: Optional[Mapping[str, Any]] = None) -> Sequence[Mapping[str, Any]]:
        """Run a read-only SQL query and return an iterable of mappings."""

    def execute(self, query: str, params: Optional[Mapping[str, Any]] = None) -> None:
        """Execute a statement that mutates state (INSERT/UPDATE/DELETE)."""


class HaloFHIRClientProtocol(Protocol):
    """Protocol describing the minimal interface required from a FHIR client."""

    def get_patient(self, patient_id: str) -> Mapping[str, Any]:
        """Return the demographics for the patient identified by *patient_id*."""


@dataclass
class RecallRecord:
    """Normalized representation of a recall row retrieved from the database."""

    recall_id: str
    patient_id: str
    due: bool
    status: Optional[str] = None
    raw_payload: Mapping[str, Any] = field(default_factory=dict)

    @staticmethod
    def _normalize_boolean(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "t", "yes", "y"}
        return False

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "RecallRecord":
        if not row:
            raise ValueError("Recall row payload is empty")

        recall_id = cls._extract_first(row, ("RecallID", "recall_id", "id"))
        patient_id = cls._extract_first(row, ("PatientID", "patient_id", "patientId", "subject_id"))
        due_value = cls._extract_first(row, ("Due", "due", "is_due", "due_flag"), allow_missing=False)
        status = cls._extract_first(row, ("Status", "status"), allow_missing=True)

        return cls(
            recall_id=str(recall_id),
            patient_id=str(patient_id),
            due=cls._normalize_boolean(due_value),
            status=str(status) if status is not None else None,
            raw_payload=dict(row),
        )

    @staticmethod
    def _extract_first(
        row: Mapping[str, Any],
        keys: Sequence[str],
        *,
        allow_missing: bool = False,
    ) -> Any:
        for key in keys:
            if key in row and row[key] is not None:
                return row[key]
        if allow_missing:
            return None
        raise KeyError(f"Expected one of {keys!r} in recall row but none were present")


@dataclass
class RecallProcessingResult:
    recall_id: str
    patient_id: str
    success: bool
    message: str
    patient_name: Optional[str] = None
    contact_points: Dict[str, Any] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_report_entry(self) -> Dict[str, Any]:
        return {
            "recall_id": self.recall_id,
            "patient_id": self.patient_id,
            "success": self.success,
            "message": self.message,
            "patient_name": self.patient_name,
            "contact_points": self.contact_points,
            "details": self.details,
        }


def _run_sql_query(client: HaloSQLClientProtocol, query: str, params: Optional[Mapping[str, Any]] = None) -> Sequence[Mapping[str, Any]]:
    if hasattr(client, "fetch_all"):
        return client.fetch_all(query, params)
    if hasattr(client, "query"):
        return client.query(query, params)
    if hasattr(client, "select"):
        return client.select(query, params)
    raise AttributeError("HaloSQLClient implementation must provide fetch_all/query/select method")


def _run_sql_execute(client: HaloSQLClientProtocol, query: str, params: Optional[Mapping[str, Any]] = None) -> None:
    if hasattr(client, "execute"):
        client.execute(query, params)
        return
    if hasattr(client, "run"):
        client.run(query, params)
        return
    if hasattr(client, "update"):
        client.update(query, params)
        return
    raise AttributeError("HaloSQLClient implementation must provide execute/run/update method")


def _get_patient_demographics(client: HaloFHIRClientProtocol, patient_id: str) -> Mapping[str, Any]:
    if hasattr(client, "get_patient"):
        return client.get_patient(patient_id)
    if hasattr(client, "read_patient"):
        return client.read_patient(patient_id)
    if hasattr(client, "fetch_patient"):
        return client.fetch_patient(patient_id)
    if hasattr(client, "retrieve_patient"):
        return client.retrieve_patient(patient_id)
    raise AttributeError("HaloFHIRClient implementation must expose a patient retrieval method")


def send_reminder(patient: Mapping[str, Any], recall: RecallRecord) -> None:
    """Stub notification dispatcher.

    In production this function would integrate with email, SMS, or push providers.
    For now it simply logs the intention to send a reminder.
    """

    display_name = patient.get("name") or patient.get("fullName") or patient.get("display")
    logger.info(
        "Sending recall reminder for patient %s (recall_id=%s)",
        display_name or recall.patient_id,
        recall.recall_id,
    )


class RecallAgent:
    """Coordinates the recall notification workflow."""

    def __init__(
        self,
        sql_client: HaloSQLClientProtocol,
        fhir_client: HaloFHIRClientProtocol,
        *,
        report_path: Path | str = "recall_report.json",
        notification_status: str = "Notified",
    ) -> None:
        self._sql_client = sql_client
        self._fhir_client = fhir_client
        self._report_path = Path(report_path)
        self._notification_status = notification_status

    def run(self) -> Path:
        recalls = self._fetch_due_recalls()
        results: List[RecallProcessingResult] = []

        for recall in recalls:
            if not recall.due:
                logger.debug("Skipping recall %s because it is not due", recall.recall_id)
                continue

            logger.debug("Processing recall %s for patient %s", recall.recall_id, recall.patient_id)
            try:
                patient = self._fetch_patient_demographics(recall.patient_id)
                send_reminder(patient, recall)
                self._update_recall_status(recall)
                result = RecallProcessingResult(
                    recall_id=recall.recall_id,
                    patient_id=recall.patient_id,
                    success=True,
                    message="Reminder sent",
                    patient_name=_extract_patient_name(patient),
                    contact_points=_extract_contact_points(patient),
                    details={"status": self._notification_status},
                )
                logger.info("Recall %s notified successfully", recall.recall_id)
            except Exception as exc:  # noqa: BLE001 - we intentionally capture all failures per recall
                logger.exception("Failed to process recall %s", recall.recall_id)
                result = RecallProcessingResult(
                    recall_id=recall.recall_id,
                    patient_id=recall.patient_id,
                    success=False,
                    message=str(exc),
                    details={"raw_recall": recall.raw_payload},
                )
            results.append(result)

        self._write_report(recalls, results)
        return self._report_path

    def _fetch_due_recalls(self) -> List[RecallRecord]:
        query = "SELECT * FROM recalls WHERE Due = 1"
        rows = _run_sql_query(self._sql_client, query)
        recalls: List[RecallRecord] = []
        for row in rows:
            try:
                recall = RecallRecord.from_row(row)
            except Exception as exc:  # noqa: BLE001 - continue while capturing context
                logger.warning("Skipping invalid recall row %s: %s", row, exc)
                continue
            recalls.append(recall)
        return recalls

    def _fetch_patient_demographics(self, patient_id: str) -> Mapping[str, Any]:
        patient = _get_patient_demographics(self._fhir_client, patient_id)
        if not patient:
            raise ValueError(f"No demographics returned for patient {patient_id}")
        return patient

    def _update_recall_status(self, recall: RecallRecord) -> None:
        params: Dict[str, Any] = {
            "status": self._notification_status,
            "recall_id": recall.recall_id,
            "notified_at": datetime.now(timezone.utc).isoformat(),
        }
        if hasattr(self._sql_client, "update_recall_status"):
            self._sql_client.update_recall_status(recall.recall_id, params["status"], params["notified_at"])
            return

        update_statement = (
            "UPDATE recalls SET Due = 0, Status = %(status)s, LastNotifiedAt = %(notified_at)s "
            "WHERE RecallID = %(recall_id)s"
        )
        _run_sql_execute(self._sql_client, update_statement, params)

    def _write_report(self, recalls: Iterable[RecallRecord], results: Iterable[RecallProcessingResult]) -> None:
        report_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_due": sum(1 for recall in recalls if recall.due),
            "notifications": [result.to_report_entry() for result in results],
        }
        report_payload["total_notified"] = sum(1 for item in report_payload["notifications"] if item["success"])
        report_payload["total_failures"] = sum(1 for item in report_payload["notifications"] if not item["success"])

        self._report_path.parent.mkdir(parents=True, exist_ok=True)
        with self._report_path.open("w", encoding="utf-8") as handle:
            json.dump(report_payload, handle, indent=2, sort_keys=True)
        logger.info("Recall report written to %s", self._report_path)


def _extract_patient_name(patient: Mapping[str, Any]) -> Optional[str]:
    name = patient.get("name")
    if isinstance(name, str):
        return name
    if isinstance(name, Mapping):
        return name.get("text") or name.get("display")
    if isinstance(name, Sequence):
        for item in name:
            if isinstance(item, Mapping):
                candidate = item.get("text") or item.get("display")
                if candidate:
                    return candidate
            elif isinstance(item, str) and item.strip():
                return item
    return patient.get("fullName") or patient.get("display")


def _extract_contact_points(patient: Mapping[str, Any]) -> Dict[str, Any]:
    telecom = patient.get("telecom")
    contact_points: Dict[str, Any] = {}
    if isinstance(telecom, Sequence):
        for entry in telecom:
            if isinstance(entry, Mapping):
                system = entry.get("system")
                value = entry.get("value")
                if system and value:
                    contact_points.setdefault(system, []).append(value)
    else:
        for key in ("phone", "email"):
            if key in patient and patient[key]:
                contact_points.setdefault(key, []).append(patient[key])
    return contact_points


__all__ = [
    "HaloFHIRClientProtocol",
    "HaloSQLClientProtocol",
    "RecallAgent",
    "RecallProcessingResult",
    "RecallRecord",
    "send_reminder",
]
