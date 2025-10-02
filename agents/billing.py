"""Billing agent responsible for generating and submitting FHIR claims."""
from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import requests


LOGGER = logging.getLogger("billing_agent")


class HaloFHIRClientError(RuntimeError):
    """Raised when the Halo FHIR client encounters an error."""


class BillingConfigurationError(RuntimeError):
    """Raised when the billing agent configuration is invalid."""


@dataclass
class ClaimSubmissionResult:
    """Represents the final status of a submitted claim."""

    claim_id: str
    appointment_id: str
    status: str
    billing_code: str
    amount: Decimal
    reason: Optional[str] = None
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class HaloFHIRClient:
    """Thin wrapper for Halo's FHIR endpoints."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 10.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required for HaloFHIRClient")
        if not api_key:
            raise ValueError("api_key is required for HaloFHIRClient")

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.session = session or requests.Session()

    def _request(self, method: str, path: str, **kwargs) -> Dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = kwargs.pop("headers", {})
        headers.setdefault("Authorization", f"Bearer {self.api_key}")
        headers.setdefault("Content-Type", "application/fhir+json")
        headers.setdefault("Accept", "application/fhir+json")

        try:
            response = self.session.request(
                method,
                url,
                timeout=self.timeout,
                headers=headers,
                **kwargs,
            )
        except requests.RequestException as exc:  # pragma: no cover - network failure path
            raise HaloFHIRClientError(f"Halo FHIR request failed: {exc}") from exc

        if not response.ok:
            message = (
                f"Halo FHIR request failed (status={response.status_code}): {response.text}"
            )
            raise HaloFHIRClientError(message)

        if not response.content:
            return {}

        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - malformed response
            raise HaloFHIRClientError("Halo FHIR response was not valid JSON") from exc

    def get_completed_appointments(self, start: datetime, end: datetime) -> Sequence[Dict]:
        params = {
            "status": "completed",
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
        payload = self._request("GET", "/appointments", params=params)
        appointments = payload.get("appointments")
        if appointments is None and isinstance(payload, list):
            appointments = payload
        if appointments is None:
            appointments = []
        return appointments

    def submit_claim(self, claim_resource: Dict) -> Dict:
        return self._request("POST", "/Claim", json=claim_resource)

    def get_claim_status(self, claim_id: str) -> Dict:
        if not claim_id:
            raise ValueError("claim_id is required to look up claim status")
        return self._request("GET", f"/Claim/{claim_id}")


class BillingAgent:
    """Coordinates appointment retrieval, claim generation, and submission."""

    def __init__(
        self,
        halo_client: HaloFHIRClient,
        billing_code_path: Path | str,
        *,
        report_path: Path | str = Path("daily_claim_report.csv"),
    ) -> None:
        billing_code_path = Path(billing_code_path)
        report_path = Path(report_path)

        if not billing_code_path.exists():
            raise BillingConfigurationError(
                f"Billing code file not found: {billing_code_path!s}"
            )

        self.halo_client = halo_client
        self.billing_code_path = billing_code_path
        self.report_path = report_path
        self.billing_codes = self._load_billing_codes(billing_code_path)
        self.claim_registry: Dict[str, ClaimSubmissionResult] = {}

    @staticmethod
    def _load_billing_codes(path: Path) -> Dict[str, Dict]:
        with path.open("r", encoding="utf-8") as handle:
            codes = json.load(handle)
        if not isinstance(codes, dict):
            raise BillingConfigurationError("Billing code file must contain a JSON object")
        return codes

    @staticmethod
    def _normalize_decimal(value: float | str | Decimal) -> Decimal:
        try:
            normalized = Decimal(str(value)).quantize(Decimal("0.01"))
        except (InvalidOperation, TypeError) as exc:
            raise BillingConfigurationError(f"Invalid currency amount: {value}") from exc
        return normalized

    @staticmethod
    def _appointment_window(target_date: date) -> tuple[datetime, datetime]:
        start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
        end = datetime.combine(target_date, time.max, tzinfo=timezone.utc)
        return start, end

    def run_daily_billing(self, target_date: Optional[date] = None) -> List[ClaimSubmissionResult]:
        target_date = target_date or date.today()
        start, end = self._appointment_window(target_date)
        LOGGER.info("Fetching completed appointments from %s to %s", start, end)

        appointments = self.halo_client.get_completed_appointments(start, end)
        results: List[ClaimSubmissionResult] = []

        for appointment in appointments:
            try:
                result = self._process_appointment(appointment)
            except BillingConfigurationError as exc:
                LOGGER.error(
                    "Failed to process appointment %s: %s",
                    appointment.get("id", "<unknown>"),
                    exc,
                )
                continue
            except HaloFHIRClientError as exc:
                LOGGER.error(
                    "Halo client error for appointment %s: %s",
                    appointment.get("id", "<unknown>"),
                    exc,
                )
                continue

            results.append(result)
            self.claim_registry[result.claim_id] = result

        self._export_report(results)
        return results

    def _process_appointment(self, appointment: Dict) -> ClaimSubmissionResult:
        appointment_id = str(
            appointment.get("id")
            or appointment.get("appointmentId")
            or appointment.get("resource", {}).get("id")
            or ""
        )
        if not appointment_id:
            raise BillingConfigurationError("Appointment is missing a unique identifier")
        appointment_type = (
            appointment.get("appointmentType")
            or appointment.get("type")
            or appointment.get("reason")
        )
        if not appointment_type:
            raise BillingConfigurationError(
                f"Appointment {appointment_id} is missing an appointment type"
            )

        code_info = self.billing_codes.get(appointment_type)
        if code_info is None:
            raise BillingConfigurationError(
                f"No billing code mapping found for appointment type '{appointment_type}'"
            )

        charge_amount = self._normalize_decimal(
            appointment.get("chargeAmount", code_info.get("charge_amount", "0"))
        )

        claim_resource = self._build_claim_resource(appointment, code_info, charge_amount)
        submission = self.halo_client.submit_claim(claim_resource)
        claim_id = str(
            submission.get("id")
            or submission.get("claimId")
            or submission.get("identifier")
            or ""
        )
        if not claim_id:
            raise HaloFHIRClientError(
                f"Claim submission response did not include an identifier: {submission}"
            )

        status_payload = self.halo_client.get_claim_status(claim_id)
        status, reason = self._parse_claim_status(status_payload)

        if status.lower() in {"rejected", "denied", "error"}:
            LOGGER.error(
                "Claim %s for appointment %s rejected: %s",
                claim_id,
                appointment_id,
                reason or "No rejection reason supplied",
            )
        else:
            LOGGER.info(
                "Claim %s for appointment %s processed with status '%s'",
                claim_id,
                appointment_id,
                status,
            )

        return ClaimSubmissionResult(
            claim_id=claim_id,
            appointment_id=appointment_id,
            status=status,
            billing_code=str(code_info.get("procedure_code")),
            amount=charge_amount,
            reason=reason,
        )

    @staticmethod
    def _build_claim_resource(
        appointment: Dict,
        code_info: Dict,
        amount: Decimal,
    ) -> Dict:
        patient_id = appointment.get("patientId") or appointment.get("patient", {}).get("id")
        provider_id = appointment.get("practitionerId") or appointment.get("providerId")
        if not patient_id or not provider_id:
            raise BillingConfigurationError(
                "Appointment must include patientId and practitionerId for claim generation"
            )

        encounter_id = appointment.get("encounterId")
        coverage_id = appointment.get("insurancePlanId") or appointment.get("coverageId")
        appointment_start = appointment.get("start")
        created_dt = datetime.now(timezone.utc).isoformat()

        product_coding = {
            "system": "http://www.ama-assn.org/go/cpt",
            "code": str(code_info.get("procedure_code")),
            "display": code_info.get("display"),
        }

        diagnosis_codes = appointment.get("diagnosisCodes") or []
        diagnosis: List[Dict] = []
        for index, diagnosis_code in enumerate(diagnosis_codes, start=1):
            diagnosis.append(
                {
                    "sequence": index,
                    "diagnosisCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/sid/icd-10",
                                "code": diagnosis_code,
                            }
                        ]
                    },
                }
            )

        item = {
            "sequence": 1,
            "productOrService": {"coding": [product_coding]},
            "net": {
                "value": float(amount),
                "currency": "USD",
            },
            "unitPrice": {
                "value": float(amount),
                "currency": "USD",
            },
        }

        claim: Dict = {
            "resourceType": "Claim",
            "status": "active",
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                        "code": "professional",
                    }
                ]
            },
            "use": "claim",
            "patient": {"reference": f"Patient/{patient_id}"},
            "created": created_dt,
            "provider": {"reference": f"Practitioner/{provider_id}"},
            "priority": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/processpriority",
                        "code": "normal",
                    }
                ]
            },
            "item": [item],
            "total": {
                "value": float(amount),
                "currency": "USD",
            },
        }

        if appointment_start:
            claim["billablePeriod"] = {
                "start": appointment_start,
                "end": appointment.get("end", appointment_start),
            }

        if encounter_id:
            claim["encounter"] = [{"reference": f"Encounter/{encounter_id}"}]

        if coverage_id:
            claim["insurance"] = [
                {
                    "sequence": 1,
                    "focal": True,
                    "coverage": {"reference": f"Coverage/{coverage_id}"},
                }
            ]

        if diagnosis:
            claim["diagnosis"] = diagnosis

        return claim

    @staticmethod
    def _parse_claim_status(payload: Dict) -> tuple[str, Optional[str]]:
        if not payload:
            return "unknown", None

        status = payload.get("status") or payload.get("resource", {}).get("status") or "unknown"
        outcome = payload.get("outcome") or payload.get("resource", {}).get("outcome")

        reason_messages: List[str] = []
        adjudication = payload.get("error") or payload.get("issue") or []
        if isinstance(adjudication, dict):
            adjudication = [adjudication]

        for issue in adjudication:
            code = issue.get("code") or issue.get("details", {}).get("code")
            details = (
                issue.get("diagnostics")
                or issue.get("details", {}).get("text")
                or issue.get("details")
            )
            if code and details:
                reason_messages.append(f"{code}: {details}")
            elif code:
                reason_messages.append(str(code))
            elif details:
                reason_messages.append(str(details))

        if not reason_messages and outcome in {"error", "rejected", "denied"}:
            reason_messages.append(outcome)

        reason = "; ".join(reason_messages) or None
        return status, reason

    def _export_report(self, results: Iterable[ClaimSubmissionResult]) -> None:
        report_path = self.report_path
        report_dir = report_path.parent
        if report_dir and not report_dir.exists():
            report_dir.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "submitted_at",
            "appointment_id",
            "claim_id",
            "status",
            "billing_code",
            "amount",
            "rejection_reason",
        ]

        results = list(results)
        file_exists = report_path.exists()

        with report_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for result in results:
                writer.writerow(
                    {
                        "submitted_at": result.submitted_at.isoformat(),
                        "appointment_id": result.appointment_id,
                        "claim_id": result.claim_id,
                        "status": result.status,
                        "billing_code": result.billing_code,
                        "amount": f"{result.amount:.2f}",
                        "rejection_reason": result.reason or "",
                    }
                )


__all__ = [
    "BillingAgent",
    "ClaimSubmissionResult",
    "HaloFHIRClient",
    "HaloFHIRClientError",
    "BillingConfigurationError",
]
