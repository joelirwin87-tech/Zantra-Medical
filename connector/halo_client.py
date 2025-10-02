"""Halo Connect client utilities.

This module provides high-level clients to interact with Halo Connect's
FHIR and SQL interfaces. The clients manage OAuth2 authentication, HTTP
session handling with retries, and structured error reporting to make it
safe to integrate with Halo Connect from backend services.
"""
from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

__all__ = ["HaloClientError", "HaloAuthError", "HaloAPIError", "HaloFHIRClient", "HaloSQLClient"]


# Configure module-level logging. The default log level can be overridden by the
# hosting application. We avoid configuring handlers to prevent duplicate logs
# if the application already set up logging.
logger = logging.getLogger(__name__)


DEFAULT_TOKEN_REFRESH_BUFFER_SECONDS = 60
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5

DEFAULT_TOKEN_URL = os.getenv("HALO_TOKEN_URL", "https://api.haloconnect.com/oauth2/token")
DEFAULT_FHIR_BASE_URL = os.getenv("HALO_FHIR_BASE_URL", "https://api.haloconnect.com/fhir")
DEFAULT_SQL_BASE_URL = os.getenv("HALO_SQL_BASE_URL", "https://api.haloconnect.com/sql")
DEFAULT_CLIENT_ID = os.getenv("HALO_CLIENT_ID")
DEFAULT_CLIENT_SECRET = os.getenv("HALO_CLIENT_SECRET")
DEFAULT_SCOPE = os.getenv("HALO_SCOPE", "")
DEFAULT_AUDIENCE = os.getenv("HALO_AUDIENCE", "")


class HaloClientError(RuntimeError):
    """Base exception for Halo Connect client errors."""


class HaloAuthError(HaloClientError):
    """Raised when OAuth2 authentication fails."""


class HaloAPIError(HaloClientError):
    """Raised when Halo Connect returns an error response."""


@dataclass(frozen=True)
class TokenData:
    """Container for OAuth2 token information."""

    access_token: str
    expires_at: datetime

    def is_valid(self, buffer_seconds: int) -> bool:
        """Check if the token is still valid with a refresh buffer."""

        return datetime.utcnow() + timedelta(seconds=buffer_seconds) < self.expires_at


class HaloBaseClient:
    """Shared functionality for Halo Connect API clients."""

    def __init__(
        self,
        *,
        token_url: str = DEFAULT_TOKEN_URL,
        base_url: str,
        client_id: Optional[str] = DEFAULT_CLIENT_ID,
        client_secret: Optional[str] = DEFAULT_CLIENT_SECRET,
        scope: Optional[str] = DEFAULT_SCOPE,
        audience: Optional[str] = DEFAULT_AUDIENCE,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        token_refresh_buffer: int = DEFAULT_TOKEN_REFRESH_BUFFER_SECONDS,
    ) -> None:
        if not base_url:
            raise ValueError("base_url must be provided")
        if not token_url:
            raise ValueError("token_url must be provided")
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret must be provided")

        self.base_url = base_url.rstrip("/")
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope or ""
        self.audience = audience or ""
        self.timeout = timeout
        self.token_refresh_buffer = token_refresh_buffer

        self._session = self._build_session(max_retries=max_retries, backoff_factor=backoff_factor)
        self._token_lock = threading.Lock()
        self._token: Optional[TokenData] = None

    def _build_session(self, *, max_retries: int, backoff_factor: float) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_access_token(self) -> str:
        token = self._token
        if token and token.is_valid(self.token_refresh_buffer):
            return token.access_token

        with self._token_lock:
            token = self._token
            if token and token.is_valid(self.token_refresh_buffer):
                return token.access_token

            logger.debug("Refreshing Halo Connect OAuth2 token")
            try:
                payload = {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
                if self.scope:
                    payload["scope"] = self.scope
                if self.audience:
                    payload["audience"] = self.audience

                response = self._session.post(self.token_url, data=payload, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as exc:  # network-level errors
                logger.error("Failed to obtain Halo Connect token: %s", exc)
                raise HaloAuthError("Failed to obtain Halo Connect token") from exc
            except ValueError as exc:  # response.json() failure
                logger.error("Invalid token response received from Halo Connect: %s", exc)
                raise HaloAuthError("Invalid token response from Halo Connect") from exc

            access_token = data.get("access_token")
            expires_in = data.get("expires_in")
            if not access_token or not isinstance(access_token, str):
                logger.error("Token response did not include access_token: %s", data)
                raise HaloAuthError("Token response missing access_token")
            if not expires_in:
                logger.warning("Token response missing expires_in; defaulting to 5 minutes")
                expires_in = 300

            try:
                expires_in_int = int(expires_in)
            except (TypeError, ValueError) as exc:
                logger.error("Invalid expires_in value in token response: %s", expires_in)
                raise HaloAuthError("Invalid expires_in value in token response") from exc

            expires_at = datetime.utcnow() + timedelta(seconds=expires_in_int)
            token = TokenData(access_token=access_token, expires_at=expires_at)
            self._token = token
            logger.info("Halo Connect token refreshed; expires at %s", expires_at.isoformat())
            return token.access_token

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_payload: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        expected_status: Union[int, Tuple[int, ...]] = (200,),
    ) -> Response:
        if not path:
            raise ValueError("path must be provided")
        if isinstance(expected_status, int):
            expected_status = (expected_status,)

        token = self._get_access_token()
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if headers:
            request_headers.update(headers)

        try:
            response = self._session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json_payload,
                headers=request_headers,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            logger.error("Request to Halo Connect failed: %s", exc)
            raise HaloAPIError("Failed to execute request to Halo Connect") from exc

        if response.status_code not in expected_status:
            self._log_error_response(response)
            raise HaloAPIError(
                f"Halo Connect responded with unexpected status {response.status_code}: {response.text}"
            )

        return response

    @staticmethod
    def _log_error_response(response: Response) -> None:
        content_type = response.headers.get("Content-Type", "")
        if "json" in content_type:
            try:
                parsed = response.json()
                logger.error("Halo Connect error response: status=%s body=%s", response.status_code, parsed)
                return
            except ValueError:
                pass
        logger.error(
            "Halo Connect error response: status=%s body=%s", response.status_code, response.text[:2048]
        )


class HaloFHIRClient(HaloBaseClient):
    """Client for Halo Connect's FHIR API."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_FHIR_BASE_URL,
        token_url: str = DEFAULT_TOKEN_URL,
        client_id: Optional[str] = DEFAULT_CLIENT_ID,
        client_secret: Optional[str] = DEFAULT_CLIENT_SECRET,
        scope: Optional[str] = DEFAULT_SCOPE,
        audience: Optional[str] = DEFAULT_AUDIENCE,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        token_refresh_buffer: int = DEFAULT_TOKEN_REFRESH_BUFFER_SECONDS,
    ) -> None:
        super().__init__(
            token_url=token_url,
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
            scope=scope,
            audience=audience,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            token_refresh_buffer=token_refresh_buffer,
        )

    def get_patient(self, patient_id: str) -> Dict[str, Any]:
        """Retrieve a FHIR Patient resource by identifier."""

        if not patient_id:
            raise ValueError("patient_id must be provided")
        response = self._request("GET", f"Patient/{patient_id}", headers={"Accept": "application/fhir+json"})
        return response.json()

    def get_appointment(self, appointment_id: str) -> Dict[str, Any]:
        """Retrieve a FHIR Appointment resource by identifier."""

        if not appointment_id:
            raise ValueError("appointment_id must be provided")
        response = self._request(
            "GET",
            f"Appointment/{appointment_id}",
            headers={"Accept": "application/fhir+json"},
        )
        return response.json()

    def search_appointments(self, patient_id: str, **search_params: Any) -> Dict[str, Any]:
        """Search appointments associated with a patient.

        Additional FHIR search parameters can be supplied as keyword arguments.
        """

        if not patient_id:
            raise ValueError("patient_id must be provided")

        params: Dict[str, Any] = {"patient": patient_id}
        params.update({k: v for k, v in search_params.items() if v is not None})
        response = self._request(
            "GET",
            "Appointment",
            params=params,
            headers={"Accept": "application/fhir+json"},
        )
        return response.json()

    def create_appointment(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new appointment using a FHIR Appointment resource payload."""

        if not isinstance(payload, dict) or not payload:
            raise ValueError("payload must be a non-empty dictionary")
        response = self._request(
            "POST",
            "Appointment",
            json_payload=payload,
            headers={"Accept": "application/fhir+json"},
            expected_status=(200, 201),
        )
        return response.json()


class HaloSQLClient(HaloBaseClient):
    """Client for Halo Connect's SQL API."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_SQL_BASE_URL,
        token_url: str = DEFAULT_TOKEN_URL,
        client_id: Optional[str] = DEFAULT_CLIENT_ID,
        client_secret: Optional[str] = DEFAULT_CLIENT_SECRET,
        scope: Optional[str] = DEFAULT_SCOPE,
        audience: Optional[str] = DEFAULT_AUDIENCE,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        token_refresh_buffer: int = DEFAULT_TOKEN_REFRESH_BUFFER_SECONDS,
    ) -> None:
        super().__init__(
            token_url=token_url,
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
            scope=scope,
            audience=audience,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            token_refresh_buffer=token_refresh_buffer,
        )

    def run_sql(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query using Halo Connect's SQL API."""

        if not query or not isinstance(query, str):
            raise ValueError("query must be a non-empty string")

        payload = {"query": query}
        response = self._request("POST", "query", json_payload=payload, expected_status=(200, 201))
        return response.json()
