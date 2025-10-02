"""Dashboard web application for Zantra Medical agents.

This module exposes a small Flask application that surfaces operational
metrics and provides access to raw agent task logs. Data is loaded from
JSON files inside the repository. Missing files are tolerated so the
application can run in environments where the datasets have not yet been
populated.
"""
from __future__ import annotations

from datetime import date, datetime
import json
import os
from pathlib import Path
from typing import Iterable, List, MutableMapping, Sequence

from flask import Flask, Response, jsonify, render_template_string, request

DATE_FORMAT = "%Y-%m-%d"


class DashboardRepository:
    """Repository responsible for loading dashboard data from disk."""

    def __init__(self, base_path: Path | None = None) -> None:
        repo_root = base_path or Path(__file__).resolve().parents[1]
        self._data_dir = repo_root / "data"
        self._appointments_file = self._data_dir / "appointments.json"
        self._recalls_file = self._data_dir / "recalls.json"
        self._claims_file = self._data_dir / "claims.json"
        self._agent_logs_file = self._data_dir / "agent_logs.json"

    def _load_collection(self, file_path: Path) -> List[MutableMapping[str, object]]:
        if not file_path.exists():
            return []
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return []
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, MutableMapping)]
        return []

    def get_agent_logs(self) -> List[MutableMapping[str, object]]:
        return self._load_collection(self._agent_logs_file)

    def get_appointments(self) -> List[MutableMapping[str, object]]:
        return self._load_collection(self._appointments_file)

    def get_recalls(self) -> List[MutableMapping[str, object]]:
        return self._load_collection(self._recalls_file)

    def get_claims(self) -> List[MutableMapping[str, object]]:
        return self._load_collection(self._claims_file)


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError:
        return None


def filter_records(
    records: Sequence[MutableMapping[str, object]],
    target_date: date | None,
    provider: str | None,
) -> List[MutableMapping[str, object]]:
    provider_normalized = provider.lower() if provider else None
    filtered: List[MutableMapping[str, object]] = []
    for record in records:
        record_date = parse_iso_date(str(record.get("date")))
        record_provider = str(record.get("provider", ""))
        if target_date and record_date != target_date:
            continue
        if provider_normalized and record_provider.lower() != provider_normalized:
            continue
        filtered.append(record)
    return filtered


def collect_providers(*collections: Iterable[MutableMapping[str, object]]) -> List[str]:
    seen = set()
    providers: List[str] = []
    for collection in collections:
        for record in collection:
            provider = str(record.get("provider", "")).strip()
            if provider and provider not in seen:
                providers.append(provider)
                seen.add(provider)
    return sorted(providers)


def build_dashboard_context(
    repo: DashboardRepository,
    target_date: date | None,
    provider: str | None,
) -> MutableMapping[str, object]:
    appointments_all = repo.get_appointments()
    recalls_all = repo.get_recalls()
    claims_all = repo.get_claims()

    appointments = filter_records(appointments_all, target_date, provider)
    recalls = filter_records(recalls_all, target_date, provider)
    claims = filter_records(claims_all, target_date, provider)

    providers = collect_providers(appointments_all, recalls_all, claims_all)
    logs = repo.get_agent_logs()

    return {
        "filters": {
            "date": target_date.strftime(DATE_FORMAT) if target_date else "",
            "provider": provider or "",
            "available_providers": providers,
        },
        "appointments": appointments,
        "recalls": recalls,
        "claims": claims,
        "logs": logs,
    }


app = Flask(__name__)
repository = DashboardRepository()

dashboard_template = """
<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <meta http-equiv=\"refresh\" content=\"60\">
    <title>Zantra Medical Dashboard</title>
    <link
      href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css\"
      rel=\"stylesheet\"
      integrity=\"sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH\"
      crossorigin=\"anonymous\"
    >
  </head>
  <body class=\"bg-light\">
    <nav class=\"navbar navbar-expand-lg navbar-dark bg-primary\">
      <div class=\"container-fluid\">
        <a class=\"navbar-brand\" href=\"#\">Zantra Medical Dashboard</a>
      </div>
    </nav>
    <main class=\"container my-4\">
      <section class=\"mb-4\">
        <form class=\"row gy-2 gx-3 align-items-center\" method=\"get\" action=\"/dashboard\" aria-label=\"Dashboard filters\">
          <div class=\"col-md-3\">
            <label for=\"filter-date\" class=\"form-label\">Date</label>
            <input
              id=\"filter-date\"
              name=\"date\"
              type=\"date\"
              class=\"form-control\"
              value=\"{{ filters.date }}\"
            >
          </div>
          <div class=\"col-md-3\">
            <label for=\"filter-provider\" class=\"form-label\">Provider</label>
            <select id=\"filter-provider\" name=\"provider\" class=\"form-select\">
              <option value=\"\">All Providers</option>
              {% for option in filters.available_providers %}
                <option value=\"{{ option }}\" {% if option == filters.provider %}selected{% endif %}>{{ option }}</option>
              {% endfor %}
            </select>
          </div>
          <div class=\"col-md-3 align-self-end\">
            <button type=\"submit\" class=\"btn btn-primary w-100\">Apply Filters</button>
          </div>
        </form>
      </section>
      <section class=\"row g-4\">
        <div class=\"col-lg-4\">
          <div class=\"card shadow-sm h-100\">
            <div class=\"card-header bg-success text-white\">Appointments Booked Today</div>
            <div class=\"card-body\">
              {% if appointments %}
                <div class=\"table-responsive\">
                  <table class=\"table table-sm table-striped\">
                    <thead>
                      <tr>
                        <th scope=\"col\">Patient</th>
                        <th scope=\"col\">Provider</th>
                        <th scope=\"col\">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {% for appointment in appointments %}
                        <tr>
                          <td>{{ appointment.patient or '—' }}</td>
                          <td>{{ appointment.provider or '—' }}</td>
                          <td>{{ appointment.time or '—' }}</td>
                        </tr>
                      {% endfor %}
                    </tbody>
                  </table>
                </div>
              {% else %}
                <p class=\"text-muted mb-0\">No appointments found for the selected filters.</p>
              {% endif %}
            </div>
          </div>
        </div>
        <div class=\"col-lg-4\">
          <div class=\"card shadow-sm h-100\">
            <div class=\"card-header bg-info text-white\">Recalls Processed Today</div>
            <div class=\"card-body\">
              {% if recalls %}
                <div class=\"table-responsive\">
                  <table class=\"table table-sm table-striped\">
                    <thead>
                      <tr>
                        <th scope=\"col\">Patient</th>
                        <th scope=\"col\">Provider</th>
                        <th scope=\"col\">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {% for recall in recalls %}
                        <tr>
                          <td>{{ recall.patient or '—' }}</td>
                          <td>{{ recall.provider or '—' }}</td>
                          <td>{{ recall.status or '—' }}</td>
                        </tr>
                      {% endfor %}
                    </tbody>
                  </table>
                </div>
              {% else %}
                <p class=\"text-muted mb-0\">No recalls found for the selected filters.</p>
              {% endif %}
            </div>
          </div>
        </div>
        <div class=\"col-lg-4\">
          <div class=\"card shadow-sm h-100\">
            <div class=\"card-header bg-warning text-dark\">Claims Submitted Today</div>
            <div class=\"card-body\">
              {% if claims %}
                <div class=\"table-responsive\">
                  <table class=\"table table-sm table-striped\">
                    <thead>
                      <tr>
                        <th scope=\"col\">Patient</th>
                        <th scope=\"col\">Provider</th>
                        <th scope=\"col\">Amount</th>
                      </tr>
                    </thead>
                    <tbody>
                      {% for claim in claims %}
                        <tr>
                          <td>{{ claim.patient or '—' }}</td>
                          <td>{{ claim.provider or '—' }}</td>
                          <td>{{ claim.amount or '—' }}</td>
                        </tr>
                      {% endfor %}
                    </tbody>
                  </table>
                </div>
              {% else %}
                <p class=\"text-muted mb-0\">No claims found for the selected filters.</p>
              {% endif %}
            </div>
          </div>
        </div>
      </section>
      <section class=\"mt-5\">
        <div class=\"card shadow-sm\">
          <div class=\"card-header bg-secondary text-white\">Agent Audit Log</div>
          <div class=\"card-body\">
            {% if logs %}
              <div class=\"table-responsive\">
                <table class=\"table table-sm table-striped\">
                  <thead>
                    <tr>
                      <th scope=\"col\">Timestamp</th>
                      <th scope=\"col\">Agent</th>
                      <th scope=\"col\">Action</th>
                      <th scope=\"col\">Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {% for log in logs %}
                      <tr>
                        <td>{{ log.timestamp or '—' }}</td>
                        <td>{{ log.agent or '—' }}</td>
                        <td>{{ log.action or '—' }}</td>
                        <td>{{ log.details or '—' }}</td>
                      </tr>
                    {% endfor %}
                  </tbody>
                </table>
              </div>
            {% else %}
              <p class=\"text-muted mb-0\">No audit log entries available.</p>
            {% endif %}
          </div>
        </div>
      </section>
    </main>
    <script
      src=\"https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js\"
      integrity=\"sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz\"
      crossorigin=\"anonymous\"
    ></script>
  </body>
</html>
"""


@app.route("/tasks", methods=["GET"])
def tasks() -> Response:
    """Return agent log entries as JSON."""
    return jsonify(repository.get_agent_logs())


@app.route("/dashboard", methods=["GET"])
def dashboard() -> str:
    target_date = parse_iso_date(request.args.get("date")) or date.today()
    provider = request.args.get("provider") or None
    context = build_dashboard_context(repository, target_date, provider)
    return render_template_string(dashboard_template, **context)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=False,
    )
