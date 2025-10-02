# Data Directory

Place optional seed data for the orchestrator agents in this folder.

- `appointments.json` — list of appointment records for the recall workflow. Each record should include `patient_id`, `patient_name`, `appointment_date`, and optionally `needs_recall`.
- `claims.json` — list of pending claims for billing submissions. Each entry should include `claim_id`, `patient_id`, and `amount`.
