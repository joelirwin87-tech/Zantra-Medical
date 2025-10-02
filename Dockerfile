FROM python:3.11-slim

WORKDIR /app

COPY orchestrator /app/orchestrator
COPY README.md /app/README.md

ENV PYTHONUNBUFFERED=1

CMD ["python", "orchestrator/main.py", "run_scheduler"]
