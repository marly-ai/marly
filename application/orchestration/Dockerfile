FROM python:3.9-slim

WORKDIR /app

COPY ../common /app/common

# Copy the application directory
COPY ./application /app/application

COPY ./application/orchestration/requirements.txt /app/application/orchestration/requirements.txt

RUN pip install --no-cache-dir -r /app/application/orchestration/requirements.txt

ENV PYTHONPATH /app

EXPOSE 8002

CMD ["uvicorn", "application.orchestration.start_orchestrator:app", "--host", "0.0.0.0", "--port", "8002"]