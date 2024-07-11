FROM python:3.9-slim

WORKDIR /app

COPY ../common /app/common

# Copy the application directory
COPY ./application /app/application

COPY ./application/worker/loading/requirements.txt /app/application/worker/loading/requirements.txt

RUN pip install --no-cache-dir -r /app/application/worker/loading/requirements.txt

ENV PYTHONPATH /app

EXPOSE 8005

CMD ["uvicorn", "application.worker.loading.worker.loading_worker:run_loadings", "--host", "0.0.0.0", "--port", "8005"]
