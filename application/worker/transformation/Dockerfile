FROM python:3.9-slim

WORKDIR /app

COPY ../common /app/common

# Copy the application directory
COPY ./application /app/application

COPY ./application/worker/transformation/requirements.txt /app/application/worker/transformation/requirements.txt

RUN pip install --no-cache-dir -r /app/application/worker/transformation/requirements.txt

ENV PYTHONPATH /app

EXPOSE 8004

CMD ["uvicorn", "application.worker.transformation.worker.transformation_worker:run_transformations", "--host", "0.0.0.0", "--port", "8004"]