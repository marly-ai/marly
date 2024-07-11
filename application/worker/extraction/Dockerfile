FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    libmupdf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ../common /app/common

# Copy the application directory
COPY ./application /app/application

COPY ./application/worker/extraction/requirements.txt /app/application/worker/extraction/requirements.txt

RUN pip install --no-cache-dir -r /app/application/worker/extraction/requirements.txt

ENV PYTHONPATH /app

EXPOSE 8003

CMD ["uvicorn", "application.worker.extraction.worker.extraction_worker:run_extractions", "--host", "0.0.0.0", "--port", "8003"]

