FROM python:3.10-slim

WORKDIR /app

# Copy the common directory
COPY ./common /app/common

# Copy the pipeline application directory
COPY ./application/pipeline /app/application/pipeline

# Copy the requirements file
COPY ./application/pipeline/requirements.txt /app/requirements.txt

# Copy example files
COPY ./examples/example_files /app/example_files

RUN apt-get update && apt-get install -y \
    build-essential \
    make \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set the Python path
ENV PYTHONPATH /app

# Expose the port
EXPOSE 8002

# Run the application
CMD ["uvicorn", "application.pipeline.start_pipeline:app", "--host", "0.0.0.0", "--port", "8100"]