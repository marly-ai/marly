FROM python:3.9-slim

WORKDIR /app

# Copy the common directory
COPY ./common /app/common

# Copy the transformation application directory
COPY ./application/transformation /app/application/transformation

# Copy the requirements file
COPY ./application/transformation/requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set the Python path
ENV PYTHONPATH /app

# Run the transformation service
CMD ["python", "application/transformation/start_transformation.py"]