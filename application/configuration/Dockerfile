FROM python:3.9-slim

WORKDIR /app

# Copy the common directory
COPY ./common /app/common

# Copy the application directory
COPY ./application /app/application

# Copy requirements.txt and install dependencies
COPY ./application/configuration/requirements.txt /app/application/configuration/requirements.txt
RUN pip install --no-cache-dir -r /app/application/configuration/requirements.txt

ENV PYTHONPATH /app

EXPOSE 8001

CMD ["uvicorn", "application.configuration.start_config:app", "--host", "0.0.0.0", "--port", "8001"]
