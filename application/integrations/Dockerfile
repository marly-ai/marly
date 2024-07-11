FROM python:3.9-slim

WORKDIR /app

COPY ../common /app/common
COPY ./application/integrations /app/application/integrations
COPY ./application/integrations/requirements.txt /app/application/integrations/requirements.txt

RUN pip install --no-cache-dir -r /app/application/integrations/requirements.txt

ENV PYTHONPATH /app

EXPOSE 8000

CMD ["uvicorn", "application.integrations.start_integrations:app", "--host", "0.0.0.0", "--port", "8000"]

