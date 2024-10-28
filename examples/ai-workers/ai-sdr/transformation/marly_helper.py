import json
import logging
import time
from dotenv import load_dotenv
import os
from marly import Marly

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "http://localhost:8100"

def process_data(raw_data):
    schema_1 = {
        "id": "Unique identifier for the profile for all contacts",
        "firstName": "First name of the person for all contacts",
        "lastName": "Last name of the person for all contacts",
        "headline": "Professional headline or tagline for all contacts",
        "location": "Geographic location of the person for all contacts",
        "summary": "Brief professional summary or bio for all contacts",
        "connectionsCount": "Number of LinkedIn connections for all contacts"
    }
 
    client = Marly(base_url=BASE_URL)

    try:
        pipeline_response_model = client.pipelines.create(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            provider_model_name=os.getenv("AZURE_MODEL_NAME"),
            provider_type="azure",
            workloads=[
                {
                    "destination": "sqlite",
                    "documents_location": "contacts_table",
                    "schemas": [json.dumps(schema_1)],
                    "raw_data": json.dumps(raw_data),
                }
            ],
            additional_params={
                "azure_endpoint": os.getenv("AZURE_ENDPOINT"),
                "azure_deployment": os.getenv("AZURE_DEPLOYMENT_ID"),
                "api_version": os.getenv("AZURE_API_VERSION")
            }
        )
        
        logging.debug(f"Task ID: {pipeline_response_model.task_id}")

        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            time.sleep(30)

            results = client.pipelines.retrieve(pipeline_response_model.task_id)
            logging.debug(f"Poll attempt {attempt + 1}: Status - {results.status}")

            if results.status == 'COMPLETED':
                logging.debug(f"Pipeline completed with results: {results.results}")
                return results.results
            elif results.status == 'FAILED':
                logging.error(f"Error: {results.error_message}")
                return None

            attempt += 1

        logging.warning("Timeout: Pipeline execution took too long.")
        return None

    except Exception as e:
        logging.error(f"Error in pipeline process: {e}")
        return None
