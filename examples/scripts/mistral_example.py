import json
import logging
import time
from dotenv import load_dotenv
import os
from marly import Marly

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "http://localhost:8100"

def process_pdf():
    schema_1 = {
        "Firm": "The name of the firm",
        "Number of Funds": "The number of funds managed by the firm",
        "Commitment": "The commitment amount in millions of dollars",
        "Percent of Total Comm": "The percentage of total commitment",
        "Exposure (FMV + Unfunded)": "The exposure including fair market value and unfunded commitments in millions of dollars",
        "Percent of Total Exposure": "The percentage of total exposure",
        "TVPI": "Total Value to Paid-In multiple",
        "Net IRR": "Net Internal Rate of Return as a percentage"
    }

    client = Marly(base_url=BASE_URL)

    try:
        pipeline_response_model = client.pipelines.create(
            api_key=os.getenv("MISTRAL_API_KEY"),
            provider_model_name="mistral-large-latest",
            provider_type="mistral",
            workloads=[
                {
                    "file_name": "lacers reduced",
                    "data_source": "local_fs",
                    "documents_location": "/app/example_files",
                    "schemas": [json.dumps(schema_1)],
                }
            ]
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

if __name__ == "__main__":
    results = process_pdf()
    print(results)