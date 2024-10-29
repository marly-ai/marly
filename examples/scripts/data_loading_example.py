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
    raw_data = json.dumps({
        "Firm": "Evergreen Capital Partners",
        "Number of Funds": 7,
        "Commitment": 150.5,
        "Percent of Total Comm": 12.3,
        "Exposure (FMV + Unfunded)": 175.2,
        "Percent of Total Exposure": 14.8,
        "TVPI": 1.45,
        "Net IRR": 18.7,
        "Founded Year": 2005,
        "Headquarters": "New York, NY",
        "AUM": 3500.0,
        "Investment Strategy": "Growth Equity",
        "Target Industries": ["Technology", "Healthcare", "Consumer"],
        "Managing Partners": ["John Smith", "Sarah Johnson"],
        "Last Fund Closing Date": "2022-09-15",
        "ESG Focus": True
    })

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
                    "raw_data": raw_data,
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
            time.sleep(5)

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
