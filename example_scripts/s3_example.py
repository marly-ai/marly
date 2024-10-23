import json
import logging
import os
from dotenv import load_dotenv
from marly import Marly
import time
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = os.getenv("S3_FILE_KEY")
BASE_URL = "http://localhost:8100"

SCHEMA_1 = {
    "Firm": "The name of the firm",
    "Number of Funds": "The number of funds managed by the firm",
    "Commitment": "The commitment amount in millions of dollars",
    "Percent of Total Comm": "The percentage of total commitment",
    "Exposure (FMV + Unfunded)": "The exposure including fair market value and unfunded commitments in millions of dollars",
    "Percent of Total Exposure": "The percentage of total exposure",
    "TVPI": "Total Value to Paid-In multiple",
    "Net IRR": "Net Internal Rate of Return as a percentage"
}

def process_pdf():
    client = Marly(base_url=BASE_URL)

    try:
        pipeline_response = client.pipelines.create(
            api_key=os.getenv("GROQ_API_KEY"),
            provider_model_name="llama-3.1-70b-versatile",
            provider_type="groq",
            workloads=[
                {
                    "file_name": S3_FILE_KEY,
                    "data_source": "s3",
                    "documents_location": S3_BUCKET_NAME,
                    "schemas": [json.dumps(SCHEMA_1)]
                }
            ],
            additional_params={
                "bucket_name": S3_BUCKET_NAME,
                "region_name": "us-east-1",
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
            }
        )
        logging.debug(f"Task ID: {pipeline_response.task_id}")

        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            time.sleep(10)

            results = client.pipelines.retrieve(pipeline_response.task_id)
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
