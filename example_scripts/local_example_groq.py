import base64
import json
import zlib
import logging
import requests
from dotenv import load_dotenv
import os
import time
load_dotenv()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "http://localhost:8100"
PDF_FILE_PATH = "./lacers_reduced.pdf"

def read_and_encode_pdf(file_path):
    with open(file_path, "rb") as file:
        pdf_content = base64.b64encode(zlib.compress(file.read())).decode('utf-8')
    logging.debug(f"{file_path} read and encoded")
    return pdf_content

def get_pipeline_results(task_id):
    logging.debug(f"Fetching results for task ID: {task_id}")
    response = requests.get(f"{BASE_URL}/pipelines/{task_id}")
    return response.json()

def process_pdf(pdf_file):
    pdf_content = read_and_encode_pdf(pdf_file)

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

    pipeline_request = {
        "workloads": [
            {
                "pdf_stream": pdf_content,
                "schemas": [json.dumps(schema_1)]
            }
        ],
        "provider_type": "groq",
        "provider_model_name": "llama-3.1-70b-versatile",
        "api_key": os.getenv("GROQ_API_KEY"),
        "additional_params": {}
    }

    logging.debug("Sending POST request to pipeline endpoint")
    try:
        response = requests.post(f"{BASE_URL}/pipelines", json=pipeline_request)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending request: {e}")
        return

    logging.debug(f"Response status code: {response.status_code}")
    logging.debug(f"Response headers: {response.headers}")
    logging.debug(f"Response content: {response.text}")

    try:
        result = response.json()
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON response")
        return

    task_id = result.get("task_id")
    if not task_id:
        logging.error("Invalid task_id: task_id is None or empty")
        return
    logging.debug(f"Task ID: {task_id}")
    
    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        logging.debug(f"Waiting for pipeline to complete. Attempt {attempt + 1} of {max_attempts}")
        time.sleep(30)

        results = get_pipeline_results(task_id)
        logging.debug(f"Poll attempt {attempt + 1}: Status - {results['status']}")

        if results['status'] == 'COMPLETED':
            logging.debug(f"Pipeline completed with results: {results['results']}")
            return results['results']
        elif results['status'] == 'FAILED':
            logging.error(f"Error: {results.get('error_message', 'Unknown error')}")
            return None

        attempt += 1

    logging.warning("Timeout: Pipeline execution took too long.")
    return None

if __name__ == "__main__":
    process_pdf(PDF_FILE_PATH)
