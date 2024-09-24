import base64
import json
import zlib
import logging
import os
from dotenv import load_dotenv
from marly import Marly
import time

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PDF_FILE_PATH = "./lacers_reduced.pdf"
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

def read_and_encode_pdf(file_path):
    with open(file_path, "rb") as file:
        pdf_content = base64.b64encode(zlib.compress(file.read())).decode('utf-8')
    logging.debug(f"{file_path} read and encoded")
    return pdf_content

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

    #running locally
    client = Marly(base_url="http://localhost:8100")

    try:
        pipeline_response = client.pipelines.create(
            api_key=os.getenv("CEREBRAS_API_KEY"),
            provider_model_name="llama3.1-70b",
            provider_type="cerebras",
            workloads=[{"pdf_stream": pdf_content, "schemas": [json.dumps(SCHEMA_1)]}],
        )

        while True:
            results = client.pipelines.retrieve(pipeline_response.task_id)
            if results.status == 'COMPLETED':
                return json.dumps([json.loads(results.results[0].metrics[f'schema_{i}']) for i in range(len(results.results[0].metrics))])
            elif results.status == 'FAILED':
                return None
            time.sleep(15)

    except Exception as e:
        logging.error(f"Error in pipeline process: {e}")
        return None

# Usage
if __name__ == "__main__":
    result = process_pdf(PDF_FILE_PATH)
    print(result)
