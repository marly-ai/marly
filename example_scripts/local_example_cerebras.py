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
    """
    Reads a PDF file, compresses its content, and encodes it in base64.

    Args:
        file_path (str): Path to the PDF file.

    Returns:
        str: Base64-encoded and zlib-compressed PDF content.
    """
    with open(file_path, "rb") as file:
        pdf_content = base64.b64encode(zlib.compress(file.read())).decode('utf-8')
    logging.debug(f"{file_path} read and encoded")
    return pdf_content

def process_pdf(pdf_file):
    """
    Submits the PDF to the Marly pipeline for processing.

    Args:
        pdf_file (str): Path to the PDF file.

    Returns:
        Optional[str]: JSON-formatted results if successful; otherwise, None.
    """
    pdf_content = read_and_encode_pdf(pdf_file)

    # Initialize the Marly client
    client = Marly(base_url=BASE_URL)

    # Define additional parameters for the workload
    additional_params = {
        "source_config": {
            "documents_location": "./documents",
            "data_source": "local",  
        }
    }

    workload = {
        "pdf_stream": pdf_content,
        "schemas": [json.dumps(SCHEMA_1)],
        "data_source": "local",
        "documents_location": "./documents",
        "file_name": os.path.basename(pdf_file),
        "additional_params": additional_params
    }

    try:
        # Create the pipeline task
        pipeline_response = client.pipelines.create(
            api_key=os.getenv("CEREBRAS_API_KEY"),
            provider_model_name="llama3.1-70b",
            provider_type="cerebras",
            workloads=[workload],
        )

        # Poll for the pipeline results
        while True:
            results = client.pipelines.retrieve(pipeline_response.task_id)
            if results.status == 'COMPLETED':
                return json.dumps([json.loads(results.results[0].metrics[f'schema_{i}']) for i in range(len(results.results[0].metrics))])
            elif results.status == 'FAILED':
                logging.error("Pipeline processing failed.")
                return None
            time.sleep(15)

    except Exception as e:
        logging.error(f"Error in pipeline process: {e}")
        return None

if __name__ == "__main__":
    result = process_pdf(PDF_FILE_PATH)
    if result:
        logging.info(f"Pipeline Results:\n{result}")
    else:
        logging.error("Failed to process the PDF.")