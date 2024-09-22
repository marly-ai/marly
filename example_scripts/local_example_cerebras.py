import base64
import json
import zlib
import logging
import os
import time
from dotenv import load_dotenv
from marly import Marly

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PDF_FILE_PATH = "./lacers_reduced.pdf"

#Will return the schema results in markdown mode, else will return in JSON format
markdown_mode = True

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
    # TODO: Change back to 8100
    client = Marly(base_url="http://localhost:8100")

    try:
        pipeline_response_model = client.pipelines.create(
            api_key=os.getenv("CEREBRAS_API_KEY"),
            provider_model_name="llama3.1-70b",
            provider_type="cerebras",
            markdown_mode = markdown_mode,
            workloads=[
                {
                    "pdf_stream": pdf_content,
                    "schemas": [json.dumps(schema_1)],
                }
            ],
        )
        logging.debug(f"Task ID: {pipeline_response_model.task_id}")
    except Exception as e:
        logging.error(f"Error creating pipeline: {e}")
        return None

    # Quick check for cached results
    time.sleep(5)
    try:
        results = client.pipelines.retrieve(pipeline_response_model.task_id)
        if results.status == 'COMPLETED':
            parsed_results = [json.loads(results.results[0].metrics[f'schema_{i}']) for i in range(len(results.results[0].metrics))]
            logging.info(f"Cached results found and returned {parsed_results}")
            return json.dumps(parsed_results, indent=2)
    except Exception as e:
        logging.debug(f"No cached results available: {e}")

    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        logging.debug(f"Waiting for pipeline to complete. Attempt {attempt + 1} of {max_attempts}")
        time.sleep(30)

        try:
            results = client.pipelines.retrieve(pipeline_response_model.task_id)
            logging.debug(f"Poll attempt {attempt + 1}: Status - {results.status}")

            if results.status == 'COMPLETED':
                if markdown_mode:
                    parsed_results = [results.results[0].metrics[f'schema_{i}'] for i in range(len(results.results[0].metrics))]
                else:
                    parsed_results = [json.loads(results.results[0].metrics[f'schema_{i}']) for i in range(len(results.results[0].metrics))]

                logging.info(f"Results: {parsed_results}")
                return parsed_results  # No need to json.dumps() here if we're returning Markdown
            elif results.status == 'FAILED':
                logging.error(f"Error: {results.error_message or 'Unknown error'}")
                return None

        except Exception as e:
            logging.error(f"Error fetching pipeline results: {e}")
            return None

        attempt += 1

    logging.warning("Timeout: Pipeline execution took too long.")
    return None

if __name__ == "__main__":
    process_pdf(PDF_FILE_PATH)
