import json
import logging
import time
from dotenv import load_dotenv
import os
from marly import Marly
import zlib
import base64

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PDF_FILE_PATH = "../example_files/lacers_reduced.pdf"
PDF_FILE_PATH2 = "../example_files/attention_paper.pdf"
WEBSITE_URL = "https://techcrunch.com/2024/08/09/anysphere-a-github-copilot-rival-has-raised-60m-series-a-at-400m-valuation-from-a16z-thrive-sources-say/"
BASE_URL = "http://localhost:8100"
schema_amount = 3

def read_and_encode_pdf(file_path):
    with open(file_path, "rb") as file:
        pdf_content = base64.b64encode(zlib.compress(file.read())).decode('utf-8')
    logging.debug(f"{file_path} read and encoded")
    return pdf_content


def poll_pipeline_results(client, pipeline_response_model, expected_schema_count=3):
    logging.info(f"Task ID: {pipeline_response_model.task_id}")

    max_attempts = 60  # Increased to allow for longer processing time
    attempt = 0
    all_results = []

    while attempt < max_attempts:
        time.sleep(20)

        results = client.pipelines.retrieve(pipeline_response_model.task_id)
        logging.debug(f"Poll attempt {attempt + 1}: Status - {results.status}")

        if results.status == 'COMPLETED':
            logging.debug(f"Pipeline completed with results: {results.results}")
            all_results.extend(results.results)
            
            # Check if we have received all expected schemas
            if len(all_results) >= expected_schema_count:
                logging.info(f"All expected schemas received. Total schemas: {len(all_results)}")
                return all_results
            else:
                logging.info(f"Received {len(all_results)} schemas so far. Continuing to poll...")
        
        elif results.status == 'FAILED':
            logging.error(f"Error: {results.error_message}")
            return None
        
        elif results.status == 'PROCESSING':
            logging.info("Pipeline still processing. Continuing to poll...")
        
        else:
            logging.warning(f"Unexpected status: {results.status}")

        attempt += 1

    logging.warning("Timeout: Pipeline execution took too long.")
    return all_results if all_results else None

def process_pdf(pdf_file1, pdf_file2):
    pdf_content = read_and_encode_pdf(pdf_file1)
    pdf_content2 = read_and_encode_pdf(pdf_file2)
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

    schema_2 = {
        "Author": "The authors first name and last name",
        "Institution": "The company, school, or institution most closely related to the author",
        "Email": "The authors email address"
    }

    SCHEMA_WEBSITE = {
        "Names": "The first name and last name of the company founders",
        "Company Name": "Name of the Company",
        "Round": "The round of funding",
        "Round Size": "How much money has the company raised",
        "Investors": "The names of the investors in the companies (names of investors and firms)",
        "Company Valuation": "The current valuation of the company",
        "Summary": "Three sentence summary of the company"
    }

    client = Marly(base_url=BASE_URL)

    try:
        pipeline_response_model = client.pipelines.create(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            provider_model_name=os.getenv("AZURE_MODEL_NAME"),
            provider_type="azure",
            workloads= [
                {
                    "pdf_stream": pdf_content,
                    "schemas": [json.dumps(schema_1)]
                },
                {
                    "pdf_stream": pdf_content2,
                    "schemas": [json.dumps(schema_2)]
                },
                {
                    "data_source": "web",
                    "documents_location": WEBSITE_URL,
                    "schemas": [json.dumps(SCHEMA_WEBSITE)],
                },
            ],
            additional_params={
                "azure_endpoint": os.getenv("AZURE_ENDPOINT"),
                "azure_deployment": os.getenv("AZURE_DEPLOYMENT_ID"),
                "api_version": os.getenv("AZURE_API_VERSION")
            }
        )
        
        # Call the polling function
        final_results = poll_pipeline_results(client, pipeline_response_model, schema_amount)
    
        if final_results:
            logging.info(f"Final results: {final_results}")
            return final_results
        else:
            logging.error("Failed to retrieve complete results")
            return None
        
        # logging.debug(f"Task ID: {pipeline_response_model.task_id}")

        # max_attempts = 20
        # attempt = 0
        # while attempt < max_attempts:
        #     time.sleep(20)

        #     results = client.pipelines.retrieve(pipeline_response_model.task_id)
        #     logging.debug(f"Poll attempt {attempt + 1}: Status - {results.status}")

        #     if results.status == 'COMPLETED':
        #         logging.debug(f"Pipeline completed with results: {results.results}")
        #         return results.results
        #     elif results.status == 'FAILED':
        #         logging.error(f"Error: {results.error_message}")
        #         return None

        #     attempt += 1

        # logging.warning("Timeout: Pipeline execution took too long.")
        # return None

    except Exception as e:
        logging.error(f"Error in pipeline process: {e}")
        return None
    

    
    
def process_results(results):
    processed_data = []
    
    for result in results:
        for item in result.results:
            result_data = {
                "schema_id": item.get("schema_id"),
                "metrics": None,
                "schema_data": None,
                "task_id": result.task_id,
                "pdf_key": result.pdf_key
            }

            # Extract metrics (this is a string containing JSON data, so we'll parse it)
            if "metrics" in item and item["metrics"]:
                metrics_json = item["metrics"].get(result_data["schema_id"])
                if metrics_json:
                    result_data["metrics"] = json.loads(metrics_json)

            # Extract schema_data
            if "schema_data" in item and item["schema_data"]:
                result_data["schema_data"] = item["schema_data"]

            processed_data.append(result_data)

    return processed_data

if __name__ == "__main__":
    result = process_pdf(PDF_FILE_PATH, PDF_FILE_PATH2)
    print(result)
    processed_results = process_results(result)
    # Parsing the relevant data from the result
    first_result = result[0]  # Extract the first item in the results array
    
    # Save to a JSON file
    with open("./output_ocr/extracted_data.json", "w") as json_file:
        json.dump(processed_results, json_file, indent=4)

    print("Data has been saved to extracted_data.json")