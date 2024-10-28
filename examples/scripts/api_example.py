import base64
import json
import zlib
import logging
import time
import requests

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

PDF_FILE = "<my_file_name>"
API_KEY = "<my_api_key>"
BASE_URL = "https://api.marly.ai";

def read_and_encode_pdf(file_path):
    with open(file_path, "rb") as file:
        pdf_content = base64.b64encode(zlib.compress(file.read())).decode('utf-8')
    logging.debug(f"{file_path} read and encoded")
    return pdf_content

def get_pipeline_results(task_id):
    logging.debug(f"Fetching results for task ID: {task_id}")
    response = requests.get(f"{BASE_URL}/pipelines/{task_id}", headers={"marly-api-key": API_KEY})
    return response.json()

def process_pdf(pdf_file):
    pdf_content = read_and_encode_pdf(pdf_file)

    schema_1 = {
        "Type of change": "Description of the type of change",
        "Location": "Location of the change",
        "Item": "Description of the item"
    }

    pipeline_request = {
        "license_key": "1234567890",
        "workloads": [
            {
                "pdf_stream": pdf_content,
                "schemas": [json.dumps(schema_1)]
            }
        ]
    }

    logging.debug("Sending POST request to pipeline endpoint")
    response = requests.post(f"{BASE_URL}/pipelines", json=pipeline_request, headers={"marly-api-key": API_KEY})

    logging.debug(f"Response status code: {response.status_code}")
    logging.debug(f"Response headers: {response.headers}")
    logging.debug(f"Response content: {response.text}")

    result = response.json()
    task_id = result.get("task_id")
    if not task_id:
        raise ValueError("Invalid task_id: task_id is None or empty")
    logging.debug(f"Task ID: {task_id}")

    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        time.sleep(35)

        results = get_pipeline_results(task_id)
        logging.debug(f"Poll attempt {attempt + 1}: Status - {results['status']}")

        if results['status'] == 'Completed':
            return results
        elif results['status'] == 'Failed':
            logging.error(f"Error: {results.get('error_message', 'Unknown error')}")
            return None

        attempt += 1

    logging.warning("Timeout: Pipeline execution took too long.")
    return None

def main():
    results = process_pdf(PDF_FILE)

    if results:
        print("Raw API Response:")
        print(json.dumps(results, indent=2))

        if 'results' in results and results['results']:
            for i, result in enumerate(results['results']):
                print(f"\nResult {i + 1}:")
                metrics = result.get('metrics', {})
                print("Metrics:")
                print(json.dumps(metrics, indent=2))

                schema_3 = metrics.get('schema_0', '{}')
                print("\nSchema 3:")
                print(schema_3)

                try:
                    schema_3_json = json.loads(schema_3)
                    print("\nSchema 3 (parsed):")
                    print(json.dumps(schema_3_json, indent=2))

                    for key, value in schema_3_json.items():
                        print(f"\n{key}:")
                        print(value)

                except json.JSONDecodeError:
                    print("\nFailed to parse schema_3 as JSON")
        else:
            print("No results found in the API response")
    else:
        print("Failed to process PDF. Please check the logs for more information.")

if __name__ == "__main__":
    main()
