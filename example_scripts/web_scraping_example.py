import json
import logging
import time
from dotenv import load_dotenv
import os
from marly import Marly

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "http://0.0.0.0:8100" # TODO: change this back

website_url = "https://techcrunch.com/2024/08/28/san-franciscos-ai-hackathons-agency-lets-you-see-what-your-ai-agents-do/"

def process_pdf():
    # schema_1 = {
    #     "Title" : "Name of the Paper or Article",
    #     "Summary" : "Description of the paper",
    #     "Paper Link" : "Hyperlink to the paper",
    #     "Tweet Link" : "Hyperlink to the tweet"
    # }

    schema_2 = {
        "Names" : "Names of the startup founders",
        "Company Name" : "Name of the Company",
        "Round Size" : "How much money has the company raised",
        "Investors" : "List of investors in the company",
        "Summary" : "Three sentence summary of the company"
    }

    client = Marly(base_url=BASE_URL)

    try:
        pipeline_response_model = client.pipelines.create(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            provider_model_name=os.getenv("AZURE_MODEL_NAME"),
            provider_type="azure",
            workloads=[
                {
                    "data_source": "web",
                    "documents_location": website_url,
                    "schemas": [json.dumps(schema_2)],
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
    #
    except Exception as e:
        logging.error(f"Error in pipeline process: {e}")
        return None
    return None

if __name__ == "__main__":
    results = process_pdf()
    print(results)