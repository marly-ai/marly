import json
import logging
import time
from dotenv import load_dotenv
import os
from marly import Marly

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "http://localhost:8100"
WEBSITE_URL = "https://www.forbes.com/sites/kenrickcai/2023/02/07/mindsdb-series-a-funding-ai-in-workplace/"
WEBSITE_URL2 = "https://techcrunch.com/2024/08/09/anysphere-a-github-copilot-rival-has-raised-60m-series-a-at-400m-valuation-from-a16z-thrive-sources-say/"

SCHEMA = {
    "Names": "The first name and last name of the company founders",
    "Company Name": "Name of the Company",
    "Round Size": "How much money has the company raised",
    "Investors": "The names of the investors in the companies (names of investors and firms)",
    "Summary": "Three sentence summary of the company"
}

def process_website():
    client = Marly(base_url=BASE_URL)

    try:
        pipeline_response = client.pipelines.create(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            provider_model_name=os.getenv("AZURE_MODEL_NAME"),
            provider_type="azure",
            workloads=[{
                "data_source": "web",
                "documents_location": WEBSITE_URL,
                "schemas": [json.dumps(SCHEMA)],
            },
            {
                "data_source": "web",
                "documents_location": WEBSITE_URL2,
                "schemas": [json.dumps(SCHEMA)],
            }
            ],
            additional_params={
                "azure_endpoint": os.getenv("AZURE_ENDPOINT"),
                "azure_deployment": os.getenv("AZURE_DEPLOYMENT_ID"),
                "api_version": os.getenv("AZURE_API_VERSION")
            }
        )

        while True:
            results = client.pipelines.retrieve(pipeline_response.task_id)
            #logging.info(f"Results: {results}")
            if results.status == 'COMPLETED':
                processed_results = []
                for result in results.results:
                    for schema_result in result.results:
                        processed_result = json.loads(schema_result['metrics']['schema_0'])
                        processed_results.append(processed_result)
                return json.dumps(processed_results, indent=2)
            elif results.status == 'FAILED':
                return None
            time.sleep(15)

    except Exception as e:
        logging.error(f"Error in pipeline process: {e}")
        return None

if __name__ == "__main__":
    result = process_website()
    print(result)