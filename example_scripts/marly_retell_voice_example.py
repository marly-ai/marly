import sys
import os

# Add the parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import json
import logging
import time
from dotenv import load_dotenv
import os
from marly import Marly
from common.models.azure_model import AzureModel

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "http://localhost:8100"

WEBSITE_URL = "https://lu.ma/ke0rwi8n"
DEVPOST_URL = "https://vertical-specific-ai-agents.devpost.com/"

EXAMPLE_SCHEMA = {
    "Names": "The first name and last name of the company founders",
    "Company Name": "Name of the Company",
    "Round": "The round of funding",
    "Round Size": "How much money has the company raised",
    "Investors": "The names of the investors in the companies (names of investors and firms)",
    "Company Valuation": "The current valuation of the company",
    "Summary": "Three sentence summary of the company"
}

# Initialize the Azure model
azure_model = AzureModel(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    model_name=os.getenv("AZURE_MODEL_NAME"),
    additional_params={
        "api_version": os.getenv("AZURE_API_VERSION"),
        "azure_endpoint": os.getenv("AZURE_ENDPOINT"),
        "azure_deployment": os.getenv("AZURE_DEPLOYMENT_ID")
    }
)

def process_data(schema):
    client = Marly(base_url=BASE_URL)

    try:
        pipeline_response = client.pipelines.create(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            provider_model_name=os.getenv("AZURE_MODEL_NAME"),
            provider_type="azure",
            workloads=[{
                "data_source": "web",
                "documents_location": WEBSITE_URL,
                "schemas": [json.dumps(schema)],
            },
            {
                "data_source": "web",
                "documents_location": DEVPOST_URL,
                "schemas": [json.dumps(schema)],
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

def generate_json_schema(user_input: str) -> dict:
    prompt = f"""
    Based on the following user input, generate a JSON schema that represents the structure of the data:

    User Input: {user_input}
    EXAMPLE_FORMAT: {EXAMPLE_SCHEMA}

    TASK: Generate a JSON schema based on the EXAMPLE_FORMAT
    
    Respond with only the JSON schema, nothing else.
    """

    messages = [
        {"role": "system", "content": "You are a helpful AI assistant that generates JSON schemas based on user input."},
        {"role": "user", "content": prompt}
    ]

    response = azure_model.do_completion(messages, response_format={"type": "json_object"})
    
    try:
        schema = json.loads(response)
        return schema
    except json.JSONDecodeError:
        print("Error: The model did not return a valid JSON schema.")
        return None

if __name__ == "__main__":
    user_input = "Tell me about the hackathon sponsors"
    schema = generate_json_schema(user_input)

    if schema:
        #print(schema)
        process_data(schema)
    else:
        print("Failed to generate a valid JSON schema.")
