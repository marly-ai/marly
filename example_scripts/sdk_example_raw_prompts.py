from marly import Marly
from dotenv import load_dotenv
import os 

load_dotenv()

""" Search for a structured schema in a local PDF file and move it to a PostgreSQL database.
    This Example is based on the lacers.pdf example in the example_files folder """

""" NOTE: This example will be highly unstable because we are hardcoding prompts. Some extra prompt engineering/formatting maybe required."""

client = Marly(base_url="http://localhost:8100")

# Register Local FS source (Docker will copy /app/example_files into the container env)
local_fs_source_config = client.integrations.sources.register_local_fs(
    base_path="/app/example_files"
)
print(f"Local FS Source registered: {local_fs_source_config}")

# Register PostgreSQL destination
""" NOTE: You will need to configure postgres to accept connections from the containerized environment """
postgres_destination_config = client.integrations.destinations.register_postgres(
    host=str(os.getenv("POSTGRES_HOST", "host.docker.internal")),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    database=str(os.getenv("POSTGRES_DB", "postgres")),
    user=str(os.getenv("POSTGRES_USER", "postgres")),
    password=str(os.getenv("POSTGRES_PASSWORD", "postgres")),
    schema=str(os.getenv("POSTGRES_SCHEMA", "public"))
)
print(f"PostgreSQL Destination registered: {postgres_destination_config}")

# Add a schema
schema_config = client.configuration.schemas.create(
    keywords={
        "Firm": "The name of the firm",
        "Number of Funds": "The number of funds managed by the firm",
        "Commitment": "The commitment amount in millions of dollars",
        "Percent of Total Comm": "The percentage of total commitment",
        "Exposure (FMV + Unfunded)": "The exposure including fair market value and unfunded commitments in millions of dollars",
        "Percent of Total Exposure": "The percentage of total exposure",
        "TVPI": "Total Value to Paid-In multiple",
        "Net IRR": "Net Internal Rate of Return as a percentage"
    },
    created_by="test-user",
)
my_schema = schema_config.id
print(f"Schema ID: {my_schema}")

# Update output mapping to match the new schema
output_mapping_config = client.configuration.output_mappings.create(
    output_data_type="postgres",
    created_by="user1",
    column_locations={
        "public.fund_performance": {
            "firm": "char",
            "number_of_funds": "char",
            "commitment": "char",
            "percent_of_total_commitment": "char",
            "exposure": "char",
            "percent_of_total_exposure": "char",
            "tvpi": "char",
            "net_irr": "char"
        }
    },
    data_locations=["public.fund_performance"]
)
my_output_mapping = output_mapping_config.id
print(f"Output Mapping ID: {my_output_mapping}")

# Add a model
model_config = client.configuration.models.create(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    model_name=os.getenv("MODEL"),
    model_type="azure",
    additional_params={
        "api_version": os.getenv("OPENAI_API_VERSION"),
        "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "azure_deployment": os.getenv("ENGINE")
    },
    created_by="test-user",
)
my_model = model_config.id
print(f"Model ID: {my_model}")

# no prompt ids needed if you arent using observability
prompt_configs = [
    {
        "messages": [
            {"role": "user", "content": "list_of_pdfs: {first_value}\n\ndescription: {second_value}\n\nGiven the description of the pdf and list_of_pdfs, return only the exact filename matching to the description of the pdf. You must respond with only the EXACT filename and nothing else. Do not include any special characters or spaces in your response. example response: test123.pdf"}
        ],
        "name": "FILE_EXTRACTION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": "",
        "created_by": "test-user"
    },
    {
        "messages": [
            {"role": "user", "content": "Metrics: {first_value}\n\nGiven the set of 'Metrics' generate a list of 3 example values. Do not change the name of the metric just think of example values.\n\nExample Response:\n\n1. Metric 1: Example of Metric 1\nMetric 2: Example of Metric 2\nMetric 3: Example of Metric 3\nMetric 4: Example of Metric 4.\n\n2. Metric 1: Example of Metric 1\nMetric 2: Example of Metric 2\nMetric 3: Example of Metric 3\nMetric 4: Example of Metric 4.\n\n3. Metric 1: Example of Metric 1\nMetric 2: Example of Metric 2\nMetric 3: Example of Metric 3\nMetric 4: Example of Metric 4."}
        ],
        "name": "EXAMPLE_GENERATION",
        "variables": {"first_value": ""},
        "prompt_id": "",
        "created_by": "test-user"
    },
    {
        "messages": [
            {"role": "user", "content": "content: {first_value}\n\nmetrics_list: {second_value}\n\nInstructions: Given 'content', find all the metrics in 'metrics_list' for each item in 'metrics_list', organize them and correctly format them while following the 'Rules'. Refer to 'Example Response Format' for exactly how your response should be formatted.\n\nRules:\n\n1. Do not include any extra text or explanations in your response.\n2. If you can't find the value for a metric or are not sure what the metric value is put '-'\n3. Respond with text only, no code.\n4. Format the response clearly, with each metric and its value on a new line.\n5. Ignore any metrics found not listed in 'metrics_list'\n\nExample Response Format:\n\n{third_value}\n\nTask: Your response must only contain a list of each metric in 'metrics_list' for each portfolio company in 'content'. Please do not include any extra text in your response besides the list of items from 'metrics_list' and their individual metrics."}
        ],
        "name": "EXTRACTION",
        "variables": {"first_value": "", "second_value": "", "third_value": ""},
        "prompt_id": "",
        "created_by": "test-user"
    },
    {
        "messages": [
            {"role": "user", "content": "Metrics: {first_value}\n\ncolumn_names: {second_value}\n\nGiven the metrics and the column_names, structure the metrics into a JSON object. The keys of the JSON object should be derived from the column_names, and the values should be the corresponding values from the metrics. If a value is not found, use null.\n\nRules:\n1. Include all keys from column_names in the JSON object.\n2. No extra text in your response besides the structured JSON.\n\nTask: Respond only with the structured JSON based on the given metrics and column_names. Do not mark the json with 'json' or include any text besides the actual json object in your response"}
        ],
        "name": "TRANSFORMATION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": "",
        "created_by": "test-user"
    },
    {
        "messages": [
            {
                "role": "user",
                "content": "Metrics_List: {first_value}\n\nExample Response: {second_value}\n\nInstructions:\nYou are provided with two key pieces of information:\n\n1. Metrics_List: A set of key-value pairs representing a schema extracted from a document. This list may contain repetitive information about a given type of metric.\n\n2. Example Response: The format you must follow for the output.\n\nTask:\nYour task is to consolidate the information found in Metrics_List into a unified format as demonstrated in the Example Response. This involves merging repetitive entries and filling in any missing information based on other entries. Do not provide code do the actual work!\n\nProblem Statement:\nGiven a list of metric objects in Metrics_List, consolidate all information so that each unique metric contains all available data. The Example Response demonstrates the desired output format after consolidation.\n\nExample Input:\n\n1. Name: Bob\nAge: -\nLocation: China\nJob: -\n\n2. Name: Bob\nAge: -\nLocation: China\nJob: Data Engineer\n\n3. Name: Bob\nAge: 79\nLocation: -\nJob: -\n\nExample Output:\nName: Bob\nAge: 79\nLocation: China\nJob: Data Engineer\n\nDeliverable\nReturn a numbered list of all metric objects merged with all the information consolidated, in a numbered list."
            }
        ],
        "name": "VALIDATION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": "",
        "created_by": "test-user"
    },
    {
        "messages": [
            {"role": "system", "content": "You are an AI that processes images to find information."},
            {"role": "user", "content": "Given an image and a list of search terms: {first_value}, respond with 'Yes' if one or more of the search terms are present and have associated values in the page content, otherwise respond with 'No'. The image is provided as a base64 encoded string: {second_value}"}
        ],
        "name": "PAGE_FINDER",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": "",
        "created_by": "test-user"
    },
]

prompt_ids = []

for config in prompt_configs:
    prompt_config = client.configuration.prompts.create(**config)
    prompt_ids.append(prompt_config.id)

print(f"Prompt IDs: {prompt_ids}")

# Create a pipeline
pipeline_config = client.configuration.pipelines.create(
    name="Local FS to PostgreSQL Pipeline",
    run_type="batch",
    pipeline_schema_id=my_schema,
    created_by="test-user",
    integrations={
        "input_data_source": "local_fs",
        "output_data_source": "postgres"
    },
    model_id=my_model,
    prompt_ids={
        "FILE_EXTRACTION": prompt_ids[0],
        "EXAMPLE_GENERATION": prompt_ids[1],
        "EXTRACTION": prompt_ids[2],
        "TRANSFORMATION": prompt_ids[3],
        "VALIDATION": prompt_ids[4],
        "PAGE_FINDER": prompt_ids[5]
    },
    # dont need one if theres no intermediary structured doc that needs to be loaded elsewhere
    normalization_id=""
)
my_pipeline = pipeline_config.id
print(f"Pipeline ID: {my_pipeline}")

# Run the pipeline
pipeline_request = {
    "filenames": ["lacers"],
    "pipeline_id": my_pipeline,
    "output_mapping_ids": [my_output_mapping]
}

# Execute the pipeline
result = client.orchestrations.pipelines.run_pipeline(**pipeline_request)
print(f"Pipeline execution result: {result}")