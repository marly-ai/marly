from marly import Marly
from dotenv import load_dotenv
import os 

load_dotenv()

""" Search for a structured schema in a local PDF file and move it to a PostgreSQL database.
    This Example is based on the lacers.pdf example in the example_files folder """

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
    api_key=os.getenv("OPENAI_API_KEY"),
    created_by="test-user",
    model_name="gpt-4",
    model_type="openai",
)
my_model = model_config.id
print(f"Model ID: {my_model}")

# Add prompt configurations (prompt_ids must come from a Marly supported observability provider)
prompt_configs = [
    {
        "messages": [{"role": "system", "content": "You are an AI assistant specialized in extracting fund performance data."}],
        "name": "FILE_EXTRACTION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": os.getenv("PP_FILE_EXTRACTION_PROMPT_ID"),
        "created_by": "test-user"
    },
    {
        "messages": [{"role": "system", "content": "You are an AI assistant specialized in generating examples of fund performance data."}],
        "name": "EXAMPLE_GENERATION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": os.getenv("PP_EXAMPLE_GENERATION_PROMPT_ID"),
        "created_by": "test-user"
    },
    {
        "messages": [{"role": "system", "content": "You are an AI assistant specialized in extracting fund performance information."}],
        "name": "EXTRACTION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": os.getenv("PP_EXTRACTION_PROMPT_ID"),
        "created_by": "test-user"
    },
    {
        "messages": [{"role": "system", "content": "You are an AI assistant specialized in transforming fund performance data."}],
        "name": "TRANSFORMATION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": os.getenv("PP_TRANSFORMATION_PROMPT_ID"),
        "created_by": "test-user"
    },
    {
        "messages": [{"role": "system", "content": "You are an AI assistant specialized in validating fund performance data."}],
        "name": "VALIDATION",
        "variables": {"first_value": "", "second_value": ""},
        "prompt_id": os.getenv("PP_VALIDATION_PROMPT_ID"),
        "created_by": "test-user"
    },
    {
        "messages": [{"role": "system", "content": "You are an AI assistant specialized in looking at images."}],
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