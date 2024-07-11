from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
import yaml

from application.configuration.routes.model_routes import api_router as model_router
from application.configuration.routes.schema_routes import api_router as schema_router
from application.configuration.routes.pipeline_routes import api_router as pipeline_router
from application.configuration.routes.output_mapping_config_routes import api_router as output_mapping_router
from application.configuration.routes.normalization_routes import api_router as normalization_router
from application.configuration.routes.prompt_routes import api_router as prompt_router
from application.orchestration.routes.pipeline_routes import api_router as orchestration_pipeline_router
from application.integrations.sources.routes.source_routes import api_router as source_router
from application.integrations.destinations.routes.destination_routes import router as destination_router

app = FastAPI(title="Marly API", version="1.0.0")

app.include_router(model_router, prefix="/configuration/models", tags=["Model Management"])
app.include_router(schema_router, prefix="/configuration/schemas", tags=["Schema Management"])
app.include_router(pipeline_router, prefix="/configuration/pipelines", tags=["Pipeline Management"])
app.include_router(output_mapping_router, prefix="/configuration/output-mappings", tags=["Output Mapping Management"])
app.include_router(normalization_router, prefix="/configuration/normalizations", tags=["Normalization Management"])
app.include_router(prompt_router, prefix="/configuration/prompts", tags=["Prompt Management"])
app.include_router(orchestration_pipeline_router, prefix="/orchestration/pipelines", tags=["Pipeline Execution"])
app.include_router(source_router, prefix="/integrations/sources", tags=["Source Management"])
app.include_router(destination_router, prefix="/integrations/destinations", tags=["Destination Management"])

def generate_openapi_spec():
    openapi_schema = get_openapi(
        title="Marly API",
        version="1.0.0",
        description="API for Marly data integration platform",
        routes=app.routes,
    )
    
    with open("marly_api_spec.yaml", "w") as f:
        yaml.dump(openapi_schema, f, sort_keys=False)

if __name__ == "__main__":
    generate_openapi_spec()
    print("OpenAPI specification has been generated and saved to 'marly_api_spec.yaml'")
