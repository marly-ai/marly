from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from application.configuration.routes.pipeline_routes import api_router as pipeline_api_router
from application.configuration.routes.schema_routes import api_router as schema_api_router
from application.configuration.routes.model_routes import api_router as model_api_router
from application.configuration.routes.prompt_routes import api_router as prompt_api_router
from application.configuration.routes.output_mapping_config_routes import api_router as output_mapping_api_router
from application.configuration.routes.normalization_routes import api_router as normalization_api_router

app = FastAPI(
    title="Configuration API",
    docs_url='/docs',
    openapi_url='/openapi.json'
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

app.include_router(schema_api_router, prefix="/configuration/schemas", tags=["Schema Management"])
app.include_router(pipeline_api_router, prefix="/configuration/pipelines", tags=["Pipeline Management"])
app.include_router(model_api_router, prefix="/configuration/models", tags=["Model Management"])
app.include_router(prompt_api_router, prefix="/configuration/prompts", tags=["Prompt Management"])
app.include_router(output_mapping_api_router, prefix="/configuration/output-mappings", tags=["Output Mapping Management"])
app.include_router(normalization_api_router, prefix="/configuration/normalizations", tags=["Normalization Management"])
