from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from application.orchestration.routes.pipeline_routes import api_router as pipeline_api_router

app = FastAPI(
    title="Orchestration API",
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

app.include_router(pipeline_api_router, prefix="/orchestration/pipelines", tags=["Pipeline Execution"])
