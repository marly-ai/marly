from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from application.pipeline.routes.pipeline_routes import api_router as pipeline_api_router

app = FastAPI(
    title="Marly API",
    description="The Unstructured Data Processor for Agents",
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

app.include_router(pipeline_api_router, prefix="", tags=["Pipeline Execution"])
