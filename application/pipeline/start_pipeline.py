import sys
import os

# Add the parent directory of 'application' to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from application.pipeline.routes.pipeline_routes import api_router as pipeline_api_router
import uvicorn

app = FastAPI(
    title="Marly API",
    description="The Data Processor for Agents",
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

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8100)
