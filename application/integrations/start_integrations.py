from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from application.integrations.destinations.routes.destination_routes import router as destination_router
from application.integrations.sources.routes.source_routes import api_router as source_router

app = FastAPI(
    title="Integrations API",
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

app.include_router(destination_router, prefix="/integrations/destinations", tags=["Destination Management"])
app.include_router(source_router, prefix="/integrations/sources", tags=["Source Management"])
