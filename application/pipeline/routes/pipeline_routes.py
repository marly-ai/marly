from fastapi import APIRouter, HTTPException, status
from application.pipeline.models.models import PipelineRequestModel, PipelineResponseModel, PipelineResult
from application.pipeline.service.pipeline_service import run_pipeline, get_pipeline_results
from fastapi.responses import JSONResponse
import logging

api_router = APIRouter()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@api_router.post("/pipelines", 
                 response_model=PipelineResponseModel, 
                 status_code=status.HTTP_202_ACCEPTED,
                 summary="Run pipeline",
                 description="Initiates a pipeline processing job for the given PDF and schemas.",
                 responses={
                     202: {"description": "Pipeline processing started"},
                     500: {"description": "Internal server error"}
                 })
async def run_pipeline_route(pipeline_request: PipelineRequestModel):
    try:
        response = await run_pipeline(pipeline_request)
        if isinstance(response, dict) and "error" in response:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=response["error"])
        return JSONResponse(content={"task_id": response["task_id"], "message": "Pipeline processing started"}, status_code=status.HTTP_202_ACCEPTED)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@api_router.get("/pipelines/{task_id}", 
                response_model=PipelineResult,
                summary="Get pipeline results",
                description="Retrieves the results of a pipeline processing job.",
                responses={
                    200: {"description": "Pipeline results retrieved successfully"},
                    404: {"description": "Task not found"},
                    500: {"description": "Internal server error"}
                })
async def get_pipeline_results_route(task_id: str):
    try:
        results = await get_pipeline_results(task_id)
        if isinstance(results, tuple) and results[1] == 500:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=results[0]['error'])
        if not results:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
        return JSONResponse(content=results, status_code=status.HTTP_200_OK)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error in get_pipeline_results_route: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred")
