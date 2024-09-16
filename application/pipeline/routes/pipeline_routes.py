from fastapi import APIRouter, HTTPException
from application.pipeline.models.models import PipelineRequestModel, PipelineResponseModel, PipelineResult, JobStatus
from application.pipeline.service.pipeline_service import run_pipeline
from common.redis.redis_config import redis_client
import json
import logging

api_router = APIRouter()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@api_router.post("/pipelines", summary="Run pipeline")
async def run_pipeline_route(pipeline_request: PipelineRequestModel):
    try:
        response = await run_pipeline(pipeline_request)
        if isinstance(response, dict) and "error" in response:
            raise HTTPException(status_code=500, detail=response["error"])
        return {"task_id": response["task_id"], "message": "Pipeline processing started"}
    except Exception as e:
        logger.error(f"Error starting pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/pipelines/{task_id}", summary="Get pipeline results", response_model=PipelineResult)
async def get_pipeline_results(task_id: str):
    try:
        entries = await redis_client.xrange(f"job-status:{task_id}")
        if not entries:
            return PipelineResult(
                task_id=task_id,
                status=JobStatus.PENDING,
                results=[],
                total_run_time="N/A"
            )

        results = []
        total_run_time = "N/A"
        status = JobStatus.PENDING

        for _, entry in entries:
            if b'result' in entry:
                result_data = entry[b'result']
                if result_data:
                    try:
                        result = json.loads(result_data)
                        if 'results' in result:
                            results.extend(result['results'])
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode JSON result: {e}")
                        continue
            if b'total_run_time' in entry:
                total_run_time = entry[b'total_run_time']
            if b'status' in entry:
                try:
                    status = JobStatus(json.loads(entry[b'status']))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON status: {e}")
                    continue

        return PipelineResult(
            task_id=task_id,
            status=status,
            results=results,
            total_run_time=total_run_time
        )
    except Exception as e:
        logger.error(f"Error retrieving pipeline results: {e}")
        raise HTTPException(status_code=500, detail=str(e))
