import asyncio
import json
import logging
from typing import Optional, Dict, Any, Union
from redis.asyncio import Redis
from redis.exceptions import RedisError
from datetime import datetime
from application.transformation.models.models import (
    TransformationRequestModel,
    TransformationResponseModel,
    SchemaResult,
    JobStatus,
    TransformationOnlyRequestModel
)
from application.transformation.service.transformation_handler import run_transformation, run_transformation_only
from common.redis.redis_config import get_redis_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_transformations() -> None:
    logger.info("Starting transformation worker")
    redis = await get_redis_connection()
    last_id_transformation = "0-0"
    last_id_transformation_only = "0-0"
    
    task_workloads = {}

    while True:
        try:
            result = await redis.xread(
                streams={
                    "transformation-stream": last_id_transformation,
                    "transformation-only-stream": last_id_transformation_only
                },
                count=1,
                block=0
            )
            
            for stream_name, messages in result:
                for message_id, message in messages:
                    logger.info(f"Received message from stream {stream_name}: ID {message_id}")
                    payload = message.get(b"payload")
                    if payload:
                        try:
                            payload_dict = json.loads(payload.decode('utf-8'))
                            
                            if stream_name == b"transformation-stream":
                                request = TransformationRequestModel(**payload_dict)
                            else:
                                request = TransformationOnlyRequestModel(**payload_dict)
                            
                            if request.task_id not in task_workloads:
                                total_workloads = await get_total_workloads(redis, request.task_id)
                                task_workloads[request.task_id] = {
                                    'total': total_workloads,
                                    'completed': 0
                                }
                            
                            existing_results = await get_existing_results(redis, request.task_id)
                            transformation_result = await process_transformation(request)
                            merged_results = merge_results(existing_results, transformation_result)
                            
                            serialized_result = json.dumps(merged_results.dict())
                            await redis.xadd(f"results-stream:{request.task_id}", {"payload": serialized_result})
                            
                            task_workloads[request.task_id]['completed'] += 1
                            
                            if task_workloads[request.task_id]['completed'] == task_workloads[request.task_id]['total']:
                                logger.info(f"All workloads completed for task {request.task_id}")
                                await update_job_status(redis, request.task_id, JobStatus.COMPLETED, serialized_result)
                                del task_workloads[request.task_id]  # Cleanup
                            else:
                                await update_job_status(redis, request.task_id, JobStatus.IN_PROGRESS, None)
                            
                        except Exception as e:
                            logger.error(f"Error processing transformation task: {e}")
                            await update_job_status(redis, request.task_id, JobStatus.FAILED, str(e))
                            if request.task_id in task_workloads:
                                del task_workloads[request.task_id]
                    
                    if stream_name == b"transformation-stream":
                        last_id_transformation = message_id
                    elif stream_name == b"transformation-only-stream":
                        last_id_transformation_only = message_id

        except RedisError as e:
            logger.error(f"Error reading from Redis stream: {e}")
            await asyncio.sleep(1)

async def process_transformation(
    transformation_request: Union[TransformationRequestModel, TransformationOnlyRequestModel]
) -> TransformationResponseModel:
    redis = await get_redis_connection()
    try:
        transformed_results = []

        if isinstance(transformation_request, TransformationOnlyRequestModel):
            transformed_metrics = await run_transformation_only(
                task_id=transformation_request.task_id,
                data_location_key=transformation_request.data_location_key,
                schemas=transformation_request.schemas,
                destination=transformation_request.destination,
                raw_data=transformation_request.raw_data
            )
            if isinstance(transformed_metrics, str):
                transformed_metrics = json.loads(transformed_metrics)
            
            transformed_results.append(SchemaResult(
                schema_id=f"{transformation_request.task_id}-transformation_only",
                schema_data={}, 
                metrics=transformed_metrics
            ))
        else:
            for schema_result in transformation_request.results:
                transformed_metrics = await run_transformation(
                    metrics=schema_result.metrics,
                    schema=schema_result.schema_data,
                    source_type=transformation_request.source_type
                )
                if isinstance(transformed_metrics, str):
                    transformed_metrics = json.loads(transformed_metrics)
                
                transformed_results.append(SchemaResult(
                    schema_id=schema_result.schema_id,
                    schema_data=schema_result.schema_data,
                    metrics=transformed_metrics
                ))

        response = TransformationResponseModel(
            task_id=transformation_request.task_id,
            pdf_key=transformation_request.data_location_key if isinstance(transformation_request, TransformationOnlyRequestModel) else transformation_request.pdf_key,
            results=transformed_results
        )

        return response
    except Exception as e:
        logger.error(f"Error processing transformation task {transformation_request.task_id}: {e}")
        await update_job_status(redis, transformation_request.task_id, JobStatus.FAILED, str(e))
        raise

async def update_job_status(
    redis: Redis,
    task_id: str,
    status: JobStatus,
    result: Optional[str]
) -> None:
    fields: Dict[str, Any] = {"status": json.dumps(status.value)}
    if result:
        fields["result"] = result

    start_time_str = await redis.get(f"job-start-time:{task_id}")
    start_time = int(start_time_str) if start_time_str else 0
    current_time = int(datetime.utcnow().timestamp())
    total_run_time = current_time - start_time
    run_time_str = f"{total_run_time // 60} minutes" if total_run_time >= 60 else f"{total_run_time} seconds"
    fields["total_run_time"] = run_time_str

    await redis.xadd(f"job-status:{task_id}", fields)

async def get_existing_results(redis: Redis, task_id: str) -> Optional[TransformationResponseModel]:
    try:
        entries = await redis.xrevrange(f"results-stream:{task_id}", count=1)
        if entries:
            _, message = entries[0]
            payload = message.get(b"payload")
            if payload:
                return TransformationResponseModel(**json.loads(payload.decode('utf-8')))
    except Exception as e:
        logger.error(f"Error getting existing results: {e}")
    return None

def merge_results(existing: Optional[TransformationResponseModel], new: TransformationResponseModel) -> TransformationResponseModel:
    if not existing:
        return new
    
    return TransformationResponseModel(
        task_id=new.task_id,
        pdf_key=new.pdf_key,
        results=existing.results + new.results
    )

async def get_total_workloads(redis: Redis, task_id: str) -> int:
    """Get the total number of workloads for a task from Redis."""
    try:
        workload_count = await redis.get(f"workload-count:{task_id}")
        return int(workload_count) if workload_count else 1
    except Exception as e:
        logger.error(f"Error getting workload count: {e}")
        return 1

async def clear_transformation_streams() -> None:
    retries = 0
    max_retries = 5
    delay = 1

    while retries < max_retries:
        try:
            redis = await get_redis_connection()
            await redis.xtrim("transformation-stream", approximate=True, maxlen=0)
            await redis.xtrim("transformation-only-stream", approximate=True, maxlen=0)
            logger.info("Cleared transformation-stream and transformation-only-stream")
            return
        except RedisError as e:
            if "LOADING" in str(e):
                logger.info(f"Redis is still loading. Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
                retries += 1
                delay *= 2
            else:
                raise e

    raise Exception("Failed to clear transformation streams after maximum retries")
