import json
import uuid
import base64
import zlib
import hashlib
from io import BytesIO
from typing import List, Dict
import redis.asyncio as redis
import logging
import time
import asyncio

from application.pipeline.models.models import PipelineRequestModel, PipelineResponseModel, JobStatus, PipelineResult, ExtractionRequestModel
from common.text_extraction.text_extractor import get_pdf_page_count
from common.models.model_factory import ModelFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_pipeline(customer_input: PipelineRequestModel):
    logger.info("Starting Pipeline Run...")

    try:
        con = await redis.from_url("redis://redis:6379/0", encoding="utf-8", decode_responses=True)
    except Exception as e:
        logger.error(f"Redis connection error: {e}")
        return {
            "error": "Service temporarily unavailable",
            "details": "Unable to connect to the database. Please try again later."
        }

    task_id = str(uuid.uuid4())
    start_time = int(time.time())
    await con.set(f"job-start-time:{task_id}", start_time)
    await con.xadd(
        f"job-status:{task_id}",
        {"status": json.dumps(JobStatus.PENDING.value), "start_time": str(start_time)}
    )
    try:
         ModelFactory.create_model(
            model_type=customer_input.provider_type,
            model_name=customer_input.provider_model_name,
            api_key=customer_input.api_key,
            additional_params=customer_input.additional_params
        )

    except ValueError as e:
        logger.error(f"Model creation error: {e}")
        return {
            "error": "Invalid model configuration",
            "details": str(e)
        }
    
    # publish details to be used by other workers
    model_details_json = json.dumps({
        "provider_type": customer_input.provider_type,
        "provider_model_name": customer_input.provider_model_name,
        "api_key": customer_input.api_key,
        "markdown_mode": customer_input.markdown_mode,
        "additional_params": customer_input.additional_params
    })
    await con.set("model-details", model_details_json)

    pdf_hash = hashlib.sha256(json.dumps([w.dict() for w in customer_input.workloads]).encode()).hexdigest()
    cache_key = f"cache:{pdf_hash}"
    cached_hash = await con.get(cache_key)
    if cached_hash:
        logger.info(f"Cache hit for key: {cache_key}")
        cached_response = await con.get(cached_hash)
        if cached_response:
            return json.loads(cached_response)

    async def process_workload(index, workload_combo):
        try:
            compressed_data = base64.b64decode(workload_combo.pdf_stream)
            decompressed_pdf = zlib.decompress(compressed_data)
            
            pdf_cursor = BytesIO(decompressed_pdf)
            logger.info(f"Decompressed PDF for workload {index}")
            try:
                page_count = get_pdf_page_count(pdf_cursor)
                logger.debug(f"Page count for workload {index}: {page_count}")
            except Exception as e:
                logger.error(f"Error getting page count: {e}")
                return 0

            pdf_key = f"pdf:{task_id}:{index}"
            await con.set(pdf_key, base64.b64encode(decompressed_pdf).decode())
            logger.info(f"PDF stored in Redis with key: {pdf_key} as base64 string")

            schemas = [json.loads(schema) for schema in workload_combo.schemas]

            task_payload = ExtractionRequestModel(
                task_id=task_id,
                pdf_key=pdf_key,
                schemas=schemas
            )

            await con.xadd("extraction-stream", {"payload": json.dumps(task_payload.dict())})

            return page_count
        except Exception as e:
            logger.error(f"Error processing workload: {e}")
            return 0

    # Process all workloads
    workload_results = await asyncio.gather(*[process_workload(index, workload_combo) 
                        for index, workload_combo in enumerate(customer_input.workloads)])

    total_pages = sum(workload_results)
    logger.info(f"Total pages processed: {total_pages}")

    # Update job status to IN_PROGRESS
    await con.xadd(
        f"job-status:{task_id}",
        {"status": json.dumps(JobStatus.IN_PROGRESS.value)}
    )

    response = PipelineResponseModel(
        message="Tasks submitted successfully",
        task_id=task_id
    )

    response_hash = hashlib.sha256(json.dumps(response.dict()).encode()).hexdigest()
    await con.setex(cache_key, 3600, response_hash)
    await con.setex(response_hash, 3600, json.dumps(response.dict()))

    return {"task_id": response.task_id, "message": response.message}

async def get_results_from_stream(con: redis.Redis, task_id: str) -> List[Dict]:
    entries = await con.xrange(f"job-status:{task_id}")
    results = []
    for _, entry in entries:
        if b'result' in entry:
            result = json.loads(entry[b'result'])
            if 'results' in result:
                results.extend(result['results'])
    return results

async def get_pipeline_results(task_id: str):
    try:
        con = await redis.from_url("redis://redis:6379/0", encoding="utf-8", decode_responses=True)
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return {"error": "Failed to connect to Redis"}, 500

    try:
        status_stream = await con.xrange(f"job-status:{task_id}")
    except Exception as e:
        logger.error(f"Failed to fetch job status: {e}")
        return {"error": "Failed to fetch job status"}, 500

    if not status_stream:
        return {"error": "Task not found"}, 404

    last_entry = status_stream[-1]
    _, entry = last_entry

    try:
        status = JobStatus(json.loads(entry['status']))
    except (json.JSONDecodeError, ValueError):
        logger.error(f"Invalid status value: {entry['status']}")
        status = JobStatus.PENDING

    total_run_time = entry.get('total_run_time', 'N/A')
    
    results = []
    if 'result' in entry:
        try:
            result_data = json.loads(entry['result'])
            if isinstance(result_data, dict) and 'results' in result_data:
                results = result_data['results']
            elif isinstance(result_data, list):
                results = result_data
            else:
                results = [result_data]
        except json.JSONDecodeError:
            logger.error(f"Failed to parse result JSON: {entry['result']}")

    response = PipelineResult(
        task_id=task_id,
        status=status,
        results=results,
        total_run_time=total_run_time
    )

    return response.dict()
