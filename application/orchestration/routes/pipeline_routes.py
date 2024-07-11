from fastapi import APIRouter, HTTPException
from application.orchestration.request.pipeline.pipeline_request import PipelineRequestModel
from application.configuration.repositories.pipeline_respository import PipelineRepository
from application.configuration.repositories.schema_repository import SchemaRepository
from application.configuration.repositories.output_mapping_repository import OutputMappingRepository
from common.redis.redis_config import redis_client
import json
import logging

api_router = APIRouter()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

p = redis_client.pubsub()
p.subscribe('loading-response-topic')

@api_router.post(
    "/run-pipeline",
    summary="run pipeline given a set of parameters and operation files",
)
def run_pipeline(customer_input: PipelineRequestModel):
    try:
        logger.info("Deleting existing extraction tasks...")
        redis_client.delete("extraction-tasks")

        logger.info(f"Fetching pipeline with ID: {customer_input.pipeline_id}")
        pipeline = PipelineRepository.get(customer_input.pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")

        logger.info(f"Fetching output mappings with IDs: {customer_input.output_mapping_ids}")
        output_mappings = [OutputMappingRepository.get(mapping_id) for mapping_id in customer_input.output_mapping_ids]
        redis_client.publish("workload-size-topic", len(output_mappings))

        if not all(output_mappings):
           raise HTTPException(status_code=404, detail="One or more output mappings not found")

        real_keywords = SchemaRepository.get(pipeline.pipeline_schema_id)
        if not real_keywords:
            raise HTTPException(status_code=404, detail="Schema not found")

        for output_mapping, filename in zip(output_mappings, customer_input.filenames):
            logger.info(f"Creating task for filename: {filename}")
            try:
                if not output_mapping.data_locations:
                    raise ValueError("Output mapping data_locations is empty")

                task_payload = {
                    "filename": filename,
                    "keywords": json.dumps(real_keywords.keywords),
                    "input_data_source": pipeline.integrations.get("input_data_source", ""),
                    "output_data_type": str(output_mapping.output_data_type),
                    "output_data_source": pipeline.integrations.get("output_data_source", ""),
                    "data_location": output_mapping.data_locations[0], 
                    "prompt_ids": json.dumps({prompt_type: str(prompt_id) for prompt_type, prompt_id in pipeline.prompt_ids.items()}),
                    "model_id": str(pipeline.model_id),
                    "normalization_id": pipeline.normalization_id
                }
                if output_mapping.output_filename:
                    task_payload["output_filename"] = output_mapping.output_filename
                if output_mapping.column_locations:
                    columns = next(iter(output_mapping.column_locations.values()))
                    task_payload["column_locations"] = json.dumps(columns)

                logger.debug(f"Task payload: {task_payload}")
                redis_client.xadd("extraction-stream", task_payload)
            except Exception as e:
                logger.error(f"Error creating task for filename {filename}: {e}")
                raise

        task_count = redis_client.llen("extraction-tasks")
        logger.info(f"Number of extraction tasks in the queue: {task_count}")
        
        pipeline_responses = []
        first_message_received = False
        while True:
            message = p.get_message(timeout=None if not first_message_received else 0.1)
            if message and message['type'] == 'message':
                first_message_received = True
                json_data = message['data'].decode('utf-8')
                data_dict = json.loads(json_data)
                pipeline_responses.append(data_dict)
            elif first_message_received:
                break
        
        return {"message": "Tasks processed successfully", "results": pipeline_responses}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
