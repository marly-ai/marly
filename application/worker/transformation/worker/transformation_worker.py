from fastapi import HTTPException
from application.worker.transformation.data_models.request_transformation import TransformationRequestModel
from application.worker.transformation.data_models.response_transformation import TransformationResponseModel
from common.redis.redis_config import redis_client
from application.worker.transformation.model_executor.transformation_model_executor import run_transformation
import json
import time
import logging
from uuid import UUID
from application.configuration.repositories.prompt_repository import PromptRepository
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_transformation_with_retry(task_data):
    return process_transformation(task_data)

def process_transformation(task_data):
    start_time = time.time()
    logger.info(f"Processing transformation task: {task_data}")
    try:
        task = {k.decode('utf-8'): v.decode('utf-8') for k, v in task_data.items()}
        
        transformation_request = TransformationRequestModel(
            metrics=task['metrics'],
            output_data_source=task['output_data_source'],
            output_data_type=task['output_data_type'],
            data_location=task['data_location'],
            prompt_ids=json.loads(task['prompt_ids']),
            model_id=task['model_id'],
            output_filename=task.get('output_filename'),
            column_locations=json.loads(task['column_locations']) if 'column_locations' in task else None,
            normalization_id=task.get('normalization_id')
        )
        
        metrics = transformation_request.metrics
        output_data_source = transformation_request.output_data_source
        data_location = transformation_request.data_location
        prompt_ids = transformation_request.prompt_ids
        prompts = {}
        for prompt_type, prompt_id in prompt_ids.items():
            prompt = PromptRepository.get(UUID(prompt_id))
            if not prompt:
                raise ValueError(f"Prompt not found for type: {prompt_type}")
            prompts[prompt_type] = prompt
     
        logger.info("Starting transformation process")
        transformed_data = run_transformation(metrics, prompts, transformation_request.model_id, transformation_request.column_locations) or ""
        logger.info("Transformation process completed")
        
        response = TransformationResponseModel(
            transformed_metrics=transformed_data,
            output_data_source=output_data_source,
            output_data_type=transformation_request.output_data_type,
            data_location=data_location,
            column_locations=transformation_request.column_locations,
            output_filename=transformation_request.output_filename,
            normalization_id=transformation_request.normalization_id,
            time=int(time.time() - start_time)
        )

        logger.info("Transformation task processed successfully")
        return response.dict()
    except Exception as e:
        logger.error(f"Error processing transformation task: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

def run_transformations():
    logger.info("Starting transformation worker")
    
    redis_client.delete('transformation-stream')
    logger.info("Deleted existing transformation stream")
    
    last_id = '0-0'
    
    while True:
        try:
            streams = redis_client.xread({'transformation-stream': last_id}, count=1, block=0)
            if streams:
                for stream, messages in streams:
                    for message_id, message in messages:
                        logger.info(f"Processing task from transformation-stream: {message}")
                        try:
                            result = process_transformation_with_retry(message)
                            serialized_result = {}
                            for k, v in result.items():
                                if v is None:
                                    serialized_result[k] = ''
                                elif isinstance(v, (dict, list)):
                                    serialized_result[k] = json.dumps(v)
                                else:
                                    serialized_result[k] = str(v)

                            pipe = redis_client.pipeline()
                            pipe.xadd('loading-stream', serialized_result)
                            pipe.xdel('transformation-stream', message_id)
                            pipe.execute()
                            
                            logger.info("Pushed result to loading-stream and deleted from transformation-stream")
                        except RetryError:
                            logger.error(f"All retries exhausted for message {message_id}. Deleting the message.")
                            redis_client.xdel('transformation-stream', message_id)
                        
                        last_id = message_id
            else:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)