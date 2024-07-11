from fastapi import HTTPException
from application.worker.loading.data_models.loading_request import LoadingRequestModel
from common.redis.redis_config import redis_client
from application.worker.loading.loading_executor.loading_executor import run_loading
import json
import time
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_loading_with_retry(task_data):
    return process_loading(task_data)

def process_loading(task_data):
    start_time = time.time()
    logger.info(f"Processing loading task: {task_data}")
    try:
        loading_request = LoadingRequestModel(
            transformed_metrics=task_data['transformed_metrics'],
            output_data_source=task_data['output_data_source'],
            output_data_type=task_data['output_data_type'],
            data_location=task_data['data_location'],
            column_locations=json.loads(task_data['column_locations']) if 'column_locations' in task_data else None,
            output_filename=task_data.get('output_filename'),
            normalization_id=task_data.get('normalization_id'),
            time=task_data.get('time')
        )
        
        logger.info("Starting loading process")
        response = run_loading(loading_request) or ""
        logger.info("Loading process completed")
        
        response.time = int(time.time() - start_time)
        
        logger.info("Loading task processed successfully")
        return response.dict()
    except Exception as e:
        logger.error(f"Error processing loading task: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

def run_loadings():
    logger.info("Starting loading worker")
    
    redis_client.delete('loading-stream')
    logger.info("Deleted existing loading stream")
    
    last_id = '0-0'
    workload_size = 0
    processed_tasks = 0
    
    pubsub = redis_client.pubsub()
    pubsub.subscribe('workload-size-topic')
    
    while True:
        try:
            message = pubsub.get_message(timeout=1)
            if message and message['type'] == 'message':
                new_workload_size = int(message['data'])
                if new_workload_size != workload_size:
                    logger.info(f"Received new workload size: {new_workload_size}")
                    workload_size = new_workload_size
                    processed_tasks = 0
        
            streams = redis_client.xread({'loading-stream': last_id}, count=1, block=0)
            if streams:
                for stream_name, messages in streams:
                    for message_id, message in messages:
                        logger.info(f"Processing task from loading-stream: {message}")
                        try:
                            task_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in message.items()}
                            result = process_loading_with_retry(task_data)
                            
                            redis_client.publish('loading-response-topic', json.dumps(result))
                            pipe = redis_client.pipeline()
                            pipe.xdel('loading-stream', message_id)
                            pipe.execute()
                            logger.info("Published result to loading-response-topic and deleted from loading-stream")
                            
                            processed_tasks += 1
                            if processed_tasks == workload_size:
                                logger.info(f"Completed processing for all {workload_size} tasks")
                                processed_tasks = 0 
                        except RetryError:
                            logger.error(f"All retries exhausted for message {message_id}. Deleting the message.", exc_info=True)
                            redis_client.xdel('loading-stream', message_id)
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {str(e)}", exc_info=True)
                        
                        last_id = message_id
            else:
                logger.debug("No new messages in loading-stream. Waiting...")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)
            time.sleep(1)
