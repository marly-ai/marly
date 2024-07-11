from fastapi import HTTPException
from application.worker.extraction.data_models.request_extraction import ExtractionRequestModel
from application.orchestration.response.extraction.response_extraction import ExtractionResponseModel
from application.integrations.sources.repositories.source_repository import SourceRepository
from common.redis.redis_config import redis_client
from application.worker.extraction.model_executor.extraction_model_executor import run_extraction
import json
import time
import logging
from uuid import UUID
from application.configuration.repositories.prompt_repository import PromptRepository
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_extraction_with_retry(task_data):
    return process_extraction(task_data)

def process_extraction(task_data):
    start_time = time.time()
    logger.info(f"Processing extraction task: {task_data}")
    try:    
        extraction_request = ExtractionRequestModel(
            filename=task_data['filename'],
            keywords=json.loads(task_data['keywords']),
            input_data_source=task_data['input_data_source'],
            output_data_type=task_data['output_data_type'],
            output_data_source=task_data['output_data_source'],
            data_location=task_data['data_location'],
            prompt_ids=json.loads(task_data['prompt_ids']),
            model_id=task_data['model_id'],
            output_filename=task_data.get('output_filename') or None,
            output_mapping=json.loads(task_data['column_locations']) if 'column_locations' in task_data else None,
            normalization_id=task_data['normalization_id']
        )
        
        filename = extraction_request.filename
        keywords = extraction_request.keywords
        input_data_source = extraction_request.input_data_source
        data_location = extraction_request.data_location
        prompt_ids = extraction_request.prompt_ids
        prompts = {}
        for prompt_type, prompt_id in prompt_ids.items():
            prompt = PromptRepository.get(UUID(prompt_id))
            if not prompt:
                raise ValueError(f"Prompt not found for type: {prompt_type}")
            prompts[prompt_type] = prompt
     
        logger.info(f"Getting integration for input_data_source: {input_data_source}")
        source = SourceRepository.get_integration(input_data_source)
        lst_of_pdfs = source.read_all()
        logger.info(f"Read {len(lst_of_pdfs)} PDFs from source")

        logger.info("Starting extraction process")
        metrics = run_extraction(filename, keywords, lst_of_pdfs, input_data_source, prompts, extraction_request.model_id) or ""
        logger.info("Extraction process completed")
        
        response = ExtractionResponseModel(
            metrics=metrics or '',
            output_data_source=extraction_request.output_data_source or '',
            output_data_type=extraction_request.output_data_type or '',
            time=int(time.time() - start_time),
            data_location=data_location or '',
            column_locations=extraction_request.output_mapping or '',
            prompt_ids=extraction_request.prompt_ids or '',
            model_id=extraction_request.model_id or '',
            normalization_id=extraction_request.normalization_id or ''
        )

        if extraction_request.output_filename:
            response.output_filename = extraction_request.output_filename
        
        logger.info("Extraction task processed successfully")
        return response.dict()
    except Exception as e:
        logger.error(f"Error processing extraction task: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

def run_extractions():
    logger.info("Starting extraction worker")
    
    redis_client.delete('extraction-stream')
    logger.info("Deleted existing extraction stream")
    
    last_id = '0-0'
    
    while True:
        try:
            streams = redis_client.xread({'extraction-stream': last_id}, count=1, block=0)
            if streams:
                for stream_name, messages in streams:
                    for message_id, message in messages:
                        logger.info(f"Processing task from extraction-stream: {message}")
                        try:
                            task_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in message.items()}
                            result = process_extraction_with_retry(task_data)
                            
                            serialized_result = {}
                            for k, v in result.items():
                                if v is None:
                                    serialized_result[k] = ''
                                elif isinstance(v, (dict, list)):
                                    serialized_result[k] = json.dumps(v)
                                else:
                                    serialized_result[k] = str(v)
                            
                            pipe = redis_client.pipeline()
                            pipe.xadd('transformation-stream', serialized_result)
                            pipe.xdel('extraction-stream', message_id)
                            pipe.execute()
                            
                            logger.info("Pushed result to transformation-stream and deleted from extraction-stream")
                        except RetryError:
                            logger.error(f"All retries exhausted for message {message_id}. Deleting the message.")
                            redis_client.xdel('extraction-stream', message_id)
                        
                        last_id = message_id
            else:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)