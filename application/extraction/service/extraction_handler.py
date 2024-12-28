import asyncio
import logging
import time
import gc
from typing import List, Dict, NamedTuple, Optional
from io import BytesIO
from redis.asyncio import Redis
from common.redis.redis_config import get_redis_connection
from common.text_extraction.text_extractor import extract_page_as_markdown, find_common_pages
import base64
from dotenv import load_dotenv
from common.models.model_factory import ModelFactory
from langsmith import Client as LangSmithClient
from common.prompts.prompt_enums import PromptType
import tiktoken
from dataclasses import dataclass
from celery import Celery
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from application.extraction.service.processing_handler import (
    get_latest_model_details,
    preprocess_messages,
    process_web_content
)
from common.agents.prs_agent import process_extraction, AgentMode

# Initialize Celery
celery_app = Celery('extraction_tasks')
celery_app.conf.update(
    broker_url='redis://redis:6379/0',
    result_backend='redis://redis:6379/0',
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    task_routes={
        'process_pdf_chunk': {'queue': 'pdf_processing'},
        'process_batch': {'queue': 'batch_processing'}
    },
    task_time_limit=3600,  # 1 hour timeout
    worker_prefetch_multiplier=1,  # One task per worker
    worker_max_tasks_per_child=100  # Restart worker after 100 tasks
)

# Worker pool configuration
MAX_WORKERS = 10
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
process_pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

langsmith_client = LangSmithClient()

class PageNumbers(NamedTuple):
    pages: List[int]

@dataclass
class ProcessingProgress:
    current: int
    total: int
    stage: str
    timestamp: float
    status: str

class ProcessingQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.results = {}
        self.progress = {}
        
    async def add_job(self, pdf_key: str, job_id: str):
        await self.queue.put((pdf_key, job_id))
        self.progress[job_id] = ProcessingProgress(0, 0, "queued", time.time(), "pending")
    
    async def get_progress(self, job_id: str) -> Optional[ProcessingProgress]:
        return self.progress.get(job_id)

# Global queue instance
processing_queue = ProcessingQueue()

async def get_model_client():
    """Get model client for batch processing."""
    redis = await get_redis_connection()
    model_details = await get_latest_model_details(redis)
    if not model_details:
        raise Exception("Could not get model details")
    
    return ModelFactory.create_model(
        model_type=model_details.provider_type,
        model_name=model_details.provider_model_name,
        api_key=model_details.api_key,
        additional_params=model_details.additional_params
    )

@celery_app.task(name='process_pdf_chunk')
def process_pdf_chunk(pdf_chunk: bytes, start_page: int, end_page: int, job_id: str):
    """Celery task for processing a chunk of PDF pages."""
    file_stream = BytesIO(pdf_chunk)
    return asyncio.run(_process_pdf_chunk(file_stream, start_page, end_page, job_id))

@celery_app.task(name='process_batch')
def process_batch(batch_content: str, keywords: str, examples: str, job_id: str):
    """Celery task for processing a batch of content."""
    return asyncio.run(_process_batch(batch_content, keywords, examples, job_id))

async def _process_pdf_chunk(file_stream: BytesIO, start_page: int, end_page: int, job_id: str) -> Dict:
    """Process a chunk of PDF pages asynchronously."""
    try:
        extracted_contents = []
        for page in range(start_page, end_page + 1):
            content = await process_page(file_stream, page)
            if content:
                token_count = estimate_tokens(content)
                extracted_contents.append((page, content, token_count))
        return {'success': True, 'contents': extracted_contents}
    except Exception as e:
        logger.error(f"Error processing PDF chunk {start_page}-{end_page}: {e}")
        return {'success': False, 'error': str(e)}

async def _process_batch(batch_content: str, keywords: str, examples: str, job_id: str) -> str:
    """Process a batch of content asynchronously."""
    try:
        client = await get_model_client()  # Get model client for this batch
        result = await call_llm_with_file_content(batch_content, keywords, examples, client)
        return result if result else ""
    except Exception as e:
        logger.error(f"Error processing batch in job {job_id}: {e}")
        return ""

async def run_extraction(pdf_key: str, schemas: List[Dict[str, str]], job_id: str = None) -> List[str]:
    """Enhanced extraction with smart processing selection."""
    if not job_id:
        job_id = f"job_{int(time.time())}"
    
    logger.info(f"Starting extraction for pdf_key: {pdf_key}, job_id: {job_id}")
    await track_progress(job_id, 0, len(schemas), "initialization")
    
    redis = await get_redis_connection()
    try:
        file_stream = await get_file_stream(redis, pdf_key)
        file_size = file_stream.getbuffer().nbytes
        
        # Get model client and examples
        client = await get_model_client()
        examples = await get_examples(client, str(schemas))
        formatted_keywords = "\n".join([f"{k}: {v}" for k, v in schemas[0].items()])
        
        # Get relevant pages first
        page_numbers = await get_relevant_page_numbers(client, file_stream, formatted_keywords)
        total_relevant_pages = len(page_numbers.pages)
        
        # Choose processing method based on number of relevant pages
        DISTRIBUTED_THRESHOLD = 10  # pages
        
        if total_relevant_pages <= DISTRIBUTED_THRESHOLD:
            # Direct processing for small number of relevant pages
            logger.info(f"Using direct processing for {total_relevant_pages} relevant pages")
            metrics = await retrieve_multi_page_metrics(
                page_numbers.pages, formatted_keywords, file_stream,
                examples, client, job_id
            )
            await track_progress(job_id, len(schemas), len(schemas), "completed", "success")
            return [metrics] if metrics else []
        else:
            # Distributed processing for larger number of relevant pages
            logger.info(f"Using distributed processing for {total_relevant_pages} relevant pages")
            
            # Calculate optimal chunk size for relevant pages
            chunk_size = max(1, total_relevant_pages // MAX_WORKERS)
            chunks = [page_numbers.pages[i:i + chunk_size] for i in range(0, total_relevant_pages, chunk_size)]
            
            # Process chunks in parallel
            chunk_tasks = []
            for chunk_pages in chunks:
                task = process_pdf_chunk.delay(
                    base64.b64encode(file_stream.getvalue()).decode(),
                    min(chunk_pages),
                    max(chunk_pages),
                    job_id
                )
                chunk_tasks.append(task)
            
            # Collect results
            results = []
            for task in chunk_tasks:
                result = task.get()
                if result['success']:
                    results.extend(result['contents'])
            
            if not results:
                logger.error("No results from distributed processing")
                return []
            
            # Combine and validate results
            combined_content = "\n=== BATCH BREAK ===\n".join([content for _, content, _ in sorted(results, key=lambda x: x[0])])
            final_result = await validate_metrics(combined_content, examples, client)
            
            await track_progress(job_id, len(schemas), len(schemas), "completed", "success")
            await cleanup_processed_files(redis, pdf_key)
            
            return [final_result] if final_result else []
            
    except Exception as e:
        logger.error(f"Error in extraction process: {e}")
        await track_progress(job_id, 0, len(schemas), "failed", "error")
        return []

async def process_small_pdf(file_stream: BytesIO, schemas: List[Dict[str, str]], examples: str, client, job_id: str) -> List[str]:
    """Direct processing for small PDFs."""
    try:
        formatted_keywords = "\n".join([f"{k}: {v}" for k, v in schemas[0].items()])
        page_numbers = await get_relevant_page_numbers(client, file_stream, formatted_keywords)
        
        metrics = await retrieve_multi_page_metrics(
            page_numbers.pages, formatted_keywords, file_stream,
            examples, client, job_id
        )
        
        await track_progress(job_id, len(schemas), len(schemas), "completed", "success")
        return [metrics] if metrics else []
        
    except Exception as e:
        logger.error(f"Error in direct processing: {e}")
        return []

async def track_progress(job_id: str, current: int, total: int, stage: str, status: str = "running"):
    processing_queue.progress[job_id] = ProcessingProgress(
        current=current,
        total=total,
        stage=stage,
        timestamp=time.time(),
        status=status
    )

async def cleanup_processed_files(redis: Redis, pdf_key: str):
    """Cleanup temporary files and memory after processing."""
    try: 
        await redis.delete(f"processed:{pdf_key}")
        gc.collect()
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def estimate_tokens(text: str) -> int:
    """More accurate token estimation using tiktoken."""
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(text))
    except Exception:
        # Fallback to char-based estimation if tiktoken fails
        return len(text) // 4

async def calculate_optimal_batch_size(content_length: int, max_tokens: int = 3000) -> int:
    """Calculate optimal batch size based on content length."""
    return min(max_tokens, max(1000, content_length // 2))

async def process_schema(client, file_stream: BytesIO, schema: Dict[str, str], job_id: str, schema_idx: int, total_schemas: int) -> str:
    """Enhanced schema processing with progress tracking."""
    try:
        await track_progress(job_id, schema_idx, total_schemas, f"processing_schema_{schema_idx}")
        formatted_keywords = "\n".join([f"{k}: {v}" for k, v in schema.items()])
        
        page_numbers = await get_relevant_page_numbers(client, file_stream, formatted_keywords)
        examples = await get_examples(client, formatted_keywords)
        
        await track_progress(job_id, schema_idx + 0.5, total_schemas, f"extracting_metrics_{schema_idx}")
        metrics = await retrieve_multi_page_metrics(
            page_numbers.pages, formatted_keywords, file_stream, 
            examples if examples else "", client, job_id
        )
        
        return metrics if metrics else ""
    except Exception as e:
        logger.error(f"Error processing schema {schema_idx}: {e}")
        return ""

async def get_file_stream(redis: Redis, pdf_key: str) -> BytesIO:
    try:
        base64_string: str = await redis.get(pdf_key)
        decoded_bytes = base64.b64decode(base64_string)
        return BytesIO(decoded_bytes)
    except Exception as e:
        logger.error(f"Failed to process file stream: {e}")
        raise e

async def get_examples(client, formatted_keywords: str) -> str:
    try:
        prompt = langsmith_client.pull_prompt(PromptType.EXAMPLE_GENERATION.value)
        messages = prompt.invoke({"first_value": formatted_keywords})
        processed_messages = preprocess_messages(messages)
        if processed_messages:
            return client.do_completion(processed_messages)
    except Exception as e:
        logger.error(f"Error generating examples: {e}")
    return ""

async def get_relevant_page_numbers(client, file_stream: BytesIO, formatted_keywords: str) -> PageNumbers:
    try:
        common_pages = await find_common_pages(client, file_stream, formatted_keywords)
        return PageNumbers(pages=common_pages)
    except Exception as e:
        logger.error(f"Error with find_common_pages: {e}")
        raise Exception("Cannot process: No common pages found or unreadable file.")

async def retrieve_multi_page_metrics(
    pages: List[int], keywords: str, file_stream: BytesIO, examples: str, client, job_id: str
) -> str:
    try:
        # Dynamic token limit calculation
        MAX_TOKENS = await calculate_optimal_batch_size(3000)
        logger.info(f"Using dynamic token limit: {MAX_TOKENS}")
        
        extracted_contents = []
        total_pages = len(pages)
        
        # Extract content with progress tracking
        for idx, page in enumerate(pages):
            await track_progress(job_id, idx, total_pages, f"extracting_page_{page}")
            content = await process_page(file_stream, page)
            if content:
                # Accurate token estimation
                token_count = estimate_tokens(content)
                extracted_contents.append((page, content, token_count))
                logger.info(f"Page {page}: {token_count} tokens")

        if not extracted_contents:
            logger.error("No content extracted from pages")
            return ""

        # Process pages in optimized batches
        all_results = []
        current_batch = []
        current_token_count = 0
        
        for page_num, content, token_count in extracted_contents:
            if current_token_count + token_count > MAX_TOKENS:
                # Process current batch
                if current_batch:
                    batch_content = "\n=== PAGE BREAK ===\n".join([c for _, c, _ in current_batch])
                    batch_result = await call_llm_with_file_content(batch_content, keywords, examples, client)
                    if batch_result:
                        all_results.append(batch_result)
                    
                    # Clear memory after batch processing
                    del batch_content
                    gc.collect()
                
                # Start new batch
                current_batch = [(page_num, content, token_count)]
                current_token_count = token_count
            else:
                current_batch.append((page_num, content, token_count))
                current_token_count += token_count

        # Process final batch
        if current_batch:
            batch_content = "\n=== PAGE BREAK ===\n".join([c for _, c, _ in current_batch])
            batch_result = await call_llm_with_file_content(batch_content, keywords, examples, client)
            if batch_result:
                all_results.append(batch_result)
            
            # Clear memory
            del batch_content
            gc.collect()

        # Combine and validate results with progress tracking
        combined_results = "\n=== BATCH BREAK ===\n".join(all_results)
        await track_progress(job_id, total_pages, total_pages, "validating_results")
        
        validation_result = await validate_metrics(combined_results, examples, client)
        
        # Clear memory after processing
        del all_results
        del combined_results
        gc.collect()
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Error in retrieve_multi_page_metrics: {e}")
        await track_progress(job_id, 0, len(pages), "failed", "error")
        return ""

async def process_page(file_stream: BytesIO, page: int) -> str:
    try:
        content = extract_page_as_markdown(file_stream, page)
        return content.decode('utf-8') if isinstance(content, bytes) else content
    except Exception as e:
        logger.error(f"Error extracting page {page} as markdown: {e}")
        return ""

async def call_llm_with_file_content(formatted_content: str, keywords: str, examples: str, client) -> str:
    try:
        text = f"""SOURCE DOCUMENT:
            {formatted_content}

            EXAMPLE FORMAT:
            {examples}

            METRICS TO EXTRACT:
            {keywords}

            Note: The source document may contain multiple pages separated by '=== PAGE BREAK ==='.
            Extract all relevant metrics from each page section while maintaining accuracy."""
        return process_extraction(text, client, AgentMode.EXTRACTION)
    except Exception as e:
        logger.error(f"Error calling LLM with file content: {e}")
    return ""

async def validate_metrics(
    llm_results: str,
    examples: str,
    client
) -> str:
    try:
        # Split into original batches
        batched_results = llm_results.split("=== BATCH BREAK ===")
        MAX_VALIDATION_TOKENS = await calculate_optimal_batch_size(3000)
        
        # Process validation in chunks with memory management
        validated_chunks = []
        current_chunk = []
        current_token_count = 0
        
        for batch in batched_results:
            if not batch.strip():
                continue
                
            # Accurate token estimation
            batch_token_count = estimate_tokens(batch.strip())
            
            if current_token_count + batch_token_count > MAX_VALIDATION_TOKENS:
                # Validate current chunk
                if current_chunk:
                    try:
                        chunk_text = "\n".join(current_chunk)
                        prompt = langsmith_client.pull_prompt(PromptType.VALIDATION.value)
                        messages = prompt.invoke({
                            "first_value": chunk_text,
                            "second_value": examples
                        })
                        processed_messages = preprocess_messages(messages)
                        if processed_messages:
                            chunk_validation = client.do_completion(processed_messages)
                            validated_chunks.append(chunk_validation)
                        
                        # Clear memory
                        del chunk_text
                        del messages
                        gc.collect()
                    except Exception as e:
                        logger.error(f"Error validating chunk: {e}")
                
                # Start new chunk
                current_chunk = [batch.strip()]
                current_token_count = batch_token_count
            else:
                current_chunk.append(batch.strip())
                current_token_count += batch_token_count
        
        # Process final chunk
        if current_chunk:
            try:
                chunk_text = "\n".join(current_chunk)
                prompt = langsmith_client.pull_prompt(PromptType.VALIDATION.value)
                messages = prompt.invoke({
                    "first_value": chunk_text,
                    "second_value": examples
                })
                processed_messages = preprocess_messages(messages)
                if processed_messages:
                    chunk_validation = client.do_completion(processed_messages)
                    validated_chunks.append(chunk_validation)
                
                # Clear memory
                del chunk_text
                del messages
                gc.collect()
            except Exception as e:
                logger.error(f"Error validating final chunk: {e}")
        
        # If we have multiple validated chunks, do a final consolidation
        if len(validated_chunks) > 1:
            try:
                final_consolidation = "\n".join(validated_chunks)
                # Check if final consolidation needs chunking
                if estimate_tokens(final_consolidation) > MAX_VALIDATION_TOKENS:
                    logger.warning("Final consolidation exceeds token limit, will process in chunks")
                    return await validate_metrics(final_consolidation, examples, client)
                
                prompt = langsmith_client.pull_prompt(PromptType.VALIDATION.value)
                messages = prompt.invoke({
                    "first_value": final_consolidation,
                    "second_value": examples
                })
                processed_messages = preprocess_messages(messages)
                if processed_messages:
                    final_result = client.do_completion(processed_messages)
                    return final_result
            except Exception as e:
                logger.error(f"Error in final consolidation: {e}")
                return validated_chunks[0] if validated_chunks else ""
        elif validated_chunks:
            return validated_chunks[0]
        
        return ""
        
    except Exception as e:
        logger.error(f"Error validating metrics: {e}")
        return ""

async def run_web_extraction(url: str, schemas: List[Dict[str, str]], job_id: str = None) -> List[str]:
    """Handle extraction from web content using processing_handler."""
    if not job_id:
        job_id = f"job_{int(time.time())}"
    
    logger.info(f"Starting web extraction for url: {url}, job_id: {job_id}")
    await track_progress(job_id, 0, len(schemas), "initialization")
    
    try:
        redis = await get_redis_connection()
        # Process web content using existing handler
        results = await process_web_content(redis, url, schemas)
        
        await track_progress(job_id, len(schemas), len(schemas), "completed", "success")
        return results
        
    except Exception as e:
        logger.error(f"Error in web extraction: {e}")
        await track_progress(job_id, 0, len(schemas), "failed", "error")
        return []