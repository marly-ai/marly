from common.text_extraction import text_extractor
from concurrent.futures import ThreadPoolExecutor
from application.configuration.repositories.model_repository import ModelRepository
from application.integrations.sources.repositories.source_repository import SourceRepository
from application.configuration.configs.model_config import ModelConfig
from uuid import UUID
import logging
import os
from dotenv import load_dotenv
from common.observability.observability_decorator import observability

load_dotenv()

global OBSERVABILITY_PROVIDER
OBSERVABILITY_PROVIDER = os.getenv('OBSERVABILITY_PROVIDER', '')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_extraction(file_name, keywords, file_list, input_data_source, prompts, model_id):
    logger.info(f"Starting extraction for model_id: {model_id}")
    
    model_config = ModelRepository.get(UUID(model_id))
    
    if not model_config:
        logger.error(f"Model with ID {model_id} not found")
        raise ValueError(f"Model with ID {model_id} not found")
    
    logger.info(f"Model configuration retrieved: {model_config}")
    
    model_instance = ModelConfig.get_model_instance(model_config)
    logger.info(f"Model instance created: {model_instance}")
    logger.info(f"Model name: {model_config.model_name}")
    
    formatted_keywords = "\n".join(keywords.keys())
    logger.info(f"Formatted keywords: {formatted_keywords}")
    formatted_keywords_with_values = "\n".join([f"{key}: {value}" for key, value in keywords.items()])
    logger.info(f"Formatted keywords with values: {formatted_keywords_with_values}")

    logger.info(f"Prompts: {prompts}")
    logger.info(f"Observability provider: {OBSERVABILITY_PROVIDER}")
    target_file_name = get_target_file_name(prompts["FILE_EXTRACTION"], file_name, file_list, model_instance, model_config.model_name)
    logger.info(f"Target file name: {target_file_name}")
    
    examples = get_examples(prompts["EXAMPLE_GENERATION"], formatted_keywords_with_values, model_instance, model_config.model_name)
    logger.info(f"Examples: {examples}")

    data_source = SourceRepository.get_integration(input_data_source)
    pdf_blob = data_source.read({"file_key": target_file_name})
    logger.info(f"PDF blob read from data source")

    #page_numbers = {'pages': [4, 5, 6, 10, 11, 12, 13, 14]}
    #page_numbers = {'pages': [4, 6]}
    page_numbers = get_relevant_page_numbers(prompts["PAGE_FINDER"], pdf_blob, formatted_keywords, model_instance, model_config.model_name)
    logger.info(f"Page numbers: {page_numbers}")

    metrics = retrieve_multi_page_metrics(
        page_numbers["pages"], formatted_keywords, pdf_blob, examples, prompts["EXTRACTION"], prompts["VALIDATION"], model_instance, model_config.model_name
    )
    logger.info(f"Metrics: {metrics}")
    
    return metrics

@observability(OBSERVABILITY_PROVIDER)
def get_target_file_name(prompt_config, first_value, second_value, model_instance, model_name):
    return model_instance.do_completion(
        prompt_config=prompt_config,
        model_name=model_name,
        first_value=first_value,
        second_value=second_value
    )

@observability(OBSERVABILITY_PROVIDER)
def get_examples(prompt_config, formatted_keywords, model_instance, model_name):
    return model_instance.do_completion(
        prompt_config=prompt_config,
        model_name=model_name,
        first_value=formatted_keywords
    )

def get_relevant_page_numbers(prompt_config, file_blob, formatted_keywords, model_instance, model_name):
    try:
        common_pages = text_extractor.find_common_pages(file_blob, formatted_keywords, prompt_config, model_instance, model_name)
    except Exception as e:
        logger.error(f"Error with find_common_pages: {e}, using find_common_pages_old as backup.")
        common_pages = None

    if not common_pages:
        try:
            common_pages = text_extractor.find_common_pages_old(file_blob, formatted_keywords)
        except Exception as e:
            logger.error(f"Error with find_common_pages_old: {e}")
            raise ValueError("Cannot process: No common pages found or unreadable file.")

    if common_pages:
        return {"pages": common_pages}
    return None

def retrieve_multi_page_metrics(pages, keywords, file_blob, examples, prompt_config, validation_prompt_config, model_instance, model_name):
    extracted_contents = []
    
    def process_page(page):
        try:
            content = text_extractor.extract_page_as_markdown(file_blob, page)
        except Exception as e:
            logger.error(f"Error extracting content as markdown from page {page}: {e}")
            content = ""
        
        if len(content) == 0:
            try:
                content = text_extractor.extract_markdown(file_blob, page)
            except Exception as e:
                logger.error(f"Error extracting text with pymupdf from page {page}: {e}")
                return None
        
        return content
    
    for page in pages:
        content = process_page(page)
        if content:
            extracted_contents.append(content)
    
    def call_llm(content):
        return call_llm_with_file_content(prompt_config, content, keywords, examples, model_instance, model_name)
    
    if not extracted_contents:
        logger.warning("No content extracted from pages")
        return ""

    with ThreadPoolExecutor(max_workers=min(len(extracted_contents), 5)) as executor:
        llm_results = list(executor.map(call_llm, extracted_contents))

    return validate_metrics(validation_prompt_config, llm_results, examples, model_instance, model_name)

@observability(OBSERVABILITY_PROVIDER)
def call_llm_with_file_content(prompt_config, formatted_content, keywords, examples, model_instance, model_name):
    return model_instance.do_completion(
        prompt_config=prompt_config,
        model_name=model_name,
        first_value=formatted_content,
        second_value=keywords,
        third_value=examples
    )

@observability(OBSERVABILITY_PROVIDER)
def validate_metrics(prompt_config, llm_results, examples, model_instance, model_name):
    return model_instance.do_completion(
        prompt_config=prompt_config,
        model_name=model_name,
        first_value=str(llm_results),
        second_value=examples
    )
