import json
import logging
import os
from dotenv import load_dotenv
from common.models.portkey_model import PortkeyModel

load_dotenv()

logger = logging.getLogger(__name__)

def handle_portkey(prompt_config, *args, model_instance):
    logger.info(f"Raw prompt_config: {prompt_config}")
    
    if isinstance(prompt_config, str):
        logger.info("prompt_config is a string, attempting to parse as JSON")
        try:
            prompt_config = json.loads(prompt_config)
            logger.info(f"Successfully parsed prompt_config as JSON: {prompt_config}")
        except json.JSONDecodeError:
            logger.info("Failed to parse prompt_config as JSON, assuming it's a prompt ID")
            prompt_id = prompt_config
        else:
            prompt_id = prompt_config.get('prompt_id')
            logger.info(f"Extracted prompt_id from parsed JSON: {prompt_id}")
    else:
        logger.info(f"prompt_config is not a string, type: {type(prompt_config)}")
        prompt_id = prompt_config.prompt_id if hasattr(prompt_config, 'prompt_id') else None
        logger.info(f"Extracted prompt_id from prompt_config object: {prompt_id}")
    
    variables = {}
    for i, value in enumerate(args, start=1):
        if i > 5:
            break
        key = f"{'first' if i == 1 else 'second' if i == 2 else 'third' if i == 3 else 'fourth' if i == 4 else 'fifth'}_value"
        variables[key] = json.dumps(value) if isinstance(value, (list, dict)) else value

    logger.info(f"Raw variables: {variables}")
    logger.info(f"Sending to Portkey - prompt_id: {prompt_id}")
    logger.info(f"Sending to Portkey - variables: {json.dumps(variables, indent=2)}")
    
    if not prompt_id:
        raise ValueError("prompt_config with a valid prompt_id is required for Portkey")
    
    portkey_api_key = os.getenv("PORTKEY_API_KEY")
    portkey_config = os.getenv("PORTKEY_CONFIG", "")

    if not portkey_api_key or not portkey_config:
        raise ValueError("Both PORTKEY_API_KEY and PORTKEY_CONFIG environment variables are required for Portkey")
    
    if prompt_config.name == "EXTRACTION":
        portkey_model = PortkeyModel(api_key=portkey_api_key, config=portkey_config)
    else:
        portkey_model = PortkeyModel(api_key=portkey_api_key, config="")
    
    if prompt_config.name == 'PAGE_FINDER':
        from portkey_ai import Portkey
        client = Portkey(
            api_key=os.getenv("PORTKEY_API_KEY"),
            virtual_key=os.getenv("PORTKEY_VIRTUAL_KEY")
        )
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an AI that processes images to find information."},
                {"role": "user", "content": [
                    {"type": "text", "text": f"Given an image and a list of search terms: {variables.get('first_value', '')}, respond with 'Yes' if one or more of the search terms are present and have associated values in the page content, otherwise respond with 'No'."},
                    {"type": "image_url", "image_url": {
                        "url": f"data:image/png;base64,{variables.get('second_value', '')}"}
                    }
                ]}
            ],
            temperature=0.0,
        )
        return response.choices[0].message.content
    
    prompt_type = prompt_config.get('type', '') if isinstance(prompt_config, dict) else getattr(prompt_config, 'type', '')
    if prompt_type == 'promptOnly':
        messages = portkey_model.get_prompt_messages(prompt_id, variables)
        filtered_messages = [
            {
                "role": msg.role,
                "content": msg.content
            }
            for msg in messages
            if hasattr(msg, 'role') and hasattr(msg, 'content') and msg.role and msg.content
        ]
        logger.info(f"Filtered messages: {filtered_messages}")
        
        completion_args = {"messages": filtered_messages}
        
        response_format = prompt_config.get('response_format') if isinstance(prompt_config, dict) else getattr(prompt_config, 'response_format', None)
        if response_format:
            completion_args["response_format"] = {"type": response_format}
        
        return model_instance.do_completion(**completion_args)
    
    return portkey_model.do_completion({
        "prompt_id": prompt_id,
        "variables": variables
    })

