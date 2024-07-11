import json
import logging

logger = logging.getLogger(__name__)

def normal_flow(prompt_config, *args, model_instance, **kwargs):
    if isinstance(prompt_config, str):
        try:
            prompt_config = json.loads(prompt_config)
        except json.JSONDecodeError:
            raise ValueError("Invalid prompt_config string: not a valid JSON")
    logger.info(f"Raw prompt_config no observability: {prompt_config}")
    
    if isinstance(prompt_config, dict) and 'messages' in prompt_config:
        messages = prompt_config['messages']
    else:
        messages = prompt_config.messages if hasattr(prompt_config, 'messages') else []
    
    messages = [
        message._asdict() if hasattr(message, '_asdict') else 
        (message if isinstance(message, dict) else 
         {'role': message.role, 'content': message.content})
        for message in messages
    ]
    
    variables = {}
    for i, value in enumerate(args, start=1):
        if i > 5:
            break
        key = f"{'first' if i == 1 else 'second' if i == 2 else 'third' if i == 3 else 'fourth' if i == 4 else 'fifth'}_value"
        variables[key] = json.dumps(value) if isinstance(value, (list, dict)) else value

    logger.info(f"Raw variables no observability: {variables}")
    
    if variables and messages:
        messages[0]['content'] = messages[0]['content'].format(**variables)
    
    kwargs['messages'] = messages
    # the below is not ideal at all need to refactor
    if prompt_config.name == 'PAGE_FINDER':
        content = f"Given an image and a list of search terms: {variables.get('first_value', '')}, respond with 'Yes' if one or more of the search terms are present and have associated values in the page content, otherwise respond with 'No'."
        messages = [
            {"role": "system", "content": "You are an AI that processes images to find information."},
            {"role": "user", "content": [
                {"type": "text", "text": content},
                {"type": "image_url", "image_url": {
                    "url": f"data:image/png;base64,{variables.get('second_value', '')}"
                }}
            ]}
        ]
    logger.info(f"Raw messages no observability before sending to model provider: {messages}")
    response_format_raw = prompt_config.get('response_format') if isinstance(prompt_config, dict) else getattr(prompt_config, 'response_format', None)
    if response_format_raw:
        kwargs['response_format'] = {"type": response_format_raw}
        return model_instance.do_completion(**kwargs)
    
    response = model_instance.do_completion(
        messages=messages
    )
    return response

