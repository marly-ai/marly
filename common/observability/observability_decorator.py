import logging
from functools import wraps
from common.observability.portkey_handler import handle_portkey
from common.observability.normal_flow_handler import normal_flow

logger = logging.getLogger(__name__)

def observability(provider):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if len(args) < 3:
                raise ValueError("Not enough arguments provided")

            prompt_config = args[0]
            model_instance = args[-2]

            if not prompt_config:
                raise ValueError("prompt_config is required")

            if provider == "portkey":
                return handle_portkey(prompt_config, *args[1:-2], model_instance=model_instance)
            else:
                return normal_flow(prompt_config, *args[1:-2], model_instance=model_instance, **kwargs)
        return wrapper
    return decorator
