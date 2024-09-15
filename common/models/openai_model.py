from typing import Dict, List, Optional, Union, Any
from common.models.base.base_model import BaseModel
from common.models.enums.model_enums import OpenAIModelName
from openai import OpenAI
from langsmith import traceable

class OpenaiModel(BaseModel):
    def __init__(self, api_key: str, model_name: str, additional_params: Dict[str, Any] = None):
        if not api_key:
            raise ValueError("API key must be provided.")
        self.api_key = api_key
        
        if not additional_params:
            additional_params = {}
        
        self.model_name = self.validate_model_name(model_name)
        self.base_url = additional_params.get('base_url')
        
        client_params = {'api_key': self.api_key}
        if self.base_url:
            client_params['base_url'] = self.base_url
        
        self.client = OpenAI(**client_params)

    @staticmethod
    def validate_model_name(model_name: str) -> str:
        try:
            return OpenAIModelName(model_name).value
        except ValueError:
            raise ValueError(f"Invalid model name. Allowed values are: {', '.join([m.value for m in OpenAIModelName])}")

    @traceable(run_type="llm")
    def do_completion(self,
                      messages: List[Dict[str, str]],
                      model_name: Optional[str] = None,
                      max_tokens: Optional[int] = None,
                      temperature: Optional[float] = None,
                      top_p: Optional[float] = None,
                      n: Optional[int] = None,
                      stop: Optional[Union[str, List[str]]] = None,
                      response_format: Optional[Dict[str, str]] = None) -> Dict:
        if not messages:
            raise ValueError("'messages' must be provided.")
        
        params = {
            "model": self.validate_model_name(model_name) if model_name else self.model_name,
            "messages": messages,
        }
        
        if max_tokens is not None:
            params["max_tokens"] = max_tokens
        if temperature is not None:
            params["temperature"] = temperature
        if top_p is not None:
            params["top_p"] = top_p
        if n is not None:
            params["n"] = n
        if stop is not None:
            params["stop"] = stop
        if response_format is not None:
            params["response_format"] = response_format
        
        response = self.client.chat.completions.create(**params)
        
        return response.choices[0].message.content
