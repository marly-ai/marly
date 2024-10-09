from typing import Dict, List, Optional, Union, Any
from common.models.base.base_model import BaseModel
from common.models.enums.model_enums import MistralModelName, MistralAPIURL
from langsmith import traceable
import requests

class MistralModel(BaseModel):
    def __init__(self, api_key: str, model_name: str, additional_params: Dict[str, Any] = None):
        if not api_key:
            raise ValueError("API key must be provided.")
        self.api_key = api_key
        
        if not additional_params:
            additional_params = {}
        
        self.model_name = self.validate_model_name(model_name)
        self.api_url = MistralAPIURL.CHAT_COMPLETIONS.value

    @staticmethod
    def validate_model_name(model_name: str) -> str:
        try:
            return MistralModelName(model_name).value
        except ValueError:
            raise ValueError(f"Invalid model name. Allowed values are: {', '.join([m.value for m in MistralModelName])}")

    @traceable(run_type="llm")
    def do_completion(self,
                      messages: List[Dict[str, str]],
                      model_name: Optional[str] = None,
                      max_tokens: Optional[int] = None,
                      temperature: Optional[float] = None,
                      top_p: Optional[float] = None,
                      stop: Optional[Union[str, List[str]]] = None,
                      response_format: Optional[Dict[str, str]] = None) -> str:
        if not messages:
            raise ValueError("'messages' must be provided.")
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        data = {
            "model": self.validate_model_name(model_name) if model_name else self.model_name,
            "messages": messages,
        }
        
        if max_tokens is not None:
            data["max_tokens"] = max_tokens
        if temperature is not None:
            data["temperature"] = temperature
        if top_p is not None:
            data["top_p"] = top_p
        if stop is not None:
            data["stop"] = stop
        if response_format is not None:
            data["response_format"] = response_format
        
        response = requests.post(self.api_url, headers=headers, json=data)
        
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content']
        else:
            raise Exception(f"Error: {response.status_code}\n{response.text}")
