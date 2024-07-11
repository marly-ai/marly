from typing import Dict, List, Optional, Union
from common.models.base.base_model import BaseModel
from openai import AzureOpenAI
from common.models.enums.model_enums import AzureModelName

class AzureModel(BaseModel):
    def __init__(self, model_config) -> None:
        if not model_config.api_key:
            raise ValueError("API key must be provided.")
        self.api_key = model_config.api_key
        
        self.api_version = model_config.additional_params.get("api_version")
        if not self.api_version:
            raise ValueError("API version must be provided in additional_params.")
        
        self.azure_endpoint = model_config.additional_params.get("azure_endpoint")
        if not self.azure_endpoint:
            raise ValueError("Azure endpoint must be provided in additional_params.")
        
        self.azure_deployment = model_config.additional_params.get("azure_deployment")
        if not self.azure_deployment:
            raise ValueError("Azure deployment must be provided in additional_params.")
        
        self.model_name = self.validate_model_name(model_config.model_name)
        
        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
            azure_deployment=self.azure_deployment
        )

    @staticmethod
    def validate_model_name(model_name: str) -> str:
        try:
            return AzureModelName(model_name).value
        except ValueError:
            raise ValueError(f"Invalid model name. Allowed values are: {', '.join([m.value for m in AzureModelName])}")

    def do_completion(self,
                      messages: List[Dict[str, str]],
                      model_name: Optional[str] = None,
                      max_tokens: Optional[int] = None,
                      temperature: Optional[float] = None,
                      top_p: Optional[float] = None,
                      n: Optional[int] = None,
                      stop: Optional[Union[str, List[str]]] = None,
                      response_format: Optional[Dict[str, str]] = None) -> str:
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
