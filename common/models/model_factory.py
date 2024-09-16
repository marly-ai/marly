import logging
from typing import Dict, Any, Union
from common.models.enums.model_enums import ModelType, OpenAIModelName, AzureModelName, GroqModelName
from common.models.openai_model import OpenaiModel
from common.models.azure_model import AzureModel
from common.models.groq_model import GroqModel

# Configure logging for this module
logger = logging.getLogger(__name__)

class ModelFactory:
    @staticmethod
    def create_model(model_type: str, model_name: str, api_key: str, additional_params: Dict[str, Any] = None):
        if not model_type:
            raise ValueError("model_type must be provided.")
        if not model_name:
            raise ValueError("model_name must be provided.")
        if not api_key:
            raise ValueError("API key must be provided.")

        try:
            model_type_enum = ModelType(model_type.lower())
        except ValueError:
            raise ValueError(f"Invalid model type. Allowed values are: {', '.join([m.value for m in ModelType])}")

        model_config = {
            "api_key": api_key,
            "model_name": model_name,
            "additional_params": additional_params or {}
        }

        required_params = {
            ModelType.AZURE: ["api_version", "azure_endpoint", "azure_deployment"],
            ModelType.OPENAI: [],
            ModelType.GROQ: []   
        }

        missing_params = []
        for param in required_params.get(model_type_enum, []):
            if param not in model_config["additional_params"]:
                missing_params.append(param)

        if missing_params:
            raise ValueError(f"Missing required additional_params for {model_type}: {', '.join(missing_params)}")

        if model_type_enum == ModelType.OPENAI:
            OpenAIModelName(model_name)
            model_instance = OpenaiModel(**model_config)
        elif model_type_enum == ModelType.AZURE:
            AzureModelName(model_name)
            model_instance = AzureModel(**model_config)
        elif model_type_enum == ModelType.GROQ:
            GroqModelName(model_name)
            model_instance = GroqModel(**model_config)
        else:
            raise ValueError(f"Unsupported model type: {model_type_enum}")

        logger.info(f"Returning model instance of type: {model_type_enum.value}")
        return model_instance