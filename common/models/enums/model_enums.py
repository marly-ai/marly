from enum import Enum

class OpenAIModelName(Enum):
    GPT_3_5_TURBO = "gpt-3.5-turbo"
    GPT_4 = "gpt-4"
    GPT_4O = "gpt-4o"
    CLAUDE_3_HAIKU = "claude-3-haiku"

class AzureModelName(Enum):
    GPT_35_TURBO = "gpt-35-turbo"
    GPT_4 = "gpt-4"
    GPT_4_32K = "gpt-4-32k"
    GPT_4O = "gpt-4o"
