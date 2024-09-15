from enum import Enum

class ModelType(Enum):
    OPENAI = "openai"
    AZURE = "azure"
    GROQ = "groq"

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

class GroqModelName(Enum):
    DISTIL_WHISPER_ENGLISH = "distil-whisper-large-v3-en"
    GEMMA2_9B = "gemma2-9b-it"
    GEMMA_7B = "gemma-7b-it"
    LLAMA3_GROQ_70B_TOOL_USE = "llama3-groq-70b-8192-tool-use-preview"
    LLAMA3_GROQ_8B_TOOL_USE = "llama3-groq-8b-8192-tool-use-preview"
    LLAMA_3_1_70B = "llama-3.1-70b-versatile"
    LLAMA_3_1_8B = "llama-3.1-8b-instant"
    LLAMA_GUARD_3_8B = "llama-guard-3-8b"
    LLAVA_1_5_7B = "llava-v1.5-7b-4096-preview"
    META_LLAMA3_70B = "llama3-70b-8192"
    META_LLAMA3_8B = "llama3-8b-8192"
    MIXTRAL_8X7B = "mixtral-8x7b-32768"
    WHISPER = "whisper-large-v3"
