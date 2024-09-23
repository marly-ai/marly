from enum import Enum

class PromptType(Enum):
    EXAMPLE_GENERATION = "marly/example-generation"
    EXTRACTION = "marly/extraction"
    TRANSFORMATION = "marly/transformation"
    TRANSFORMATION_MARKDOWN = "noahegg/tranformation-markdown"
    VALIDATION = "marly/validation"
    RELEVANT_PAGE_FINDER = "marly/relevant-page-finder"
