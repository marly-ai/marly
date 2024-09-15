from enum import Enum

class PromptType(Enum):
    EXAMPLE_GENERATION = "marly/example-generation"
    EXTRACTION = "marly/extraction"
    TRANSFORMATION = "marly/transformation"
    VALIDATION = "marly/validation"
    RELEVANT_PAGE_FINDER = "marly/relevant-page-finder"
