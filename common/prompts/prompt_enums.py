from enum import Enum

class PromptType(Enum):
    EXAMPLE_GENERATION = "marly/example-generation"
    EXTRACTION = "marly/extraction"
    TRANSFORMATION = "marly/transformation"
    TRANSFORMATION_MARKDOWN = "noahegg/tranformation-markdown"
    TRANSFORMATION_WEB = "noahegg/web_scraper"
    TRANSFORMATION_ONLY = "marly/transformation-only"
    VALIDATION = "marly/validation"
    RELEVANT_PAGE_FINDER = "marly/relevant-page-finder"
    PLAN = "marly/plan"
    RELEVANT_PAGE_FINDER_V2 = "marly/relevant-page-finder-with-plan"
