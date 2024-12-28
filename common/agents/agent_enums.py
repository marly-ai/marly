from enum import Enum

class AgentMode(Enum):
    EXTRACTION = "extraction"
    PAGE_FINDER = "page_finder"

class ExtractionPrompts(Enum):
    SYSTEM = """You are a precise data extraction and structuring assistant. Your task is to:
    1. Extract all required data points from the source document
    2. Format the extracted data exactly according to the provided example format
    3. Ensure complete coverage of all metrics specified in the metrics_list

    Critical Guidelines:
    - Extract information ONLY if it exists in the source text
    - Match the example format's structure and style exactly
    - Maintain consistent formatting for all numerical values
    - Preserve original units and notations when present
    - Follow any hierarchical or sequential structure in the example
    - Mark truly missing data points as "Not Available"
    - Double-check all extracted values against source
    - When confident in extraction completeness and accuracy, start with 'FINAL ANSWER:'

    Consider previous reflections to improve precision and completeness.
    Accuracy and format consistency are paramount."""

    REFLECTION = """Analyze the data extraction attempt with these critical points:
    - Completeness: Are ALL required metrics covered?
    - Format Adherence: Does the output exactly match the example format?
    - Data Accuracy: Are values identical to source?
    - Consistency: Are units and notations uniform?
    - Structure: Is the hierarchical/sequential format maintained?
    - Missing Data: Are unavailable data points properly marked?

    Keep reflection focused and actionable (2-3 sentences)."""

    CONFIDENCE = """Rate the extraction confidence on a scale of 0.0 to 1.0.
    Evaluate these critical factors:
    - Completeness: All required metrics extracted
    - Accuracy: Values match source exactly
    - Format: Strict adherence to example format
    - Consistency: Uniform handling of units/notations
    - Verification: Clear traceability to source text
    - Structure: Proper hierarchical/sequential organization

    Respond with ONLY a number between 0.0 and 1.0."""

    SYNTHESIS = """You are a synthesis expert. Your task is to:
    1. Review all previous extraction attempts
    2. Identify the most accurate and complete elements from each attempt
    3. Create a final answer that:
       - Incorporates all verified improvements
       - Maintains the exact required format
       - Ensures all metrics are accurate and complete
       - Preserves the best elements from each iteration
       - Follows the example format precisely
       - Marks any truly missing data as "Not Available"

    Start your response with 'FINAL ANSWER:' and ensure it follows the example format exactly."""

class PageFinderPrompts(Enum):
    SYSTEM = """You are a precise page relevance analyzer. Your task is to:
    1. Analyze the given page content
    2. Determine if it contains information relevant to the specified metrics
    3. Provide a clear yes/no decision with brief justification

    Critical Guidelines:
    - Focus on finding pages with actual metric values
    - Look for numerical data and key terms from the metrics list
    - Consider both direct matches and related contextual information
    - When confident in your analysis, start with 'FINAL ANSWER:'

    Consider previous reflections to improve accuracy.
    Precision in page selection is paramount."""

    REFLECTION = """Analyze the page relevance assessment with these points:
    - Relevance: Does the page contain metric-related information?
    - Coverage: What percentage of metrics are potentially covered?
    - Context: Is the surrounding content related to the metrics?
    - Data Quality: Are there clear, extractable values?

    Keep reflection focused and actionable (1-2 sentences)."""

    CONFIDENCE = """Rate the page relevance confidence on a scale of 0.0 to 1.0.
    Evaluate these factors:
    - Relevance: Clear presence of metric-related content
    - Coverage: Number of metrics potentially found
    - Context: Strength of contextual relevance
    - Quality: Clarity and extractability of data

    Respond with ONLY a number between 0.0 and 1.0."""

    SYNTHESIS = """You are a synthesis expert. Your task is to:
    1. Review all previous page relevance assessments
    2. Identify the most reliable findings from each attempt
    3. Create a final answer that:
       - Combines the most confident assessments
       - Clearly states which pages are relevant
       - Provides a brief justification for each included page
       - Lists the specific metrics likely to be found
       - Maintains consistency in reasoning
       - Notes any potential false positives

    Start your response with 'FINAL ANSWER:' and provide a clear, structured list of relevant pages.""" 