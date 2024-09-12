# Marly

Marly is a schema-based document retrieval service designed to help you efficiently structure data from various documents like PDFs, PowerPoints, or Word files. By leveraging language models, Marly transforms unstructured data into organized, structured information that you can easily use in any downstream operation, such as loading into a database, making API calls, or providing context for complex Q&A tasks in your agentic workflows. All this is done without needing a vector database. With Marly, you can extract data based on multiple schemas from numerous documents with just one API call, simplifying the process of building multi-step workflows with unstructured documents.

# What is a Schema?
---
A set of key-value pairs that describes what needs to be extracted from a particular document (JSON).
```
{
    "Firm": "The name of the firm",
    "Number of Funds": "The number of funds managed by the firm",
    "Commitment": "The commitment amount in millions of dollars",
    "% of Total Comm": "The percentage of total commitment",
    "Exposure (FMV + Unfunded)": "The exposure including fair market value and unfunded commitments in millions of dollars",
    "% of Total Exposure": "The percentage of total exposure",
    "TVPI": "Total Value to Paid-In multiple",
    "Net IRR": "Net Internal Rate of Return as a percentage"
}
```

# Example Usecases
---
Financial Report Analysis Agent
Marly processes quarterly PDF reports, extracting key financial metrics using a predefined schema. This structured data enables the agent to easily generate comparative analyses and populate financial databases without manual data entry.

Customer Feedback Processing Agent
Marly extracts relevant fields from various document types (Word, emails, surveys) using a customer feedback schema. This streamlines the agent's ability to categorize feedback, update CRM databases, and generate product team reports automatically.

Research Assistant Agent
Marly processes research papers and reports, extracting methodologies, findings, and conclusions based on a research-oriented schema. This structured information forms a searchable knowledge base that can be loaded into any database, allowing the agent to efficiently answer complex research questions and generate comprehensive summaries.


# How do you run the service? 
---
Build the Platform from Source
```
docker-compose up --build
```

Try it out by running one of the example_scripts from the example_scripts folder :)
