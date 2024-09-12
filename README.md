# Marly

A schema based document retrieval service for agents that leverages language models for the structuring of unstructured data. Our goal is to help you structure relevant data points from documents like a PDF, PPT or WRD doc from any source for usage in any downstream agentic operation (i.e loading into a database, making an API call, provide context for complex RAG) without the use of a vector database etc). With Marly, you can extract any number of schemas from any number of documents with a single API call making it easier to building Mult-Step Agent Workflows with Unstructured Documents.

# What is a Schema?
---
A set of key-value pairs that describe what needs to be extracted from a particular document (JSON).
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

# How to Run
---
Build the Platform from Source
```
docker-compose up --build
```

Try it out by running one of the example_scripts from the example_scripts folder :)
