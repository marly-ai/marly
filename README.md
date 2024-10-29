<div align="center">

# Marly

[![PyPI version](https://img.shields.io/pypi/v/marly.svg)](https://pypi.org/project/marly/) [![Discord](https://img.shields.io/discord/1273126849261736011.svg?label=Discord&logo=discord)](https://discord.com/channels/1273126849261736011)

[Features](#-features) • [What is a Schema?](#-what-is-a-schema) • [Use Cases](#-use-cases) • [Getting Started](#-getting-started) • [Documentation](#-documentation)

</div>

---

Marly allows your agents to extract tables & text from your PDFs, Powerpoints, websites, etc in a structured format making it easy for them to take subsequent actions (database call, API call, creating a chart etc).

<img src="https://github.com/noaheggenschwiler/images/blob/main/marly-diagram-docs.png?raw=true" alt="Marly Logo">

---

## 🚀 Features

- 📄 Give your agents the ability to find whats relevant from large documents and websites, extract it and get it back in JSON with a single API call.
- 🔍 Extract data based on multiple schemas from numerous documents without a vector database or specifying page numbers
- 🔄 Built-in caching to enable instant retrieval of previously extracted schemas, allowing for rapid repeat extractions without reprocessing the original documents.

---

## 🧰 What is a Schema?

A schema is a set of key-value pairs describing what needs to be extracted from a particular document (JSON format).

<details>
<summary>📋 Example Schema</summary>

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

</details>

</details>

---

## 🎯 Use Cases

<table>
  <tr>
    <td align="center"><b>💼 Financial Report Analysis</b></td>
    <td align="center"><b>📊 Customer Feedback Processing</b></td>
    <td align="center"><b>🔬 Research Assistant</b></td>
    <td align="center"><b>🧠 Legal Contract Parsing</b></td>
  </tr>
  <tr>
    <td>Extract key financial metrics from quarterly PDF reports</td>
    <td>Categorize feedback from various document types</td>
    <td>Process research papers, extracting methodologies and findings</td>
    <td>Extract key legal terms and conditions from contracts</td>
  </tr>
</table>

---

## 🛠️ Getting Started

### Install the Python Package

---

To install the python package, run the following command:

```
pip install marly
```

---

### Build the Platform

---

To build the platform from source, run the following command:

```
docker-compose up --build
```

---

### Run an Example Extraction or Notebook

1. Navigate to the example scripts/example notebooks folder:

   ```bash
   cd example_scripts
   ```

   or

   ```bash
   cd example_notebooks
   ```

2. Run the example extraction script:
   ```bash
   python azure_example.py
   ```

---

## 📚 Documentation

For more detailed information, please refer to our [documentation](https://docs.marly.ai).

---

<div align="left">

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://docs.marly.ai/contribute/contribute) for more details.

## 📄 License

This project is licensed under the [MIT License](https://opensource.org/license/mit).

</div>
