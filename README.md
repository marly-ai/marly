<div align="center">

# Marly

_The Data Processor for Agents_

[Features](#-features) • [What is a Schema?](#-what-is-a-schema) • [Use Cases](#-use-cases) • [Getting Started](#-getting-started) • [Documentation](#-documentation)

</div>

---

## 🚀 Features

- 📄 Give your agents the ability to transform unstructured data into organized, structured information
- 🔍 Extract data based on multiple schemas from numerous documents with a single API call
- 🔄 Simplify multi-step agentic workflows that use unstructured documents without a vector database

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

### Build the Platform

---

To build the platform from source, run the following command:

```
docker-compose up --build
```

---

### Run an Example Extraction

1. Navigate to the example scripts:

   ```bash
   cd example_scripts
   ```

2. Run the example extraction script:
   ```bash
   python local_example_azure.py
   python local_example_groq.py
   ```

---

## 📚 Documentation

For more detailed information, please refer to our [documentation](https://docs.marly.ai).

---

<div align="left">

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](link-to-contributing-guide) for more details.

## 📄 License

This project is licensed under the [Elastic License 2.0 (ELv2)](https://www.elastic.co/licensing/elastic-license).

</div>
