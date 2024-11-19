<div align="center">

# Marly

[![PyPI version](https://img.shields.io/pypi/v/marly.svg)](https://pypi.org/project/marly/) [![Discord](https://img.shields.io/discord/1273126849261736011.svg?label=Discord&logo=discord)](https://discord.com/channels/1273126849261736011)

[Features](#-features) • [What is a Schema?](#-what-is-a-schema) • [Use Cases](#-use-cases) • [Getting Started](#-getting-started) • [Documentation](#-documentation)

</div>

---

Marly allows you to search and extract context specific data from your PDFs, WRDs, Powerpoints, Websites etc in a structured format like JSON or Markdown.

<img src="https://github.com/noaheggenschwiler/images/blob/main/updated-marly-diagram.png?raw=true" alt="Marly Logo">

---

## 🚀 Features

📄 Extract Relevant Information Seamlessly: Give your applications the ability to identify and extract relevant data from large documents or websites with just a single API call. Retrieve content in JSON or Markdown formats, making it easy to integrate into your workflows.

🔍 Multi-Schema Support: Extract data based on multiple predefined schemas from a variety of document types, without needing a vector database or specifying page numbers. Flexibly tailor your extraction to meet your specific needs.

🔄 Built-in Caching: With built-in caching, previously extracted schemas can be instantly retrieved, enabling rapid repeat extractions without having to reprocess the original documents.

🚫 No Vendor Lock-In: Enjoy complete flexibility with your choice of model provider. Whether using open-source or closed-source models, you're never tied to a specific vendor, ensuring full control.

---

## 🧰 What is a Schema?

A schema is a set of key-value pairs describing what needs to be extracted from a particular document.

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

```bash
./start-marly.sh
```

---

### Run an example script or notebook

Once the Marly platform is running you can test it out by trying one of our examples

1. Navigate to the examples folder:

   ```bash
   cd examples
   ```
2. Navigate to the scripts or notebooks folder:

   ```bash
   cd scripts
   ```
   or
   ```bash
   cd notebooks/autogen_example
   ```
3. Run one of our example scripts:
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

This project is licensed under the [Elastic License 2.0 (ELv2)](https://www.elastic.co/licensing/elastic-license).

</div>
