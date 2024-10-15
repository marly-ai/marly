from llama_index.readers.web import SimpleWebPageReader
from llama_index.core import VectorStoreIndex
from llama_index.vector_stores.pinecone import PineconeVectorStore
from llama_index.core.storage.storage_context import StorageContext
from llama_index.core.indices.postprocessor import MetadataReplacementPostProcessor
from llama_index.core.settings import Settings
from llama_index.core import SimpleDirectoryReader
from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding
from llama_index.core import Settings
import os

load_dotenv()

SCHEMA = {
    "Names": "The first name and last name of the company founders",
    "Company Name": "Name of the Company",
    "Round": "The round of funding",
    "Round Size": "How much money has the company raised",
    "Investors": "The names of the investors in the companies (names of investors and firms)",
    "Company Valuation": "The current valuation of the company",
    "Summary": "Three sentence summary of the company"
}

SCHEMA_2 = {
    "Firm": "The name of the firm",
    "Number of Funds": "The number of funds managed by the firm",
    "Commitment": "The commitment amount in millions of dollars",
    "Percent of Total Comm": "The percentage of total commitment",
    "Exposure (FMV + Unfunded)": "The exposure including fair market value and unfunded commitments in millions of dollars",
    "Percent of Total Exposure": "The percentage of total exposure",
    "TVPI": "Total Value to Paid-In multiple",
    "Net IRR": "Net Internal Rate of Return as a percentage"
}

class LlamaIndexAzure:
    def __init__(self):
        self.llm = AzureOpenAI (
            model=os.environ["AZURE_MODEL_NAME"],
            deployment_name=os.environ["AZURE_DEPLOYMENT_ID"],
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            azure_endpoint=os.environ["AZURE_ENDPOINT"],
            api_version=os.environ["AZURE_API_VERSION"],
        )

        self.embed_model = AzureOpenAIEmbedding (
            model=os.environ["EMBED_MODEL_NAME"],
            deployment_name=os.environ["EMBED_DEPLOYMENT_NAME"],
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            azure_endpoint=os.environ["EMBED_AZURE_ENDPOINT"],
            api_version=os.environ["EMBED_API_VERSION"],
        )
        Settings.llm = self.llm
        Settings.embed_model = self.embed_model

llamaindex_azure_llm = LlamaIndexAzure().llm
llamaindex_azure_embed = LlamaIndexAzure().embed_model
Settings.llm = llamaindex_azure_llm
Settings.embed_model = llamaindex_azure_embed

def ask_question(question):
    index = pinecone_setup()
    query_engine = index.as_query_engine(
        similarity_top_k=9,
        node_postprocessors=[
            MetadataReplacementPostProcessor(target_metadata_key="window")
        ],
    )
    
    return query_engine.query(question)

def pinecone_setup():
    print("Setting up Pinecone...")
    pc = Pinecone(
        api_key=os.environ.get("PINECONE_API_KEY"),
        environment="gcp-starter"
    )
    index_name = "example"
    if index_name not in pc.list_indexes().names():
        print("Creating new Pinecone index")
        load_docs_into_pinecone(pc,index_name=index_name)
        print("Pinecone setup completed and index created")
    
    print("Index already in the environment")
    pinecone_index = pc.Index(index_name)
    vector_store = PineconeVectorStore(pinecone_index=pinecone_index)
    index = VectorStoreIndex.from_vector_store(vector_store=vector_store)

    return index

def load_docs_into_pinecone(pc, index_name):
    WEBSITE_URL = "https://techcrunch.com/2013/02/08/snapchat-raises-13-5m-series-a-led-by-benchmark-now-sees-60m-snaps-sent-per-day/"
    WEBSITE_URL2 = "https://techcrunch.com/2024/08/09/anysphere-a-github-copilot-rival-has-raised-60m-series-a-at-400m-valuation-from-a16z-thrive-sources-say/"


    documents = SimpleWebPageReader().load_data(
        [WEBSITE_URL, WEBSITE_URL2]
    )

    print(f"Loaded {len(documents)} document(s)")

    reader = SimpleDirectoryReader(input_files=["./lacers_reduced.pdf"])
    documents2 = reader.load_data()

    print(f"Loaded {len(documents2)} document(s)")

    pc.create_index(
        name=index_name,
        dimension=1536,
        metric='euclidean',
        spec=ServerlessSpec(
            cloud="aws",
            region="us-east-1",
        )
    )


    pc = Pinecone(
        api_key=os.environ.get("PINECONE_API_KEY"),
        environment="gcp-starter"
    )

    pinecone_index = pc.Index(index_name)

    vector_store = PineconeVectorStore(pinecone_index=pinecone_index)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    VectorStoreIndex.from_documents(documents, storage_context=storage_context)
    VectorStoreIndex.from_documents(documents2, storage_context=storage_context)

if __name__ == "__main__":
    question1 = f"Extract values for {SCHEMA} for every company and extract {SCHEMA_2} for the 10 Largest Sponsor Relationships then return the result as JSON. Please include all the data!"
    answer = ask_question(question1)
    print("Answer:", answer)