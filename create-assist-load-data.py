# Mostly example from https://github.com/milvus-io/bootcamp/blob/master/evaluation/evaluate_fiqa_openai.ipynb
import time
from openai import OpenAI
import json
import pandas as pd
import os
from tqdm import tqdm
from datasets import Dataset
from beir import util

from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()
# IMPORTANT: Remember to create a .env variable containing: OPENAI_API_KEY=sk-xyz where xyz is your key

#dataSetStr = "fiqa"

# dataSetStr = "scifact"

dataSetStr = "trec-covid"

theAssistantName = "RAGAS-"+dataSetStr


client = OpenAI()

# Set OPENAI_API_KEY in your environment value
client.api_key = os.getenv('OPENAI_API_KEY')


class OpenAITimeoutException(Exception):
    pass

knowledge_datas_path = './knowledge_datas'
txt_doc_path = os.path.join(knowledge_datas_path, dataSetStr+'_doc.txt')

# looks outdated
# file = client.files.create(
#     file=open(fiqa_path, "rb"),
#     purpose='assistants'
# )

# Create a vector store caled "Financial Statements"
vector_store = client.beta.vector_stores.create(name=dataSetStr+"_doc")
 
# Ready the files for upload to OpenAI
file_paths = [txt_doc_path]
file_streams = [open(path, "rb") for path in file_paths]
 
# Use the upload and poll SDK helper to upload the files, add them to the vector store,
# and poll the status of the file batch for completion.
file_batch = client.beta.vector_stores.file_batches.upload_and_poll(
  vector_store_id=vector_store.id, files=file_streams
)
 
# You can print the status and the file counts of the batch to see the result of this operation.
print(file_batch.status)
print(file_batch.file_counts)

# Add the file to the assistant
assistant = client.beta.assistants.create(
    instructions="You are a customer support chatbot. You must use your file_search tool to retrieve relevant knowledge to best respond to customer queries.",
    model="gpt-4o-mini",
    name= theAssistantName,
    tools=[{"type": "file_search"}],
    tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}}
)
print('Assistant created')
print('Named: '+theAssistantName)
print(assistant.id)