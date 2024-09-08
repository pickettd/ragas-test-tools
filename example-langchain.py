# This code mostly from https://docs.ragas.io/en/latest/howtos/integrations/langchain.html
# with modifications/tweaks for current package versions
import nest_asyncio
import os
from pathlib import Path
import openai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
# IMPORTANT: Remember to create a .env variable containing: OPENAI_API_KEY=sk-xyz where xyz is your key

# Access the API key from the environment variable
api_key = os.environ.get("OPENAI_API_KEY")

# Initialize the OpenAI API client
openai.api_key = api_key

nest_asyncio.apply()

from langchain_community.document_loaders import TextLoader
from langchain.indexes import VectorstoreIndexCreator
from langchain_community.vectorstores.inmemory import InMemoryVectorStore
from langchain.chains import RetrievalQA
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings

# from langchain_openai import OpenAIEmbeddings
# embeddings = OpenAIEmbeddings()

from langchain_ollama import OllamaEmbeddings
from langchain_ollama import ChatOllama

useAllOpenAI = True
chatModelName = "gpt-4o-mini"
embedModelName = "text-embedding-3-small"

embeddings = OpenAIEmbeddings(
    model=embedModelName,
)
llm = ChatOpenAI(model=chatModelName,temperature=0)

if (useAllOpenAI):
    chatModelName = chatModelName
    embedModelName = embedModelName
else:
    chatModelName = "llama3.1:8b-instruct-q4_K_M"
    embedModelName = "nomic-embed-text:latest"
    llm = ChatOllama(model=chatModelName,temperature=0)
    embeddings = OllamaEmbeddings(
        model=embedModelName,
    )


loader = TextLoader(Path("nyc_wikipedia/nyc_text.txt"), autodetect_encoding=True)
index = VectorstoreIndexCreator(embedding=embeddings,vectorstore_cls=InMemoryVectorStore).from_loaders([loader])


# llm = ChatOllama(model="llama3.1:8b-instruct-q4_K_M",temperature=0)
qa_chain = RetrievalQA.from_chain_type(
    llm,
    retriever=index.vectorstore.as_retriever(),
    return_source_documents=True,
)

question = "How did New York City get its name?"
# Just send one question like normal langchain RAG
#result = qa_chain({"query": question})

eval_questions = [
    "What is the population of New York City as of 2020?",
    "Which borough of New York City has the highest population?",
    "What is the economic significance of New York City?",
    "How did New York City get its name?",
    "What is the significance of the Statue of Liberty in New York City?",
]

eval_answers = [
    "8,804,190",
    "Brooklyn",
    "New York City's economic significance is vast, as it serves as the global financial capital, housing Wall Street and major financial institutions. Its diverse economy spans technology, media, healthcare, education, and more, making it resilient to economic fluctuations. NYC is a hub for international business, attracting global companies, and boasts a large, skilled labor force. Its real estate market, tourism, cultural industries, and educational institutions further fuel its economic prowess. The city's transportation network and global influence amplify its impact on the world stage, solidifying its status as a vital economic player and cultural epicenter.",
    "New York City got its name when it came under British control in 1664. King Charles II of England granted the lands to his brother, the Duke of York, who named the city New York in his own honor.",
    "The Statue of Liberty in New York City holds great significance as a symbol of the United States and its ideals of liberty and peace. It greeted millions of immigrants who arrived in the U.S. by ship in the late 19th and early 20th centuries, representing hope and freedom for those seeking a better life. It has since become an iconic landmark and a global symbol of cultural diversity and freedom.",
]

examples = [
    {"query": q, "ground_truth": [eval_answers[i]]}
    for i, q in enumerate(eval_questions)
]

# Can again, send just one question but using our eval_questions structure
# result = qa_chain({"query": eval_questions[1]})

#Can send an example question with ground_truth
groundTruthIndex = 4
result = qa_chain.invoke(examples[groundTruthIndex])
# result["result"]
print("Using LLM:")
print(chatModelName)
print("Using embeddings:")
print(embedModelName)
print("\n")
print("Question: ")
print(examples[groundTruthIndex]["query"])
print("LLM Answer:")
print(result["result"])
print("\n")

# From Github https://github.com/explodinggradients/ragas/issues/571
from ragas.metrics import faithfulness, answer_relevancy, context_recall, context_precision
from ragas.integrations.langchain import EvaluatorChain

# make eval chains (fancy))
# eval_chains = {
#     m.name: EvaluatorChain(metric=m) 
#     for m in [faithfulness, answer_relevancy, context_precision, context_recall]
# }

# Recheck the result that we are going to validate.
# result
# print(result)

# Restructure the result to match the expected input format for the faithfulness chain
restructured_result = {
    "question": result["query"],
    "answer": result["result"],
    "contexts": [doc.page_content for doc in result["source_documents"]],
    "ground_truth": result["ground_truth"]
}

faithfulness_chain = EvaluatorChain(metric=faithfulness,llm=llm)
eval_result = faithfulness_chain.invoke(restructured_result)
# print (eval_result)
eval_result["faithfulness"]
print("faithfulness: ")
print(eval_result["faithfulness"])

answer_rel_chain = EvaluatorChain(metric=answer_relevancy,llm=llm)
eval_result = answer_rel_chain.invoke(restructured_result)
# print (eval_result)
eval_result["answer_relevancy"]
print("answer_relevancy: ")
print(eval_result["answer_relevancy"])

context_rel_chain = EvaluatorChain(metric=context_precision,llm=llm)
eval_result = context_rel_chain.invoke(restructured_result)
# print (eval_result)
eval_result["context_precision"]
print("context_precision: ")
print(eval_result["context_precision"])

context_recall_chain = EvaluatorChain(metric=context_recall,llm=llm)
eval_result = context_recall_chain.invoke(restructured_result)
# print (eval_result)
eval_result["context_recall"]
print("context_recall: ")
print(eval_result["context_recall"])
