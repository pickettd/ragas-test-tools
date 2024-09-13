from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from langserve import add_routes
from langchain.schema.runnable import RunnableLambda
import numpy as np



from ragas.metrics import faithfulness, answer_relevancy, context_recall, context_precision
from ragas.integrations.langchain import EvaluatorChain
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_ollama import OllamaEmbeddings
from langchain_ollama import ChatOllama

app = FastAPI()

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


@app.get("/")
async def redirect_root_to_docs():
    return RedirectResponse("/docs")

def prepare_ansRel(output: dict):
    fieldStr = "answer_relevancy"
    returnObj = {}
    returnObj[fieldStr] = output[fieldStr]
    try:
        obj = output[fieldStr]
        if isinstance(obj, np.float64):
            print("it is np.float64")
            returnObj[fieldStr] = obj.item()
        return returnObj
    except Exception as e:
        print("An error occurred preparing output: %s", str(e))


# Edit this to add the chain you want to add
add_routes(
    app,
    EvaluatorChain(metric=faithfulness,llm=llm),
    path="/ragas-faithfulness",
)
add_routes(
    app,
    EvaluatorChain(metric=answer_relevancy,llm=llm) | RunnableLambda(prepare_ansRel),
    path="/ragas-answer-relevancy",
)
add_routes(
    app,
    EvaluatorChain(metric=context_precision,llm=llm),
    path="/ragas-context-precision",
)
add_routes(
    app,
    EvaluatorChain(metric=context_recall,llm=llm),
    path="/ragas-context-recall",
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
