# Mostly example from https://github.com/milvus-io/bootcamp/blob/master/evaluation/evaluate_fiqa_openai.ipynb
# Note that this will use a very large number of tokens (like 13-20 million) and can be costly
import time
from openai import OpenAI
import json
import pandas as pd
import os
from tqdm import tqdm
from datasets import Dataset
from beir import util

from ragas import evaluate
from ragas.metrics import answer_relevancy, faithfulness, context_recall, context_precision, answer_similarity

from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# IMPORTANT: Remember to create a .env variable containing: OPENAI_API_KEY=sk-xyz where xyz is your key

# fiqa is "dev", "test", "train"
# scifact is "test", "train"
# trec-covid is "test"

# dataSetStr = "fiqa"
# splits = ["dev", "test", "train"]
# corpusIdType = 'int'
# assistantEnvVarStr = "OPENAI_FIQAT_ASSISTANT_ID"

# dataSetStr = "scifact"
# splits = ["test", "train"]
# corpusIdType = 'int'
# assistantEnvVarStr = "OPENAI_SCIFACT_ASSISTANT_ID"

dataSetStr = "trec-covid"
splits = ["test"]
corpusIdType = 'str'
assistantEnvVarStr = "OPENAI_TREC_COVID_ASSISTANT_ID"

howManyQueries = 50


client = OpenAI()

# Set OPENAI_API_KEY in your environment value
client.api_key = os.getenv('OPENAI_API_KEY')

assistant_id = os.getenv(assistantEnvVarStr)
theAssistantName = "RAGAS-"+dataSetStr




class OpenAITimeoutException(Exception):
    pass


def get_content_from_retrieved_message(message):
    # Extract the message content
    message_content = message.content[0].text
    annotations = message_content.annotations
    contexts = []
    for annotation in annotations:
        message_content.value = message_content.value.replace(annotation.text, f'')
        if (file_citation := getattr(annotation, 'file_citation', None)):
            contexts.append(file_citation.quote)
    if len(contexts) == 0:
        contexts = ['empty context.']
    return message_content.value, contexts


def try_get_answer_contexts(assistant_id, question, timeout_seconds=120):
    thread = client.beta.threads.create(
        messages=[
            {
                "role": "user",
                "content": question,
            }
        ]
    )
    thread_id = thread.id
    run = client.beta.threads.runs.create(
        thread_id=thread_id,
        assistant_id=assistant_id,
    )
    start_time = time.time()
    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            raise Exception("OpenAI retrieving answer Timeout！")

        run = client.beta.threads.runs.retrieve(
            thread_id=thread_id,
            run_id=run.id
        )
        if run.status == 'completed':
            break
    messages = client.beta.threads.messages.list(
        thread_id=thread_id
    )
    assert len(messages.data) > 1
    res, contexts = get_content_from_retrieved_message(messages.data[0])
    response = client.beta.threads.delete(thread_id)
    assert response.deleted is True
    return contexts, res


def get_answer_contexts_from_assistant(question, assistant_id, timeout_seconds=120, retry_num=6):
    res = 'failed. please retry.'
    contexts = ['failed. please retry.']
    try:
        for _ in range(retry_num):
            try:
                contexts, res = try_get_answer_contexts(assistant_id, question, timeout_seconds)
                break
            except OpenAITimeoutException as e:
                print('OpenAI retrieving answer Timeout, retry...')
                continue
    except Exception as e:
        print(e)
    return res, contexts

def prepare_dataset_without_answer(knowledge_path, max_items_per_split=None):
    dataset_name = dataSetStr

    if not os.path.exists(os.path.join(knowledge_path, f'{dataset_name}.zip')):
        url = (
            "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip".format(
                dataset_name
            )
        )
        util.download_and_unzip(url, knowledge_path)

    data_path = os.path.join(knowledge_path, dataSetStr)
    with open(os.path.join(data_path, "corpus.jsonl")) as f:
        cs = [pd.Series(json.loads(l)) for l in f.readlines()]

    corpus_df = pd.DataFrame(cs)

    corpus_df = corpus_df.rename(columns={"_id": "corpus-id", "text": "ground_truth"})
    corpus_df = corpus_df.drop(columns=["title", "metadata"])
    corpus_df["corpus-id"] = corpus_df["corpus-id"].astype(corpusIdType)
    corpus_df.head()

    with open(os.path.join(data_path, "queries.jsonl")) as f:
        qs = [pd.Series(json.loads(l)) for l in f.readlines()]

    queries_df = pd.DataFrame(qs)
    queries_df = queries_df.rename(columns={"_id": "query-id", "text": "question"})
    queries_df = queries_df.drop(columns=["metadata"])
    queries_df["query-id"] = queries_df["query-id"].astype(int)
    queries_df.head()

    split_df = {}
    for s in splits:
        split_df[s] = pd.read_csv(os.path.join(data_path, f"qrels/{s}.tsv"), sep="\t").drop(
            columns=["score"]
        )

    final_split_df = {}
    for split in split_df:
        df = queries_df.merge(split_df[split], on="query-id")
        df = df.merge(corpus_df, on="corpus-id")
        df = df.drop(columns=["corpus-id"])
        grouped = df.groupby("query-id").apply(
            lambda x: pd.Series(
                {
                    "question": x["question"].sample().values[0],
                    "ground_truths": x["ground_truth"].tolist(),
                }
            ),
            include_groups=False
        )

        if max_items_per_split is not None:
            grouped = grouped.head(max_items_per_split)

        final_split_df[split] = grouped

    return final_split_df

knowledge_datas_path = './knowledge_datas'
txt_doc_path = os.path.join(knowledge_datas_path, dataSetStr+'_doc.txt')

if not os.path.exists(knowledge_datas_path):
    os.mkdir(knowledge_datas_path)
contexts_list = []
answer_list = []

final_split_df = prepare_dataset_without_answer(knowledge_datas_path, max_items_per_split=howManyQueries)

docs = []

split = 'test'
for ds in final_split_df[split]["ground_truths"]:
    docs.extend([d for d in ds])
print("Length of "+dataSetStr+" dataset docs:")
print(len(docs))

docs_str = '\n'.join(docs)
with open(txt_doc_path, 'w') as f:
    f.write("%s"% docs_str.encode('utf-8'))

split = 'test'
question_list = final_split_df[split]["question"].to_list()
ground_truth_list = final_split_df[split]["ground_truths"].to_list()

# Starting the timer for just the generation of answers and eval portion
start_time = time.time()

print("Now generating answers from assistant")
justAskOne = False
if justAskOne:
    question_list = [question_list[0]]
    ground_truth_list = [ground_truth_list[0]]


for question in tqdm(question_list):
    answer, contexts = get_answer_contexts_from_assistant(question, assistant_id)
    # print(f'answer = {answer}')
    # print(f'contexts = {contexts}')
    # print('=' * 80)
    answer_list.append(answer)
    contexts_list.append(contexts)

ds = Dataset.from_dict({"question": question_list,
                        "contexts": contexts_list,
                        "answer": answer_list,
                        "ground_truths": ground_truth_list})

result = evaluate(
    ds,
    metrics=[
        context_precision,
        context_recall,
        faithfulness,
        answer_relevancy,
        # answer_similarity,
        # answer_correctness,
    ],

)
print("Completed running eval on "+theAssistantName+" with dataset: "+ dataSetStr)
print(result)
print("--- Eval run took %.2f seconds ---" % (time.time() - start_time))