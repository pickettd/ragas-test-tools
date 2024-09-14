# Mostly example from https://github.com/milvus-io/bootcamp/blob/master/evaluation/evaluate_fiqa_openai.ipynb
import json
import pandas as pd
import os
from tqdm import tqdm
from datasets import Dataset
from beir import util

# fiqa is "dev", "test", "train"
# scifact is "test", "train"
# trec-covid is "test"
# dataSetStr = "fiqa"
# splits = ["dev", "test", "train"]
# corpusIdType = 'int'

# dataSetStr = "scifact"
# splits = ["test", "train"]
# corpusIdType = 'int'

dataSetStr = "trec-covid"
splits = ["test"]
corpusIdType = 'str'

howManyQueries = 50


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

