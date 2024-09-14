// Import required modules
const fs = require("fs");
const path = require("path");
const axios = require("axios");
const AdmZip = require("adm-zip");

// Set dataset parameters
const dataSetStr = "trec-covid";
const splits = ["test"];
const corpusIdType = "str";

const howManyQueries = 50;

// Function to download and unzip a file
async function downloadAndUnzip(url, targetPath) {
  const response = await axios.get(url, { responseType: "arraybuffer" });
  const zip = new AdmZip(response.data);
  zip.extractAllTo(targetPath, true);
}

// Function to prepare dataset without answer
async function prepareDatasetWithoutAnswer(knowledgePath) {
  const datasetName = dataSetStr;
  const zipPath = path.join(knowledgePath, `${datasetName}.zip`);

  // Download and unzip the dataset if it doesn't exist
  if (!fs.existsSync(zipPath)) {
    const url = `https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/${datasetName}.zip`;
    await downloadAndUnzip(url, knowledgePath);
  }

  const dataPath = path.join(knowledgePath, dataSetStr);

  // Read and process corpus data
  const corpusData = fs.readFileSync(
    path.join(dataPath, "corpus.jsonl"),
    "utf-8"
  );
  const corpusDf = corpusData
    .split("\n")
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line))
    .map(({ _id, text }) => ({ "corpus-id": _id, ground_truth: text }));

  // Read and process queries data
  const queriesData = fs.readFileSync(
    path.join(dataPath, "queries.jsonl"),
    "utf-8"
  );
  const queriesDf = queriesData
    .split("\n")
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line))
    .map(({ _id, text }) => ({ "query-id": parseInt(_id), question: text }));

  // Process split data
  const splitDf = {};
  for (const split of splits) {
    const splitData = fs.readFileSync(
      path.join(dataPath, `qrels/${split}.tsv`),
      "utf-8"
    );
    splitDf[split] = splitData
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => {
        const [query_id, _, corpus_id] = line.split("\t");
        return { "query-id": parseInt(query_id), "corpus-id": corpus_id };
      });
  }

  // Merge and group data
  const finalSplitDf = {};
  for (const split of splits) {
    const mergedData = splitDf[split].map((item) => {
      const query = queriesDf.find((q) => q["query-id"] === item["query-id"]);
      const corpus = corpusDf.find((c) => c["corpus-id"] === item["corpus-id"]);
      return { ...item, ...query, ...corpus };
    });

    const groupedData = Object.values(
      mergedData.reduce((acc, item) => {
        if (!acc[item["query-id"]]) {
          acc[item["query-id"]] = {
            "query-id": item["query-id"],
            question: item.question,
            ground_truths: [],
          };
        }
        acc[item["query-id"]].ground_truths.push(item.ground_truth);
        return acc;
      }, {})
    );

    finalSplitDf[split] = groupedData;
  }

  return finalSplitDf;
}

// Main execution
async function main() {
  const knowledgeDataPath = "./knowledge_datas";
  const txtDocPath = path.join(knowledgeDataPath, `${dataSetStr}_doc.txt`);

  if (!fs.existsSync(knowledgeDataPath)) {
    fs.mkdirSync(knowledgeDataPath);
  }

  const finalSplitDf = await prepareDatasetWithoutAnswer(knowledgeDataPath);

  const docs = finalSplitDf["test"].flatMap((item) => item.ground_truths);

  console.log(`Length of ${dataSetStr} dataset docs:`);
  console.log(docs.length);

  const docsStr = docs.join("\n");
  fs.writeFileSync(txtDocPath, docsStr, "utf-8");

  const questionList = finalSplitDf["test"].map((item) => item.question);
  const groundTruthList = finalSplitDf["test"].map(
    (item) => item.ground_truths
  );

  questionList = questionList.slice(0, howManyQueries - 1);
  groundTruthList = groundTruthList.slice(0, howManyQueries - 1);

  console.log("howManyQueries Questions:", questionList);
  console.log("howManyQueries Ground Truths:", groundTruthList);
}

main().catch(console.error);
