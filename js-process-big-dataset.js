import * as readline from "node:readline";
import * as fs from "node:fs";
//import * as csv from "csv";
import { parse } from "csv-parse/sync";

// fiqa is "dev", "test", "train"
// scifact is "test", "train"
// trec-covid is "test"

// dataSetStr = "fiqa"
// splits = ["dev", "test", "train"]
// corpusIdType = 'int'
// assistantEnvVarStr = "OPENAI_FIQAT_ASSISTANT_ID"

// dataSetStr = "scifact"
// splits = ["test", "train"]
// corpusIdType = 'int'
// assistantEnvVarStr = "OPENAI_SCIFACT_ASSISTANT_ID"

const dataSetStr = "trec-covid";
const splitFileStr = "test";
const corpusIdType = "str";
const idFieldStr = "_id";

const howManyQueries = 1;

const knowledge_datas_path = "./knowledge_datas/";
const corpusFilePath = knowledge_datas_path + dataSetStr + "/corpus.jsonl";
const queryFilePath = knowledge_datas_path + dataSetStr + "/queries.jsonl";
const splitFilePath =
  knowledge_datas_path + dataSetStr + "/qrels/" + splitFileStr + ".tsv";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  // create a readline interface for reading the file line by line
  const rl = readline.createInterface({
    input: fs.createReadStream(corpusFilePath),
    crlfDelay: Infinity,
  });

  // create an array to hold the parsed JSON objects
  const corpusJsonArray = [];
  const corpusJsonMap = {};
  let isCorpusDone = false;

  // read each line of the file and parse it as JSON
  rl.on("line", (line) => {
    const obj = JSON.parse(line);
    corpusJsonArray.push(obj);
    corpusJsonMap[obj[idFieldStr]] = obj;
  });

  // log the parsed JSON objects once the file has been fully read
  rl.on("close", () => {
    console.log("parsed corpus array length is", corpusJsonArray.length);
    console.log(
      "first corpus parsed array element id is",
      corpusJsonArray[0][idFieldStr]
    );
    isCorpusDone = true;
  });
  // create a readline interface for reading the file line by line
  const rlQueries = readline.createInterface({
    input: fs.createReadStream(queryFilePath),
    crlfDelay: Infinity,
  });

  // create an array to hold the parsed JSON objects
  const queryJsonArray = [];
  const queryJsonMap = {};
  let isQueryDone = false;

  // read each line of the file and parse it as JSON
  rlQueries.on("line", (line) => {
    const obj = JSON.parse(line);
    queryJsonArray.push(obj);
    queryJsonMap[obj[idFieldStr]] = obj;
  });

  // log the parsed JSON objects once the file has been fully read
  rlQueries.on("close", () => {
    console.log("parsed query array length is", queryJsonArray.length);
    console.log(
      "first query parsed array element id is",
      queryJsonArray[0][idFieldStr]
    );
    isQueryDone = true;
  });

  // Read the split file
  const splitFileData = fs.readFileSync(splitFilePath, "utf8");
  const parsedSplitFile = parse(splitFileData, {
    columns: true,
    skip_empty_lines: true,
    delimiter: "\t",
  });
  // const parsedSplitFile = parse(splitFileData, {
  //   columns: true,
  //   skip_empty_lines: true,
  // });
  // parse the TSV
  console.log("Parsed the TSV file. Length is", parsedSplitFile.length);
  //console.log("First TSV entry is", parsedSplitFile[0]);

  const queriesToRun = [];
  for (let a = 0; a < howManyQueries; a++) {
    queriesToRun.push(parsedSplitFile[a]);
  }
  console.log("Finished getting queriesToRun. Length is", queriesToRun.length);
  console.log("First QtR entry is", queriesToRun[0]);

  if (isCorpusDone && isQueryDone) {
    console.log("Ready to build dataset");
  } else {
    await sleep(2000);
    if (isCorpusDone && isQueryDone) {
      console.log("After sleep ready to build dataset");
    }
  }
}
main();
