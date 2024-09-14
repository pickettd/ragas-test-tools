import { ChatOpenAI, OpenAIEmbeddings } from "@langchain/openai";
import { ChatOllama, OllamaEmbeddings } from "@langchain/ollama";
import { TextLoader } from "langchain/document_loaders/fs/text";
import { HNSWLib } from "@langchain/community/vectorstores/hnswlib";
//import { MemoryVectorStore } from "langchain/vectorstores/memory";
import { RecursiveCharacterTextSplitter } from "langchain/text_splitter";
import {
  ChatPromptTemplate,
  MessagesPlaceholder,
} from "@langchain/core/prompts";
import { createStuffDocumentsChain } from "langchain/chains/combine_documents";
import { createHistoryAwareRetriever } from "langchain/chains/history_aware_retriever";
import { createRetrievalChain } from "langchain/chains/retrieval";
import { RemoteRunnable } from "@langchain/core/runnables/remote";
import * as fs from "node:fs";

import loadDataFile from "./js-process-big-dataset.js";

import "dotenv/config";
const main = async () => {
  const useAllOpenAI = true;
  const runRagas = true;
  const loadSavedHnswLib = true;
  let howManyQueries = 1;

  const metrics = [
    "faithfulness",
    "answer-relevancy",
    "context-precision",
    "context-recall",
  ];
  const metricsMap = {};
  for (let metricName of metrics) {
    metricsMap[metricName] = { name: metricName, sum: 0, avg: 0 };
  }

  const useHnswLib = true;
  const dataSetStr = "trec-covid";
  const knowledge_datas_path = "./knowledge_datas/";

  let chatModelName = "gpt-4o-mini";
  let embedModelName = "text-embedding-3-small";

  let embeddings = null;
  let llm = null;

  if (useAllOpenAI) {
    chatModelName = chatModelName;
    embedModelName = embedModelName;
    embeddings = new OpenAIEmbeddings({ model: embedModelName });
    llm = new ChatOpenAI({ model: chatModelName, temperature: 0 });
  } else {
    chatModelName = "llama3.1:8b-instruct-q4_K_M";
    embedModelName = "nomic-embed-text:latest";
    llm = new ChatOllama({ model: chatModelName, temperature: 0 });
    embeddings = new OllamaEmbeddings({ model: embedModelName });
  }

  // Save the vector store to a directory
  const directoryHNSW =
    knowledge_datas_path + dataSetStr + "/HNSW_" + embedModelName;

  // const aiMsg = await llm.invoke([
  //   [
  //     "system",
  //     "You are a helpful assistant that tells jokes for 8 year old kids. Make the user laugh.",
  //   ],
  //   ["human", "I love programming."],
  // ]);
  // console.log(aiMsg);

  let examples = [];
  let groundTruthIndex = null;
  let loader = {};
  if (dataSetStr) {
    examples = await loadDataFile(dataSetStr);
    howManyQueries = examples.length;
    loader = new TextLoader(knowledge_datas_path + dataSetStr + "_doc.txt");
    groundTruthIndex = 0;
  } else {
    const eval_questions = [
      "What is the population of New York City as of 2020?",
      "Which borough of New York City has the highest population?",
      "What is the economic significance of New York City?",
      "How did New York City get its name?",
      "What is the significance of the Statue of Liberty in New York City?",
    ];

    const eval_answers = [
      "8,804,190",
      "Brooklyn",
      "New York City's economic significance is vast, as it serves as the global financial capital, housing Wall Street and major financial institutions. Its diverse economy spans technology, media, healthcare, education, and more, making it resilient to economic fluctuations. NYC is a hub for international business, attracting global companies, and boasts a large, skilled labor force. Its real estate market, tourism, cultural industries, and educational institutions further fuel its economic prowess. The city's transportation network and global influence amplify its impact on the world stage, solidifying its status as a vital economic player and cultural epicenter.",
      "New York City got its name when it came under British control in 1664. King Charles II of England granted the lands to his brother, the Duke of York, who named the city New York in his own honor.",
      "The Statue of Liberty in New York City holds great significance as a symbol of the United States and its ideals of liberty and peace. It greeted millions of immigrants who arrived in the U.S. by ship in the late 19th and early 20th centuries, representing hope and freedom for those seeking a better life. It has since become an iconic landmark and a global symbol of cultural diversity and freedom.",
    ];

    for (let a = 0; a < eval_answers.length; a++) {
      examples.push({
        query: eval_questions[a],
        ground_truth: eval_answers[a],
      });
    }
    groundTruthIndex = 4;

    loader = new TextLoader("./nyc_wikipedia/nyc_text.txt");
  }
  const source_docs = await loader.load();

  //const MAX_LINES = 10000 // Security to avoid loading huge files

  const textSplitter = new RecursiveCharacterTextSplitter({
    chunkSize: 1000,
    chunkOverlap: 100,
    // NOTE - separators may need to look at for marblism!
    //separators: ["\n"],
  });

  //   const texts = lines.slice(0, MAX_LINES)

  const documents = await textSplitter.splitDocuments(source_docs);

  //   const vectorStore = await MemoryVectorStore.fromDocuments(
  //     documents,
  //     embeddings
  //   );
  let vectorStore = null;
  console.log({ loadSavedHnswLib });
  if (loadSavedHnswLib) {
    // Load the vector store from the same directory
    vectorStore = await HNSWLib.load(directoryHNSW, embeddings);
    console.log("LOADED", { directoryHNSW });
  } else {
    vectorStore = await HNSWLib.fromDocuments(documents, embeddings);
    await vectorStore.save(directoryHNSW);
    console.log("SAVED", { directoryHNSW });
  }
  const retriever = vectorStore.asRetriever();

  const contextualizeQSystemPrompt = `
Given a chat history and the latest user question
which might reference context in the chat history,
formulate a standalone question which can be understood
without the chat history. Do NOT answer the question, just
reformulate it if needed and otherwise return it as is.`;

  //   const contextualizeQPrompt = ChatPromptTemplate.fromMessages([
  //     ["system", contextualizeQSystemPrompt],
  //     new MessagesPlaceholder("chat_history"),
  //     ["human", "{input}"],
  //   ]);

  //   const historyAwareRetriever = await createHistoryAwareRetriever({
  //     llm,
  //     retriever,
  //     rephrasePrompt: contextualizeQPrompt,
  //   });
  const personalityDefault =
    `You are an assistant for question-answering tasks. Use
  the following pieces of retrieved context to answer the
  question. If you don't know the answer, just say that you
  don't know. Use three sentences maximum and keep the answer
  concise.`.trim();

  const system = `
${personalityDefault}
\n\n
{context}`;

  const promptSystem = ChatPromptTemplate.fromMessages([
    ["system", system],
    ["human", "{input}"],
  ]);
  const questionAnswerChain = await createStuffDocumentsChain({
    llm,
    prompt: promptSystem,
  });
  const ragChain = await createRetrievalChain({
    retriever: retriever,
    combineDocsChain: questionAnswerChain,
  });
  console.time();
  for (let runIndex = 0; runIndex < examples.length; runIndex++) {
    console.log(
      { runIndex },
      "Done with creating chain. Sending query",
      examples[runIndex].query
    );
    const response = await ragChain.invoke({
      //chat_history,
      input: examples[runIndex].query,
    });
    if (howManyQueries === 1) {
      console.log("Using LLM:");
      console.log(chatModelName);
      console.log("Using embeddings:");
      console.log(embedModelName);
      console.log({ useHnswLib }, { loadSavedHnswLib });
      console.log("\n");
      console.log("Question: ");
      console.log(examples[runIndex]["query"]);
      console.log("LLM Answer:");
      console.log(response.answer);
      console.log("\n");
    }

    const contexts = [];
    for (let pageCont of response.context) {
      contexts.push(pageCont?.pageContent);
    }
    if (runRagas) {
      if (howManyQueries === 1) {
        console.log("Now running Ragas:");
      }
      const restructured_result = {
        question: examples[runIndex].query,
        answer: response.answer,
        contexts,
        ground_truth: examples[runIndex].ground_truth,
      };

      const metricResult = {};
      for (let metricStr of metrics) {
        let urlStr = "http://127.0.0.1:8100/ragas-" + metricStr;
        const ragasRemote = new RemoteRunnable({
          url: urlStr,
        });
        metricResult[metricStr] = await ragasRemote.invoke(restructured_result);
        if (howManyQueries === 1) {
          console.log(metricResult[metricStr]);
        }
        examples[runIndex][metricStr] =
          metricResult[metricStr][metricStr.replace("-", "_")];
        metricsMap[metricStr].sum +=
          metricResult[metricStr][metricStr.replace("-", "_")];
      }
    } else {
      console.log({ runRagas });
    }
  }
  console.timeEnd();
  if (runRagas) {
    let saveResultsFileName = "./" + chatModelName + "_" + embedModelName + "_";
    if (useHnswLib) {
      saveResultsFileName += "useHnswLib_";
    } else {
      saveResultsFileName += "NOTHnswLib_";
    }
    saveResultsFileName += dataSetStr + "_numQuery-" + howManyQueries;
    fs.writeFileSync(
      saveResultsFileName + "_details.json",
      JSON.stringify(examples, null, 2),
      "utf8"
    );
    console.log(
      "Saved ragas details to",
      saveResultsFileName + "_details.json"
    );
    for (let metricStr of metrics) {
      metricsMap[metricStr].avg = metricsMap[metricStr].sum / howManyQueries;
    }
    console.log(metricsMap);
    fs.writeFileSync(
      saveResultsFileName + "_summary.json",
      JSON.stringify(metricsMap, null, 2),
      "utf8"
    );
    console.log(
      "Saved ragas summary to",
      saveResultsFileName + "_summary.json"
    );
  }
};
main();
