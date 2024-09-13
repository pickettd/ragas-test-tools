import { ChatOpenAI, OpenAIEmbeddings } from "@langchain/openai";
import { ChatOllama, OllamaEmbeddings } from "@langchain/ollama";
import { TextLoader } from "langchain/document_loaders/fs/text";
import { MemoryVectorStore } from "langchain/vectorstores/memory";
import { RecursiveCharacterTextSplitter } from "langchain/text_splitter";
import {
  ChatPromptTemplate,
  MessagesPlaceholder,
} from "@langchain/core/prompts";
import { createStuffDocumentsChain } from "langchain/chains/combine_documents";
import { createHistoryAwareRetriever } from "langchain/chains/history_aware_retriever";
import { createRetrievalChain } from "langchain/chains/retrieval";

import "dotenv/config";
const main = async () => {
  const useAllOpenAI = true;
  let chatModelName = "gpt-4o-mini";
  let embedModelName = "text-embedding-3-small";

  let embeddings = new OpenAIEmbeddings({ model: embedModelName });
  let llm = new ChatOpenAI({ model: chatModelName, temperature: 0 });

  if (useAllOpenAI) {
    chatModelName = chatModelName;
    embedModelName = embedModelName;
  } else {
    chatModelName = "llama3.1:8b-instruct-q4_K_M";
    embedModelName = "nomic-embed-text:latest";
    llm = new ChatOllama({ model: chatModelName, temperature: 0 });
    embeddings = new OllamaEmbeddings({ model: embedModelName });
  }

  // const aiMsg = await llm.invoke([
  //   [
  //     "system",
  //     "You are a helpful assistant that tells jokes for 8 year old kids. Make the user laugh.",
  //   ],
  //   ["human", "I love programming."],
  // ]);
  // console.log(aiMsg);

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
  const examples = [];
  for (let a = 0; a < eval_answers.length; a++) {
    examples.push({ query: eval_questions[a], ground_truth: eval_answers[a] });
  }
  const groundTruthIndex = 4;

  const loader = new TextLoader("./nyc_wikipedia/nyc_text.txt");
  const nyc_docs = await loader.load();

  //const MAX_LINES = 10000 // Security to avoid loading huge files

  const textSplitter = new RecursiveCharacterTextSplitter({
    chunkSize: 1000,
    chunkOverlap: 100,
    separators: ["\n"],
  });

  //   const texts = lines.slice(0, MAX_LINES)

  const documents = await textSplitter.splitDocuments(nyc_docs);

  const vectorStore = await MemoryVectorStore.fromDocuments(
    documents,
    embeddings
  );

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
  const response = await ragChain.invoke({
    //chat_history,
    input: examples[groundTruthIndex].query,
  });
  console.log(response.answer);
  //console.log(response.context[0]);
  //console.log(response.context[0]?.pageContent);
  const contexts = [];
  for (let pageCont of response.context) {
    contexts.push(pageCont?.pageContent);
  }
  const restructured_result = {
    question: examples[groundTruthIndex].query,
    answer: result.answer,
    contexts,
    ground_truth: examples[groundTruthIndex].ground_truth,
  };
};
main();
