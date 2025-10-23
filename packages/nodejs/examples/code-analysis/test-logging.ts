import { SombraDB } from "sombradb";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { generateText, stepCountIs } from "ai";
import { seedCodeGraph } from "./seed.js";
import { createSombraTools } from "./tools.js";

const db = new SombraDB("./code-analysis.db");
seedCodeGraph(db);

async function runAgent() {
  const openaiCompatible = createOpenAICompatible({
    baseURL: process.env.OPENAI_BASE_URL || "https://api.openai.com/v1",
    apiKey: process.env.OPENAI_API_KEY || "",
    name: "openai-compatible",
  });

  const tools = createSombraTools(db);

  console.log("🚀 Testing tool call logging\n");

  const prompt = "Find all functions that call 'logInfo' and show me the call chain from 'handleUpdateUser' to 'query'";

  const result = await generateText({
    model: openaiCompatible("gpt-4o"),
    tools,
    stopWhen: stepCountIs(5),
    prompt,
  });

  console.log("\n📊 Agent Response:\n");
  console.log(result.text);

  console.log("\n\n🔧 Detailed Step Analysis:");
  result.steps.forEach((step: any, idx: number) => {
    console.log(`\n=== Step ${idx + 1} ===`);
    console.log('Step keys:', Object.keys(step));
    if (step.toolCalls) {
      console.log(`Tool calls (${step.toolCalls.length}):`);
      step.toolCalls.forEach((call: any, callIdx: number) => {
        console.log(`\n  Call ${callIdx + 1}:`);
        console.log(`  Tool: ${call.toolName}`);
        console.log(`  Call keys:`, Object.keys(call));
        console.log(`  Args:`, call.args);
      });
    }
    if (step.toolResults) {
      console.log(`\nTool results (${step.toolResults.length}):`);
      step.toolResults.forEach((result: any, resIdx: number) => {
        console.log(`\n  Result ${resIdx + 1}:`);
        console.log(`  Tool: ${result.toolName}`);
        console.log(`  Result keys:`, Object.keys(result));
      });
    }
  });

  db.flush();
}

runAgent().catch(console.error);
