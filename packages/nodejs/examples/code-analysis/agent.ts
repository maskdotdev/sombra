import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { generateText, stepCountIs } from "ai";
import { SombraDB } from "sombradb";
import { seedCodeGraph } from "./seed.js";
import { createSombraTools } from "./tools.js";

const db = new SombraDB("./code-analysis.db");
seedCodeGraph(db);

// Wrap tools with timing and ensure JSON-serializable results
function wrapToolsWithTiming<T extends Record<string, unknown>>(tools: T): T {
	const wrapped = {} as Record<string, unknown>;
	for (const [name, tool] of Object.entries(tools)) {
		if (tool && typeof tool === "object" && "execute" in tool) {
			const originalExecute = tool.execute as (
				...args: unknown[]
			) => Promise<unknown>;
			wrapped[name] = {
				...tool,
				execute: async (...args: unknown[]) => {
					const start = performance.now();
					const result = await originalExecute(...args);
					const duration = performance.now() - start;
					console.log(`[TOOL] ${name}: ${duration.toFixed(2)}ms`);
					// Ensure result is JSON-serializable by round-tripping through JSON
					return JSON.parse(JSON.stringify(result));
				},
			};
		}
	}
	return wrapped as T;
}

async function runAgent() {
	const openaiCompatible = createOpenAICompatible({
		baseURL: process.env.OPENAI_BASE_URL || "https://api.openai.com/v1",
		apiKey: process.env.OPENAI_API_KEY || "",
		name: "openai-compatible",
	});

	const tools = wrapToolsWithTiming(createSombraTools(db));

	console.log("🚀 Code Analysis Agent with Sombra Graph Database\n");

	const prompt = `
You are a code analysis agent with access to a graph database (Sombra) containing a codebase structure.

The graph database contains nodes representing Files, Classes, and Functions, and edges representing relationships like:
- CONTAINS: file contains class, class contains method/function
- CALLS: function calls another function
- IMPORTS: file imports from another file

Use the available tools to discover and analyze the codebase to answer these questions:
1. Are there any "query" functions in this codebase? If so, which files use them?
2. What functions does "handleCreateUser" call (if it exists)?
3. Which functions call "logInfo" (if it exists)?
4. What is the call chain from "handleUpdateUser" to "query" (if these functions exist)?

Start by exploring the graph to understand what's in the codebase, then answer each question with specific details.
`;

	const startTime = performance.now();
	const result = await generateText({
		model: openaiCompatible("gpt-4o"),
		tools,
		stopWhen: stepCountIs(10),
		prompt,
	});
	const endTime = performance.now();

	console.log("\n📊 Agent Response:\n");
	console.log(result.text);

	console.log("\n\n🔧 Tool Calls Summary:");
	let toolCallCount = 0;
	for (let idx = 0; idx < result.steps.length; idx++) {
		const step = result.steps[idx];
		console.log(`\nStep ${idx + 1}:`);

		if (step.toolCalls && step.toolCalls.length > 0) {
			console.log(`  Tool Calls (${step.toolCalls.length}):`);
			for (const call of step.toolCalls) {
				toolCallCount++;
				const args = JSON.stringify("args" in call ? call.args : {});
				console.log(`    - ${call.toolName}(${args})`);
			}
		}

		if (step.usage) {
			const promptTokens =
				"promptTokens" in step.usage ? step.usage.promptTokens : 0;
			const completionTokens =
				"completionTokens" in step.usage ? step.usage.completionTokens : 0;
			console.log(
				`  LLM Usage: ${promptTokens} prompt + ${completionTokens} completion = ${step.usage.totalTokens || 0} tokens`,
			);
		}
	}

	console.log("\n📈 Overall Stats:");
	console.log(`  Total tool calls: ${toolCallCount}`);
	console.log(`  Total steps: ${result.steps.length}`);
	console.log(`  Total execution time: ${Math.round(endTime - startTime)}ms`);
	console.log(`  Total tokens: ${result.usage.totalTokens}`);

	db.flush();
	console.log("\n✅ Database persisted to disk");
}

runAgent().catch(console.error);
