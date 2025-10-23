import { SombraDB } from "sombradb";
import { createSombraTools } from "./tools.js";
import { jsonSchema } from 'ai';
import { z } from 'zod';

const db = new SombraDB(":memory:");

const allTools = createSombraTools(db);
const tools = allTools;

console.log("Testing first tool:");
const getNodeTool = tools.getNode as any;

console.log("\nTool type:", typeof getNodeTool);
console.log("\nTool keys:", Object.keys(getNodeTool));

if (getNodeTool.parameters) {
  console.log("\nParameters type:", typeof getNodeTool.parameters);
  console.log("Parameters constructor:", getNodeTool.parameters.constructor.name);
  console.log("\nIs ZodObject?", getNodeTool.parameters._def?.typeName === 'ZodObject');
  
  // Try to convert to JSON Schema
  try {
    const jsonSchemaParam = jsonSchema(getNodeTool.parameters);
    console.log("\nConverted to JSON Schema:");
    console.log(JSON.stringify(jsonSchemaParam, null, 2));
  } catch (e: any) {
    console.log("\nFailed to convert:", e.message);
  }
}

// Test a simple Zod schema conversion
console.log("\n\n=== Testing simple Zod schema ===");
const testSchema = z.object({
  nodeId: z.number().describe('The ID of the node to retrieve')
});

console.log("Test schema type:", testSchema._def.typeName);
const converted = jsonSchema(testSchema);
console.log("Converted test schema:");
console.log(JSON.stringify(converted, null, 2));
