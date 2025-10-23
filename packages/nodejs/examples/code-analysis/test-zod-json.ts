import { z } from 'zod';
import zodToJsonSchema from 'zod-to-json-schema';

const testSchema = z.object({
  nodeId: z.number().describe('The ID of the node to retrieve')
});

console.log("Using zod-to-json-schema:");
const jsonSchema = zodToJsonSchema(testSchema, 'nodeIdSchema');
console.log(JSON.stringify(jsonSchema, null, 2));
