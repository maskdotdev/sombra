# Sombra Code Analysis Agent

This example demonstrates how to expose Sombra graph database operations as tools for an LLM agent using the AI SDK.

## Features

The database is pre-seeded with a realistic code structure including:
- 5 files (user-service.ts, auth-service.ts, user-controller.ts, database.ts, logger.ts)
- 4 classes (UserService, AuthService, UserController, Database)
- 15 functions/methods with realistic call relationships
- CONTAINS, CALLS, and IMPORTS relationships

The agent has access to specialized tools optimized for code analysis:

### Basic Operations
- **addNode**: Create nodes with labels and properties
- **addEdge**: Create relationships between nodes
- **getNode**: Retrieve node details
- **getEdge**: Retrieve edge details

### Querying
- **queryByLabel**: Find all nodes with a specific label (e.g., "Function", "Class", "File")
- **queryByProperty**: Find nodes by property name and value
- **getNeighbors**: Find connected nodes
- **getIncomingEdges**: Find what calls/uses a function (enhanced with source node details)
- **getOutgoingEdges**: Find what a function calls/uses (enhanced with target node details)

### Advanced Code Analysis
- **findFunctionCallers**: Find all functions that call a specific function (perfect for impact analysis)
- **findFilesUsingFunction**: Find all files that use a specific function (traces through relationships)
- **findCallChain**: Find the complete call chain between two functions
- **traversePath**: Find paths between nodes using BFS

### Analytics
- **analyzeGraph**: Get graph analytics and statistics

## Setup

1. Install dependencies:
```bash
cd examples/code-analysis
npm install
```

2. Set your OpenAI-compatible API credentials:
```bash
export OPENAI_API_KEY="your-api-key"
export OPENAI_BASE_URL="https://api.openai.com/v1"  # or your compatible endpoint
```

3. Run the agent:
```bash
npm start
```

## How It Works

The agent uses specialized tools to analyze the code graph efficiently:

### Example Queries

1. **Which files use the `query` function?**
   - Tool: `findFilesUsingFunction`
   - Traces CALLS → CONTAINS relationships to find files

2. **What functions does `handleCreateUser` call?**
   - Tool: `getOutgoingEdges` with edgeType filter
   - Returns direct function calls with line numbers

3. **Which functions call `logInfo`?**
   - Tool: `findFunctionCallers`
   - Returns all callers with line numbers

4. **What is the call chain from `handleUpdateUser` to `query`?**
   - Tool: `findCallChain`
   - Uses BFS to find and reconstruct the path

The LLM autonomously decides which tools to use based on the question. The specialized tools make queries more efficient and return structured data.

## Seed Data

The `seed.ts` file populates the graph with a realistic code structure. You can modify it to represent your own codebase structure for testing different analysis patterns.

## Architecture

The example is split into three files for clarity:

- **`seed.ts`**: Populates the graph with realistic code structure
- **`tools.ts`**: Defines all Sombra tools available to the agent
- **`agent.ts`**: Configures and runs the AI agent

### Adding Custom Tools

You can add more specialized tools in `tools.ts`:

```typescript
export function createSombraTools(db: SombraDB) {
  return {
    // ... existing tools
    
    yourCustomTool: tool({
      description: 'What this tool does',
      parameters: z.object({
        param: z.string().describe('Parameter description')
      }),
      execute: async ({ param }: any) => {
        // Use Sombra API
        const results = db.query().execute();
        return { results };
      }
    })
  };
}
```

### Best Practices for Tool Design

1. **Be specific**: Create tools for common patterns (e.g., `findFunctionCallers` vs generic `getIncomingEdges`)
2. **Return structured data**: Include node IDs, names, and relevant metadata
3. **Handle errors**: Check if nodes exist before querying
4. **Follow relationships**: Trace through CONTAINS, CALLS, IMPORTS as needed
5. **Add context**: Return related node details, not just IDs
