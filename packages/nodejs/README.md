# SombraDB - Node.js Bindings

High-performance graph database for Node.js and TypeScript, powered by Rust.

> ⚠️ **Alpha Software**: Sombra is under active development. APIs may change, and the project is not yet recommended for production use. Feedback and contributions are welcome!

## Installation

```bash
npm install @unyth/sombra
```

## Features

- **Property Graph Model**: Nodes, edges, and flexible properties
- **ACID Transactions**: Full transactional support with rollback
- **Fast Performance**: Native Rust implementation with NAPI bindings
- **TypeScript Support**: Full type definitions with optional generic schemas
- **Unified API**: Single class works with or without type safety
- **Cross-Platform**: Pre-built binaries for Linux, macOS, and Windows

## Quick Start

### Type-Safe API (Recommended for TypeScript)

The unified API works with or without TypeScript generics. For full compile-time type safety, define a schema:

```typescript
import { SombraDB } from '@unyth/sombra';

interface MyGraphSchema {
  nodes: {
    User: {
      name: string;
      age: number;
    };
    Post: {
      title: string;
      content: string;
    };
  };
  edges: {
    AUTHORED: {
      from: 'User';
      to: 'Post';
      properties: {
        publishedAt: number;
      };
    };
  };
}

const db = new SombraDB<MyGraphSchema>('./my_graph.db');

// Full autocomplete and type checking
const user = db.addNode('User', { name: 'Alice', age: 30 });

const post = db.addNode('Post', { 
  title: 'Hello World',
  content: 'My first post'
});

db.addEdge(user, post, 'AUTHORED', { publishedAt: Date.now() });

// Type-narrowed return values
const userNode = db.getNode<'User'>(user);
console.log(`Found: ${userNode?.properties.name}`); // typed as string

// Type-safe property search
const foundUser = db.findNodeByProperty('User', 'name', 'Alice');
```

#### Multiple Labels (Union Semantics)

Nodes can have multiple labels, and properties from any label are accepted:

```typescript
interface MyGraphSchema {
  nodes: {
    Person: { name: string; age: number };
    Employee: { employeeId: string; department: string };
  };
  edges: { /* ... */ };
}

const db = new SombraDB<MyGraphSchema>('./my_graph.db');

// Node with both Person and Employee properties (all required fields from both labels)
const user = db.addNode(['Person', 'Employee'], { 
  name: 'Alice', 
  age: 30,
  employeeId: 'E123',
  department: 'Engineering'
});

// Query by either label
db.getNodesByLabel('Person');    // returns the node
db.getNodesByLabel('Employee');  // returns the node
```

**IDE Autocomplete:**
- **Label names**: When typing `['Person', '...']`, your IDE suggests valid label names
- **Property names**: Autocomplete shows the union of properties from the selected labels; optional fields stay optional
- **Type safety**: TypeScript validates label names, property names, and required fields at compile time

**Benefits:**
- Full autocomplete for node labels, edge types, and properties
- Compile-time type validation
- Automatic conversion between TypeScript types and SombraDB format
- No manual `{type, value}` objects needed

### JavaScript API (Backwards Compatible)

For JavaScript or raw property access, use the same class without generics:

```javascript
const { SombraDB } = require('@unyth/sombra');

const db = new SombraDB('./my_graph.db');

// Raw API with explicit property format
const user = db.addNode(['User'], {
  name: { type: 'string', value: 'Alice' },
  age: { type: 'int', value: 30 }
});

const post = db.addNode(['Post']);

db.addEdge(user, post, 'AUTHORED', {
  publishedAt: { type: 'int', value: Date.now() }
});

const neighbors = db.getNeighbors(user);
console.log(`User ${user} has ${neighbors.length} connections`);
```

**Note:** The API auto-detects input format, so you can mix approaches as needed.

## Documentation

- [Getting Started Guide](https://github.com/maskdotdev/sombra/blob/main/docs/nodejs-guide.md)
- [API Reference](https://github.com/maskdotdev/sombra/blob/main/packages/nodejs/index.d.ts)
- [Main Documentation](https://github.com/maskdotdev/sombra)

## Building from Source

```bash
npm install
npm run build
npm test
```

## Repository

[GitHub](https://github.com/maskdotdev/sombra)

## License

MIT
