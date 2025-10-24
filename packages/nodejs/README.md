# SombraDB - Node.js Bindings

High-performance graph database for Node.js and TypeScript, powered by Rust.

> ⚠️ **Alpha Software**: Sombra is under active development. APIs may change, and the project is not yet recommended for production use. Feedback and contributions are welcome!

## Installation

```bash
npm install sombradb
```

## Features

- **Property Graph Model**: Nodes, edges, and flexible properties
- **ACID Transactions**: Full transactional support with rollback
- **Fast Performance**: Native Rust implementation with NAPI bindings
- **TypeScript Support**: Full type definitions included
- **Cross-Platform**: Pre-built binaries for Linux, macOS, and Windows

## Quick Start

```typescript
import { SombraDB, SombraPropertyValue } from 'sombradb';

const db = new SombraDB('./my_graph.db');

const createProp = (type: 'string' | 'int' | 'float' | 'bool', value: any): SombraPropertyValue => ({
  type,
  value
});

const user = db.addNode();
db.setNodeLabel(user, 'User');
db.setNodeProperty(user, 'name', createProp('string', 'Alice'));

const post = db.addNode();
db.setNodeLabel(post, 'Post');

db.addEdge(user, post, 'AUTHORED');

const neighbors = db.getNeighbors(user);
console.log(`User ${user} authored ${neighbors.length} posts`);
```

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

## License

MIT
