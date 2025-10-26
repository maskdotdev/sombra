# SombraDB Graph Explorer

A modern web interface for exploring SombraDB graph databases. Built with Next.js and reagraph for interactive graph visualization.

## Features

- **Interactive Graph Visualization**: Navigate your graph data with smooth, responsive interactions
- **Database Path Selection**: Connect to any SombraDB database file or use in-memory databases
- **Property Inspector**: View detailed node and edge properties in a side panel
- **Type-Safe API**: Uses SombraDB's typed API for clean property handling
- **Real-time Exploration**: Click nodes to traverse the graph and discover connections

## Getting Started

### Prerequisites

- Node.js 18+ 
- A SombraDB database file (or use the demo data)

### Installation

```bash
npm install
```

### Running the Development Server

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to see the application.

### Using the Graph Explorer

1. **Select a Database**: Enter the path to your SombraDB database file in the database selector
   - Use `./data.db` for a local file
   - Use `:memory:` for an in-memory database
   - Use absolute paths like `/path/to/database.db`

2. **Explore the Graph**: 
   - Click on nodes to traverse their connections
   - Use the properties panel to view detailed information
   - Reset the view to return to the full graph

### Creating Demo Data

To create sample data for testing:

```bash
node scripts/seed-demo.js ./demo.db
```

Then use `./demo.db` as your database path in the web interface.

## Environment Variables

Create a `.env.local` file with:

```bash
SOMBRA_DB_PATH=./your-database.db
```

This sets the default database path for the application.

## API Endpoints

The application provides several API endpoints for graph data:

- `GET /api/graph/nodes` - Get all nodes
- `GET /api/graph/edges` - Get all edges  
- `GET /api/graph/stats` - Get graph statistics
- `GET /api/graph/traverse?nodeId=X&depth=Y` - Traverse from a specific node

All endpoints accept an `X-Database-Path` header to specify the database path.

## Technology Stack

- **Next.js 16** - React framework
- **reagraph** - Graph visualization library
- **TypeScript** - Type safety
- **Tailwind CSS** - Styling
- **SombraDB** - Graph database backend

## Development

The project structure:

```
app/
  api/graph/          # API routes for graph data
  graph/              # Graph explorer page
  page.tsx            # Landing page
components/
  database-selector.tsx  # Database path input component
  graph-explorer.tsx     # Main graph visualization component
lib/
  db.ts               # Database operations
  transforms.ts       # Data transformation utilities
  types.ts            # TypeScript type definitions
scripts/
  seed-demo.js        # Demo data generation script
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with both file-based and in-memory databases
5. Submit a pull request

## License

This project is part of the SombraDB ecosystem. See the main repository for license information.
