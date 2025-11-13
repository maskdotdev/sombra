# Sombra Dashboard UI

React + shadcn UI application that powers the `sombra dashboard` experience. It
talks to the Rust CLI server via JSON endpoints (`/health`, `/api/stats`, etc.)
and can be served either by the CLI (for production) or by Vite during
development.

## Quick start

```bash
cd packages/dashboard
npm install
```

### Develop against the CLI server

Before starting the dashboard, make sure the database you point at actually
contains data. We now check in a moderately sized synthetic dataset at
`tests/fixtures/demo-db/graph-demo.sombra` (≈420 nodes / 3.2k edges) so you can
kick the Graph Explorer tires immediately:

```bash
sombra dashboard tests/fixtures/demo-db/graph-demo.sombra --read-only
```

If you prefer to seed a new database from scratch you can still run:

```bash
cargo run --bin cli -- seed-demo path/to/db.sombra --create
# or, if the CLI is installed, `sombra seed-demo path/to/db.sombra --create`
```

1. In another terminal, run the dashboard CLI (read-only is the current MVP):

   ```bash
   sombra dashboard path/to/db.sombra --read-only
   ```

   By default the server listens on `http://127.0.0.1:7654`.

2. Start the React dev server and point it at the CLI API:

   ```bash
   VITE_SOMBRA_API=http://127.0.0.1:7654 npm run dev
   ```

   Open the printed Vite URL (usually <http://localhost:5173>). API requests are
   proxied to the CLI via `VITE_SOMBRA_API`.

### Regenerate the demo database

The synthetic dataset can be recreated at any time:

```bash
# 1. Generate deterministic CSV fixtures (adjust counts via --node-count/--edge-count)
python3 scripts/generate_graph_demo.py \
  --nodes-out tests/fixtures/demo-db/graph-demo-nodes.csv \
  --edges-out tests/fixtures/demo-db/graph-demo-edges.csv

# 2. Import them into a fresh .sombra file
cargo run --bin cli -- import tests/fixtures/demo-db/graph-demo.sombra \
  --nodes tests/fixtures/demo-db/graph-demo-nodes.csv \
  --node-label-column labels \
  --node-props name,handle,role,team,city,joined_at,skills \
  --edges tests/fixtures/demo-db/graph-demo-edges.csv \
  --edge-type-column type \
  --edge-props project,strength,since \
  --create
```

### Build static assets for the CLI

The prebuilt CLI already embeds the latest `packages/dashboard/build/client`
bundle, so `sombra dashboard path/to/db.sombra` just works.

If you make frontend changes locally, rebuild and either override the embedded
files or refresh them before cutting a release:

```bash
npm run build

# Optional: point the CLI at your fresh assets instead of the embedded bundle
sombra dashboard path/to/db.sombra \
  --assets /absolute/path/to/packages/dashboard/build/client
```

(Adjust the path if your workspace layout differs.)

### Scripts

| Script            | Purpose                                    |
| ----------------- | ------------------------------------------ |
| `npm run dev`     | Vite dev server with HMR                   |
| `npm run build`   | React Router production build              |
| `npm run start`   | Serve the built server bundle (Node)       |
| `npm run typecheck` | Generate React Router types + run `tsc` |

## Features implemented so far

- **Overview cards** sourced from `/health` + `/api/stats`.
- **Query console** (JSON spec + results table) that POSTs to `/api/query` and remembers the last 5 specs locally for quick reruns.
- **Graph Explorer** page that streams a capped result set before rendering the force-directed view so large datasets stay responsive.
- shadcn UI components and Tailwind tokens ready for additional pages (jobs, query history, etc.).

---

Questions? Ping the Sombra team in the repo discussions. Happy hacking! ***
