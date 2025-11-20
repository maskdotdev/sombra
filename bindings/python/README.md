# sombra

Python bindings for the Sombra graph database. The package exposes the Stage 8
fluent query builder together with lightweight CRUD helpers that forward to the
Rust planner/executor through `pyo3`.

## Typed facade

For schema-aware CRUD helpers that mirror the TypeScript ergonomics, use the
`SombraDB` wrapper under `sombra.typed`:

```python
from typing import TypedDict
from typing_extensions import Literal

from sombra.typed import NodeSchema, SombraDB, TypedGraphSchema


class PersonProps(TypedDict):
    name: str
    age: int


class PersonNode(NodeSchema):
    properties: PersonProps


class GraphSchema(TypedGraphSchema):
    nodes: dict[str, PersonNode]
    edges: dict[str, TypedDict("KnowsEdge", {"from": Literal["Person"], "to": Literal["Person"], "properties": dict})]


schema: GraphSchema = {
    "nodes": {"Person": {"properties": {"name": "", "age": 0}}},
    "edges": {"KNOWS": {"from": "Person", "to": "Person", "properties": {}}},
}

db = SombraDB("/tmp/typed.db", schema=schema)
friend = db.add_node("Person", {"name": "Ada", "age": 33})
```

See `examples/typed.py` for a longer walkthrough that mirrors the Node.js demo.

## Quick start

```python
from sombra import Database
from sombra.query import eq

db = Database.open("/tmp/sombra.db")
db.seed_demo()

rows = db.query().nodes("User").where(eq("name", "Ada")).select("name").execute()
print(rows[0]["name"])

# Need request ids or feature flags? Ask for the metadata envelope:
payload = (
    db.query().nodes("User").request_id("example").select("name").execute(with_meta=True)
)
print(payload.request_id(), len(payload.rows()))

user_id = db.create_node("User", {"name": "New User"})
db.update_node(user_id, set_props={"bio": "updated"})
db.delete_node(user_id, cascade=True)
```

Run the end-to-end example in `examples/crud.py` to see the workflow in action:

```bash
python examples/crud.py
```

## Benchmarks

`benchmarks/crud.py` performs simple create/read/update/delete micro-benchmarks
against a throwaway database:

```bash
python benchmarks/crud.py
```

Predicate builders accept timezone-aware `datetime` objects directly and reject naive datetimes so callers do not need to juggle epochs. Property projections (`{"var": "a", "prop": "name", "as": "alias"}`) return scalar columns when you only need a subset of properties.

## Development

Install the native module via [maturin](https://www.maturin.rs/):

```bash
cd bindings/python
maturin develop --release
python -m pytest tests
```

Use `maturin build` to produce distributable wheels once you are ready to
publish the bindings.

## Release Workflow

1. Land a `feat` commit that touches `bindings/python/**` whenever you want the PyO3 wheel to bump its minor version. Release Please maps that commit to a new `sombrapy` release PR.
2. Before merging the PR, build and test the wheel locally: `maturin develop`, `pytest -q`, and `maturin build --release` (or `maturin publish --dry-run`).
3. Merge the release PR to tag the repo; the `publish-python.yml` workflow uploads the wheels to PyPI. Re-run `maturin publish` manually only if the workflow fails.
