# sombra

Python bindings for the Sombra graph database. The package exposes the Stage 8
fluent query builder together with lightweight CRUD helpers that forward to the
Rust planner/executor through `pyo3`.

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
