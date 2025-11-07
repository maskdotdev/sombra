# sombra-py

Python bindings for the Sombra graph database. The package exposes the Stage 8
fluent query builder together with lightweight CRUD helpers that forward to the
Rust planner/executor through `pyo3`.

## Quick start

```python
from sombra_py import Database

db = Database.open("/tmp/sombra.db")
db.seed_demo()

rows = (
    db.query()
    .match("User")
    .where_prop("a", "name", "=", "Ada")
    .select(["a"])
    .execute()
)

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

## Development

Install the native module via [maturin](https://www.maturin.rs/):

```bash
cd bindings/python
maturin develop --release
python -m pytest tests
```

Use `maturin build` to produce distributable wheels once you are ready to
publish the bindings.
