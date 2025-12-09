#![forbid(unsafe_code)]
#![allow(clippy::arc_with_non_send_sync)]

use std::sync::{Arc, Mutex};

use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyAny, PyBool, PyDict, PyList, PyModule, PyTuple},
    Bound,
};
use serde_json::Value;
use sombra::{
    ffi::{Database, DatabaseOptions, FfiError, QueryStream},
    primitives::pager::{PagerOptions, Synchronous},
    storage::Dir,
};

#[derive(Debug)]
struct PyConnectOptions {
    create_if_missing: bool,
    page_size: Option<u32>,
    cache_pages: Option<u32>,
    distinct_neighbors_default: bool,
    synchronous: Option<Synchronous>,
    commit_coalesce_ms: Option<u32>,
    commit_max_frames: Option<u32>,
    commit_max_commits: Option<u32>,
    group_commit_max_writers: Option<u32>,
    group_commit_max_frames: Option<u32>,
    group_commit_max_wait_ms: Option<u32>,
    async_fsync: Option<bool>,
    async_fsync_max_wait_ms: Option<u32>,
    wal_segment_size_bytes: Option<u64>,
    wal_preallocate_segments: Option<u32>,
    autocheckpoint_ms: Option<u32>,
}

impl Default for PyConnectOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            page_size: None,
            cache_pages: None,
            distinct_neighbors_default: false,
            synchronous: None,
            commit_coalesce_ms: None,
            commit_max_frames: None,
            commit_max_commits: None,
            group_commit_max_writers: None,
            group_commit_max_frames: None,
            group_commit_max_wait_ms: None,
            async_fsync: None,
            async_fsync_max_wait_ms: None,
            wal_segment_size_bytes: None,
            wal_preallocate_segments: None,
            autocheckpoint_ms: None,
        }
    }
}

#[pyclass(module = "sombra._native", unsendable)]
pub struct DatabaseHandle {
    inner: Mutex<Option<Arc<Database>>>,
}

impl DatabaseHandle {
    /// Returns a reference to the inner database, or an error if closed.
    fn with_db<F, T>(&self, f: F) -> PyResult<T>
    where
        F: FnOnce(&Arc<Database>) -> PyResult<T>,
    {
        let guard = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("[CLOSED] database lock poisoned"))?;
        match guard.as_ref() {
            Some(db) => f(db),
            None => Err(PyRuntimeError::new_err("[CLOSED] database is closed")),
        }
    }
}

#[pyclass(module = "sombra._native", unsendable)]
pub struct StreamHandle {
    inner: Mutex<Option<QueryStream>>,
}

struct ParsedNeighborOptions {
    direction: Dir,
    edge_type: Option<String>,
    distinct: bool,
}

struct ParsedBfsOptions {
    direction: Dir,
    edge_types: Option<Vec<String>>,
    max_results: Option<usize>,
}

impl Default for ParsedNeighborOptions {
    fn default() -> Self {
        Self {
            direction: Dir::Out,
            edge_type: None,
            distinct: true,
        }
    }
}

impl Default for ParsedBfsOptions {
    fn default() -> Self {
        Self {
            direction: Dir::Out,
            edge_types: None,
            max_results: None,
        }
    }
}

fn parse_connect_options(options: Option<&Bound<'_, PyDict>>) -> PyResult<PyConnectOptions> {
    let mut opts = PyConnectOptions::default();
    if let Some(dict) = options {
        if let Some(value) = dict.get_item("create_if_missing")? {
            opts.create_if_missing = value.extract::<bool>()?;
        }
        if let Some(value) = dict.get_item("page_size")? {
            opts.page_size = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("cache_pages")? {
            opts.cache_pages = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("distinct_neighbors_default")? {
            opts.distinct_neighbors_default = value.extract::<bool>()?;
        }
        if let Some(value) = dict.get_item("synchronous")? {
            let mode = value.extract::<String>()?;
            opts.synchronous = Some(parse_synchronous(&mode)?);
        }
        if let Some(value) = dict.get_item("commit_coalesce_ms")? {
            opts.commit_coalesce_ms = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("commit_max_frames")? {
            opts.commit_max_frames = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("commit_max_commits")? {
            opts.commit_max_commits = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("group_commit_max_writers")? {
            opts.group_commit_max_writers = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("group_commit_max_frames")? {
            opts.group_commit_max_frames = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("group_commit_max_wait_ms")? {
            opts.group_commit_max_wait_ms = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("async_fsync")? {
            opts.async_fsync = Some(value.extract::<bool>()?);
        }
        if let Some(value) = dict.get_item("async_fsync_max_wait_ms")? {
            opts.async_fsync_max_wait_ms = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("wal_segment_size_bytes")? {
            opts.wal_segment_size_bytes = Some(value.extract::<u64>()?);
        }
        if let Some(value) = dict.get_item("wal_preallocate_segments")? {
            opts.wal_preallocate_segments = Some(value.extract::<u32>()?);
        }
        if let Some(value) = dict.get_item("autocheckpoint_ms")? {
            opts.autocheckpoint_ms = Some(value.extract::<u32>()?);
        }
    }
    Ok(opts)
}

#[pyfunction]
fn open_database(path: &str, options: Option<&Bound<'_, PyDict>>) -> PyResult<DatabaseHandle> {
    let opts = parse_connect_options(options)?;

    let mut pager = PagerOptions::default();
    if let Some(size) = opts.page_size {
        pager.page_size = size;
    }
    if let Some(cache) = opts.cache_pages {
        pager.cache_pages = cache as usize;
    }
    if let Some(mode) = opts.synchronous {
        pager.synchronous = mode;
    }
    if let Some(ms) = opts.group_commit_max_wait_ms.or(opts.commit_coalesce_ms) {
        pager.group_commit_max_wait_ms = ms as u64;
    }
    if let Some(frames) = opts.group_commit_max_frames.or(opts.commit_max_frames) {
        pager.group_commit_max_frames = frames as usize;
    }
    if let Some(commits) = opts.group_commit_max_writers.or(opts.commit_max_commits) {
        pager.group_commit_max_writers = commits as usize;
    }
    if let Some(async_fsync) = opts.async_fsync {
        pager.async_fsync = async_fsync;
    }
    if let Some(wait_ms) = opts.async_fsync_max_wait_ms {
        pager.async_fsync_max_wait_ms = wait_ms as u64;
    }
    if let Some(bytes) = opts.wal_segment_size_bytes {
        pager.wal_segment_size_bytes = bytes;
    }
    if let Some(preallocate) = opts.wal_preallocate_segments {
        pager.wal_preallocate_segments = preallocate;
    }
    if let Some(ms) = opts.autocheckpoint_ms {
        pager.autocheckpoint_ms = Some(ms as u64);
    }

    let db_opts = DatabaseOptions {
        create_if_missing: opts.create_if_missing,
        pager,
        distinct_neighbors_default: opts.distinct_neighbors_default,
        ..DatabaseOptions::default()
    };

    let db = Database::open(path, db_opts).map_err(to_py_err)?;
    Ok(DatabaseHandle {
        inner: Mutex::new(Some(Arc::new(db))),
    })
}

#[pyfunction]
fn database_close(handle: &DatabaseHandle) -> PyResult<()> {
    let mut guard = handle
        .inner
        .lock()
        .map_err(|_| PyRuntimeError::new_err("[CLOSED] database lock poisoned"))?;
    // Take the database out, dropping it
    let _ = guard.take();
    Ok(())
}

#[pyfunction]
fn database_is_closed(handle: &DatabaseHandle) -> PyResult<bool> {
    let guard = handle
        .inner
        .lock()
        .map_err(|_| PyRuntimeError::new_err("[CLOSED] database lock poisoned"))?;
    Ok(guard.is_none())
}

#[pyfunction]
fn database_execute(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    handle.with_db(|db| {
        let response = db.execute_json(&value).map_err(to_py_err)?;
        value_to_py(py, response)
    })
}

#[pyfunction]
fn database_explain(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    handle.with_db(|db| {
        let explain = db.explain_json(&value).map_err(to_py_err)?;
        value_to_py(py, explain)
    })
}

#[pyfunction]
fn database_stream(handle: &DatabaseHandle, spec: &Bound<'_, PyAny>) -> PyResult<StreamHandle> {
    let value = any_to_value(spec)?;
    handle.with_db(|db| {
        let stream = db.stream_json(&value).map_err(to_py_err)?;
        Ok(StreamHandle {
            inner: Mutex::new(Some(stream)),
        })
    })
}

#[pyfunction]
fn database_mutate(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    handle.with_db(|db| {
        let summary = db.mutate_json(&value).map_err(to_py_err)?;
        value_to_py(py, summary)
    })
}

#[pyfunction]
fn database_create(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    handle.with_db(|db| {
        let summary = db.create_json(&value).map_err(to_py_err)?;
        value_to_py(py, summary)
    })
}

#[pyfunction]
fn database_intern(handle: &DatabaseHandle, name: &str) -> PyResult<u32> {
    handle.with_db(|db| db.intern(name).map_err(to_py_err))
}

#[pyfunction]
fn database_seed_demo(handle: &DatabaseHandle) -> PyResult<()> {
    handle.with_db(|db| db.seed_demo().map_err(to_py_err))
}

#[pyfunction]
fn database_cancel_request(handle: &DatabaseHandle, request_id: &str) -> PyResult<bool> {
    handle.with_db(|db| Ok(db.cancel_request(request_id)))
}

#[pyfunction]
fn database_get_node(
    py: Python<'_>,
    handle: &DatabaseHandle,
    node_id: u64,
) -> PyResult<Option<PyObject>> {
    handle.with_db(|db| {
        let record = db.get_node_record(node_id).map_err(to_py_err)?;
        match record {
            Some(node) => {
                let value = serde_json::to_value(node)
                    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
                value_to_py(py, value).map(Some)
            }
            None => Ok(None),
        }
    })
}

#[pyfunction]
fn database_get_edge(
    py: Python<'_>,
    handle: &DatabaseHandle,
    edge_id: u64,
) -> PyResult<Option<PyObject>> {
    handle.with_db(|db| {
        let record = db.get_edge_record(edge_id).map_err(to_py_err)?;
        match record {
            Some(edge) => {
                let value = serde_json::to_value(edge)
                    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
                value_to_py(py, value).map(Some)
            }
            None => Ok(None),
        }
    })
}

#[pyfunction]
fn database_count_nodes_with_label(handle: &DatabaseHandle, label: &str) -> PyResult<u64> {
    handle.with_db(|db| db.count_nodes_with_label(label).map_err(to_py_err))
}

#[pyfunction]
fn database_count_edges_with_type(handle: &DatabaseHandle, ty: &str) -> PyResult<u64> {
    handle.with_db(|db| db.count_edges_with_type(ty).map_err(to_py_err))
}

#[pyfunction]
fn database_list_nodes_with_label(handle: &DatabaseHandle, label: &str) -> PyResult<Vec<u64>> {
    handle.with_db(|db| db.node_ids_with_label(label).map_err(to_py_err))
}

#[pyfunction]
fn database_neighbors(
    py: Python<'_>,
    handle: &DatabaseHandle,
    node_id: u64,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let parsed = parse_neighbor_options(options)?;
    handle.with_db(|db| {
        let neighbors = db
            .neighbors_with_options(
                node_id,
                parsed.direction,
                parsed.edge_type.as_deref(),
                parsed.distinct,
            )
            .map_err(to_py_err)?;
        let list = PyList::empty_bound(py);
        for entry in neighbors {
            let row = PyDict::new_bound(py);
            row.set_item("node_id", entry.node_id)?;
            row.set_item("edge_id", entry.edge_id)?;
            row.set_item("type_id", entry.type_id)?;
            list.append(row)?;
        }
        Ok(list.into_py(py))
    })
}

#[pyfunction]
fn database_bfs_traversal(
    py: Python<'_>,
    handle: &DatabaseHandle,
    start_id: u64,
    max_depth: u32,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let parsed = parse_bfs_options(options)?;
    handle.with_db(|db| {
        let visits = db
            .bfs_traversal(
                start_id,
                parsed.direction,
                max_depth,
                parsed.edge_types.as_deref(),
                parsed.max_results,
            )
            .map_err(to_py_err)?;
        let list = PyList::empty_bound(py);
        for visit in visits {
            let row = PyDict::new_bound(py);
            row.set_item("node_id", visit.node_id)?;
            row.set_item("depth", visit.depth)?;
            list.append(row)?;
        }
        Ok(list.into_py(py))
    })
}

#[pyfunction]
fn database_pragma_get(py: Python<'_>, handle: &DatabaseHandle, name: &str) -> PyResult<PyObject> {
    handle.with_db(|db| {
        let result = db.pragma(name, None).map_err(to_py_err)?;
        value_to_py(py, result)
    })
}

#[pyfunction]
fn database_pragma_set(
    py: Python<'_>,
    handle: &DatabaseHandle,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let payload = if value.is_none() {
        Value::Null
    } else {
        any_to_value(value)?
    };
    handle.with_db(|db| {
        let result = db.pragma(name, Some(payload)).map_err(to_py_err)?;
        value_to_py(py, result)
    })
}

#[pyfunction]
fn stream_next(py: Python<'_>, handle: &StreamHandle) -> PyResult<Option<PyObject>> {
    let mut guard = handle
        .inner
        .lock()
        .map_err(|_| PyRuntimeError::new_err("[CLOSED] stream handle lock poisoned"))?;
    let stream = guard
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("[CLOSED] stream is closed"))?;
    match stream.next().map_err(to_py_err)? {
        Some(value) => value_to_py(py, value).map(Some),
        None => Ok(None),
    }
}

#[pyfunction]
fn stream_close(handle: &StreamHandle) -> PyResult<()> {
    let mut guard = handle
        .inner
        .lock()
        .map_err(|_| PyRuntimeError::new_err("[CLOSED] stream handle lock poisoned"))?;
    guard.take();
    Ok(())
}

#[pyfunction]
fn version() -> PyResult<&'static str> {
    Ok(env!("CARGO_PKG_VERSION"))
}

#[pymodule]
fn _native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(open_database, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_close, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_is_closed, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_execute, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_explain, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_stream, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_mutate, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_create, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_intern, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_pragma_get, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_pragma_set, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_cancel_request, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_get_node, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_get_edge, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_count_nodes_with_label, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_count_edges_with_type, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_list_nodes_with_label, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_neighbors, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_bfs_traversal, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(stream_next, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(stream_close, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_seed_demo, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(version, m)?)?;
    m.add_class::<DatabaseHandle>()?;
    m.add_class::<StreamHandle>()?;
    Ok(())
}

fn to_py_err(err: FfiError) -> PyErr {
    let code_name = err.code_name();
    let message = err.to_string();
    PyRuntimeError::new_err(format!("[{code_name}] {message}"))
}

fn parse_direction(value: Option<&str>) -> PyResult<Dir> {
    match value.unwrap_or("out") {
        "out" => Ok(Dir::Out),
        "in" => Ok(Dir::In),
        "both" => Ok(Dir::Both),
        other => Err(PyRuntimeError::new_err(format!(
            "invalid direction '{other}', expected 'out', 'in', or 'both'"
        ))),
    }
}

fn parse_neighbor_options(options: Option<&Bound<'_, PyDict>>) -> PyResult<ParsedNeighborOptions> {
    let mut parsed = ParsedNeighborOptions {
        direction: Dir::Out,
        distinct: true,
        ..Default::default()
    };
    if let Some(dict) = options {
        if let Some(value) = dict.get_item("direction")? {
            let dir = value.extract::<String>()?;
            parsed.direction = parse_direction(Some(&dir))?;
        }
        if let Some(value) = dict.get_item("edge_type")? {
            let ty = value.extract::<String>()?;
            if ty.trim().is_empty() {
                return Err(PyRuntimeError::new_err(
                    "edge_type must be a non-empty string",
                ));
            }
            parsed.edge_type = Some(ty);
        } else if let Some(value) = dict.get_item("edgeType")? {
            let ty = value.extract::<String>()?;
            if ty.trim().is_empty() {
                return Err(PyRuntimeError::new_err(
                    "edgeType must be a non-empty string",
                ));
            }
            parsed.edge_type = Some(ty);
        }
        if let Some(value) = dict.get_item("distinct")? {
            parsed.distinct = value.extract::<bool>()?;
        }
    }
    Ok(parsed)
}

fn parse_bfs_options(options: Option<&Bound<'_, PyDict>>) -> PyResult<ParsedBfsOptions> {
    let mut parsed = ParsedBfsOptions {
        direction: Dir::Out,
        ..Default::default()
    };
    if let Some(dict) = options {
        if let Some(value) = dict.get_item("direction")? {
            let dir = value.extract::<String>()?;
            parsed.direction = parse_direction(Some(&dir))?;
        }
        if let Some(value) = dict.get_item("edge_types")? {
            let list = value.extract::<Vec<String>>()?;
            for ty in &list {
                if ty.trim().is_empty() {
                    return Err(PyRuntimeError::new_err(
                        "edge_types entries must be non-empty strings",
                    ));
                }
            }
            parsed.edge_types = Some(list);
        } else if let Some(value) = dict.get_item("edgeTypes")? {
            let list = value.extract::<Vec<String>>()?;
            for ty in &list {
                if ty.trim().is_empty() {
                    return Err(PyRuntimeError::new_err(
                        "edgeTypes entries must be non-empty strings",
                    ));
                }
            }
            parsed.edge_types = Some(list);
        }
        if let Some(value) = dict.get_item("max_results")? {
            parsed.max_results = Some(value.extract::<usize>()?);
        } else if let Some(value) = dict.get_item("maxResults")? {
            parsed.max_results = Some(value.extract::<usize>()?);
        }
    }
    Ok(parsed)
}

fn parse_synchronous(value: &str) -> PyResult<Synchronous> {
    Synchronous::from_str(value).ok_or_else(|| {
        PyRuntimeError::new_err(format!(
            "invalid synchronous mode '{value}', expected 'full', 'normal', or 'off'"
        ))
    })
}

fn any_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if obj.is_instance_of::<PyBool>() {
        return Ok(Value::Bool(obj.extract::<bool>()?));
    }
    if let Ok(v) = obj.extract::<i64>() {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = obj.extract::<u64>() {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = obj.extract::<f64>() {
        let number = serde_json::Number::from_f64(v)
            .ok_or_else(|| PyRuntimeError::new_err("NaN not supported"))?;
        return Ok(Value::Number(number));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            let key_str: String = key.extract()?;
            map.insert(key_str, any_to_value(&value)?);
        }
        return Ok(Value::Object(map));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for element in list.iter() {
            items.push(any_to_value(&element)?);
        }
        return Ok(Value::Array(items));
    }
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let mut items = Vec::with_capacity(tuple.len());
        for element in tuple.iter() {
            items.push(any_to_value(&element)?);
        }
        return Ok(Value::Array(items));
    }
    Err(PyRuntimeError::new_err("unsupported value in query spec"))
}

fn value_to_py(py: Python<'_>, value: Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(v) => Ok(v.into_py(py)),
        Value::Number(num) => {
            if let Some(v) = num.as_i64() {
                Ok(v.into_py(py))
            } else if let Some(v) = num.as_u64() {
                Ok(v.into_py(py))
            } else if let Some(v) = num.as_f64() {
                Ok(v.into_py(py))
            } else {
                Err(PyRuntimeError::new_err("number out of range"))
            }
        }
        Value::String(s) => Ok(s.into_py(py)),
        Value::Array(arr) => {
            let list = PyList::empty_bound(py);
            for item in arr {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into_py(py))
        }
        Value::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (key, val) in map {
                dict.set_item(key, value_to_py(py, val)?)?;
            }
            Ok(dict.into_py(py))
        }
    }
}
