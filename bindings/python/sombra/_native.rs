#![forbid(unsafe_code)]

use std::sync::Arc;

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
            autocheckpoint_ms: None,
        }
    }
}

#[pyclass(module = "sombra._native", unsendable)]
pub struct DatabaseHandle {
    inner: Arc<Database>,
}

#[pyclass(module = "sombra._native", unsendable)]
pub struct StreamHandle {
    inner: QueryStream,
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
    if let Some(ms) = opts.commit_coalesce_ms {
        pager.wal_commit_coalesce_ms = ms as u64;
    }
    if let Some(frames) = opts.commit_max_frames {
        pager.wal_commit_max_frames = frames as usize;
    }
    if let Some(commits) = opts.commit_max_commits {
        pager.wal_commit_max_commits = commits as usize;
    }
    if let Some(ms) = opts.autocheckpoint_ms {
        pager.autocheckpoint_ms = Some(ms as u64);
    }

    let db_opts = DatabaseOptions {
        create_if_missing: opts.create_if_missing,
        pager,
        distinct_neighbors_default: opts.distinct_neighbors_default,
    };

    let db = Database::open(path, db_opts).map_err(to_py_err)?;
    Ok(DatabaseHandle {
        inner: Arc::new(db),
    })
}

#[pyfunction]
fn database_execute(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    let response = handle.inner.execute_json(&value).map_err(to_py_err)?;
    value_to_py(py, response)
}

#[pyfunction]
fn database_explain(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    let explain = handle.inner.explain_json(&value).map_err(to_py_err)?;
    value_to_py(py, explain)
}

#[pyfunction]
fn database_stream(handle: &DatabaseHandle, spec: &Bound<'_, PyAny>) -> PyResult<StreamHandle> {
    let value = any_to_value(spec)?;
    let stream = handle.inner.stream_json(&value).map_err(to_py_err)?;
    Ok(StreamHandle { inner: stream })
}

#[pyfunction]
fn database_mutate(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    let summary = handle.inner.mutate_json(&value).map_err(to_py_err)?;
    value_to_py(py, summary)
}

#[pyfunction]
fn database_create(
    py: Python<'_>,
    handle: &DatabaseHandle,
    spec: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let value = any_to_value(spec)?;
    let summary = handle.inner.create_json(&value).map_err(to_py_err)?;
    value_to_py(py, summary)
}

#[pyfunction]
fn database_intern(handle: &DatabaseHandle, name: &str) -> PyResult<u32> {
    handle.inner.intern(name).map_err(to_py_err)
}

#[pyfunction]
fn database_seed_demo(handle: &DatabaseHandle) -> PyResult<()> {
    handle.inner.seed_demo().map_err(to_py_err)
}

#[pyfunction]
fn database_cancel_request(handle: &DatabaseHandle, request_id: &str) -> PyResult<bool> {
    Ok(handle.inner.cancel_request(request_id))
}

#[pyfunction]
fn database_pragma_get(py: Python<'_>, handle: &DatabaseHandle, name: &str) -> PyResult<PyObject> {
    let result = handle.inner.pragma(name, None).map_err(to_py_err)?;
    value_to_py(py, result)
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
    let result = handle
        .inner
        .pragma(name, Some(payload))
        .map_err(to_py_err)?;
    value_to_py(py, result)
}

#[pyfunction]
fn stream_next(py: Python<'_>, handle: &StreamHandle) -> PyResult<Option<PyObject>> {
    match handle.inner.next().map_err(to_py_err)? {
        Some(value) => value_to_py(py, value).map(Some),
        None => Ok(None),
    }
}

#[pyfunction]
fn version() -> PyResult<&'static str> {
    Ok(env!("CARGO_PKG_VERSION"))
}

#[pymodule]
fn _native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(open_database, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_execute, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_explain, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_stream, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_mutate, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_create, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_intern, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_pragma_get, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_pragma_set, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_cancel_request, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(stream_next, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(database_seed_demo, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(version, m)?)?;
    m.add_class::<DatabaseHandle>()?;
    m.add_class::<StreamHandle>()?;
    Ok(())
}

fn to_py_err(err: FfiError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
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
