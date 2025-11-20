use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::admin::{open_graph, AdminOpenOptions, CheckpointMode, GraphHandle};
use crate::primitives::pager::{PageStore, Pager, ReadGuard, WriteGuard};
use crate::storage::catalog::Dict;
use crate::storage::{
    index::IndexDef, BulkEdgeValidator, CreateEdgeOptions, EdgeSpec, Graph, GraphWriter, NodeSpec,
    PropEntry, PropValue, PropValueOwned,
};
use crate::types::{LabelId, NodeId, PropId, SombraError, StrId, TypeId};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use thiserror::Error;
use time::format_description::{well_known::Rfc3339, FormatItem};
use time::macros::format_description;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime};

const NODE_BATCH_SIZE: usize = 256;
const EDGE_BATCH_SIZE: usize = 512;

const DATE_FMT: &[FormatItem<'static>] = format_description!("[year]-[month]-[day]");
const DATETIME_FMT_T: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
const DATETIME_FMT_T_FRAC: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");
const DATETIME_FMT_SPACE: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
const DATETIME_FMT_SPACE_FRAC: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");

type ExtIdMap = HashMap<String, u64>;

struct NodeInsert {
    ext_id: String,
    labels: Vec<String>,
    props: Vec<PropInput>,
}

struct EdgeInsert {
    src: u64,
    dst: u64,
    ty: String,
    props: Vec<PropInput>,
}

#[derive(Clone, Debug)]
struct PropInput {
    name: String,
    value: PropValueOwned,
}

/// Supported property type coercions for CSV imports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropertyType {
    /// Let the importer infer the type from literals (default).
    Auto,
    /// Boolean literal (`true`/`false`).
    Bool,
    /// 64-bit signed integer.
    Int,
    /// 64-bit floating point value.
    Float,
    /// UTF-8 string literal (disables heuristics).
    String,
    /// Calendar date (epoch days or `YYYY-MM-DD`).
    Date,
    /// Timestamp (epoch milliseconds or RFC3339).
    DateTime,
    /// `0x`-prefixed hex blob.
    Bytes,
}

impl Default for PropertyType {
    fn default() -> Self {
        Self::Auto
    }
}

/// Configuration for importing nodes from a CSV file.
#[derive(Debug, Clone)]
pub struct NodeImportConfig {
    /// Path to the CSV file containing node data.
    pub path: PathBuf,
    /// Name of the CSV column containing unique node identifiers.
    pub id_column: String,
    /// Optional CSV column name containing node labels (pipe-separated).
    pub label_column: Option<String>,
    /// Static labels to apply to all imported nodes.
    pub static_labels: Vec<String>,
    /// Optional list of CSV columns to import as node properties.
    /// If None, all columns except id and label columns are imported.
    pub prop_columns: Option<Vec<String>>,
    /// Explicit property type overrides keyed by column name.
    pub prop_types: HashMap<String, PropertyType>,
}

/// Configuration for importing edges from a CSV file.
#[derive(Debug, Clone)]
pub struct EdgeImportConfig {
    /// Path to the CSV file containing edge data.
    pub path: PathBuf,
    /// Name of the CSV column containing source node identifiers.
    pub src_column: String,
    /// Name of the CSV column containing destination node identifiers.
    pub dst_column: String,
    /// Optional CSV column name containing edge type.
    pub type_column: Option<String>,
    /// Static edge type to apply to all imported edges.
    pub static_type: Option<String>,
    /// Optional list of CSV columns to import as edge properties.
    /// If None, all columns except src, dst, and type columns are imported.
    pub prop_columns: Option<Vec<String>>,
    /// Whether to trust endpoints after validator approval.
    pub trusted_endpoints: bool,
    /// Cache capacity for endpoint existence probes (0 disables caching).
    pub exists_cache_capacity: usize,
    /// Explicit property type overrides keyed by column name.
    pub prop_types: HashMap<String, PropertyType>,
}

/// Configuration for the complete import operation.
#[derive(Debug, Clone)]
pub struct ImportConfig {
    /// Path to the database file.
    pub db_path: PathBuf,
    /// Whether to create the database if it doesn't exist.
    pub create_if_missing: bool,
    /// Whether to drop property indexes before importing.
    pub disable_indexes: bool,
    /// Whether to rebuild property indexes after import (requires disable).
    pub build_indexes: bool,
    /// Optional configuration for node import.
    pub nodes: Option<NodeImportConfig>,
    /// Optional configuration for edge import.
    pub edges: Option<EdgeImportConfig>,
}

/// Summary statistics from an import operation.
#[derive(Debug, Clone, Default)]
pub struct ImportSummary {
    /// Total number of nodes imported.
    pub nodes_imported: u64,
    /// Total number of edges imported.
    pub edges_imported: u64,
}

/// Configuration for exporting graph data to CSV files.
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// Path to the database file.
    pub db_path: PathBuf,
    /// Optional output path for exported nodes CSV.
    pub nodes_out: Option<PathBuf>,
    /// Optional output path for exported edges CSV.
    pub edges_out: Option<PathBuf>,
    /// List of property names to include in node export.
    pub node_props: Vec<String>,
    /// List of property names to include in edge export.
    pub edge_props: Vec<String>,
}

/// Summary statistics from an export operation.
#[derive(Debug, Clone, Default)]
pub struct ExportSummary {
    /// Total number of nodes exported.
    pub nodes_exported: u64,
    /// Total number of edges exported.
    pub edges_exported: u64,
}

/// Error type for CLI import/export operations.
#[derive(Error, Debug)]
pub enum CliError {
    /// Generic error message.
    #[error("{0}")]
    Message(String),
    /// IO error from file operations.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// CSV parsing or writing error.
    #[error(transparent)]
    Csv(#[from] csv::Error),
    /// Admin operation error.
    #[error(transparent)]
    Admin(#[from] crate::admin::AdminError),
    /// Storage layer error.
    #[error(transparent)]
    Storage(#[from] SombraError),
}

impl From<&str> for CliError {
    fn from(value: &str) -> Self {
        CliError::Message(value.to_string())
    }
}

impl From<String> for CliError {
    fn from(value: String) -> Self {
        CliError::Message(value)
    }
}

/// Executes a complete import operation from CSV files into the graph database.
///
/// This function imports nodes first, building an ID mapping, and then optionally imports edges.
/// The database is checkpointed after a successful import.
///
/// # Arguments
/// * `cfg` - Import configuration specifying input files and options
/// * `opts` - Admin options for opening the database
///
/// # Returns
/// An `ImportSummary` with counts of imported nodes and edges, or a `CliError` on failure.
pub fn run_import(cfg: &ImportConfig, opts: &AdminOpenOptions) -> Result<ImportSummary, CliError> {
    let nodes_cfg = cfg
        .nodes
        .as_ref()
        .ok_or_else(|| CliError::Message("--nodes is required for import".into()))?;
    if cfg.build_indexes && !cfg.disable_indexes {
        return Err(CliError::Message(
            "--build-indexes requires --disable-indexes".into(),
        ));
    }

    if !cfg.db_path.exists() {
        if cfg.create_if_missing {
            if let Some(parent) = cfg.db_path.parent() {
                fs::create_dir_all(parent)?;
            }
        } else {
            return Err(CliError::Message(format!(
                "database {} does not exist (use --create to initialize)",
                cfg.db_path.display()
            )));
        }
    }

    let mut summary = ImportSummary::default();
    let mut dropped_indexes = Vec::new();
    {
        let handle = open_graph(&cfg.db_path, opts)?;
        if cfg.disable_indexes {
            dropped_indexes = drop_all_property_indexes(&handle)?;
        }
        let mut id_map: ExtIdMap = ExtIdMap::new();
        summary.nodes_imported = import_nodes(&handle, nodes_cfg, &mut id_map)?;

        if let Some(edges_cfg) = &cfg.edges {
            if id_map.is_empty() {
                return Err(CliError::Message(
                    "cannot import edges without node id mapping".into(),
                ));
            }
            summary.edges_imported = import_edges(&handle, edges_cfg, &mut id_map)?;
        }

        handle.pager.checkpoint(CheckpointMode::BestEffort)?;
    }

    if cfg.build_indexes {
        let handle = open_graph(&cfg.db_path, opts)?;
        rebuild_property_indexes(&handle, &dropped_indexes)?;
        handle.pager.checkpoint(CheckpointMode::BestEffort)?;
    }

    Ok(summary)
}

/// Executes a complete export operation from the graph database to CSV files.
///
/// This function exports nodes and/or edges to CSV files with specified properties.
///
/// # Arguments
/// * `cfg` - Export configuration specifying output files and properties
/// * `opts` - Admin options for opening the database
///
/// # Returns
/// An `ExportSummary` with counts of exported nodes and edges, or a `CliError` on failure.
pub fn run_export(cfg: &ExportConfig, opts: &AdminOpenOptions) -> Result<ExportSummary, CliError> {
    if cfg.nodes_out.is_none() && cfg.edges_out.is_none() {
        return Err(CliError::Message(
            "export requires --nodes and/or --edges output paths".into(),
        ));
    }

    let handle = open_graph(&cfg.db_path, opts)?;
    let read = handle.pager.begin_latest_committed_read()?;
    let mut summary = ExportSummary::default();

    if let Some(path) = &cfg.nodes_out {
        summary.nodes_exported = export_nodes(&handle, &read, path, &cfg.node_props)?;
    }
    if let Some(path) = &cfg.edges_out {
        summary.edges_exported = export_edges(&handle, &read, path, &cfg.edge_props)?;
    }

    Ok(summary)
}

struct ColumnSpec {
    name: String,
    index: usize,
    prop_type: PropertyType,
}

fn import_nodes(
    handle: &GraphHandle,
    cfg: &NodeImportConfig,
    id_map: &mut ExtIdMap,
) -> Result<u64, CliError> {
    let mut reader = ReaderBuilder::new().flexible(true).from_path(&cfg.path)?;
    let headers = reader.headers()?.clone();
    let id_index = find_column(&headers, &cfg.id_column)?;
    let label_index = match &cfg.label_column {
        Some(col) => Some(find_column(&headers, col)?),
        None => None,
    };

    let mut skip = vec![id_index];
    if let Some(idx) = label_index {
        skip.push(idx);
    }
    let prop_columns = resolve_prop_columns(&headers, &cfg.prop_columns, &cfg.prop_types, &skip)?;

    let mut batch: Vec<NodeInsert> = Vec::with_capacity(NODE_BATCH_SIZE);
    let mut imported = 0u64;

    for result in reader.records() {
        let record = result?;
        let ext_id = record
            .get(id_index)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                CliError::Message(format!(
                    "missing value for node id column '{}'",
                    cfg.id_column
                ))
            })?;
        if id_map.contains_key(ext_id) {
            return Err(CliError::Message(format!(
                "duplicate node id '{ext_id}' in nodes file"
            )));
        }

        let mut labels = cfg.static_labels.clone();
        if let Some(idx) = label_index {
            if let Some(raw) = record.get(idx) {
                let parsed = parse_labels(raw);
                labels.extend(parsed);
            }
        }
        if labels.is_empty() {
            return Err(CliError::Message(format!(
                "row with id '{ext_id}' has no labels (provide --node-labels or --node-label-column)"
            )));
        }

        let props = build_props(&record, &prop_columns)?;
        batch.push(NodeInsert {
            ext_id: ext_id.to_string(),
            labels,
            props,
        });

        if batch.len() >= NODE_BATCH_SIZE {
            imported += flush_node_batch(handle, &mut batch, id_map)?;
        }
    }
    imported += flush_node_batch(handle, &mut batch, id_map)?;
    Ok(imported)
}

fn import_edges(
    handle: &GraphHandle,
    cfg: &EdgeImportConfig,
    id_map: &mut ExtIdMap,
) -> Result<u64, CliError> {
    if cfg.static_type.is_none() && cfg.type_column.is_none() {
        return Err(CliError::Message(
            "edge import requires --edge-type or --edge-type-column".into(),
        ));
    }
    let mut reader = ReaderBuilder::new().flexible(true).from_path(&cfg.path)?;
    let headers = reader.headers()?.clone();
    let src_index = find_column(&headers, &cfg.src_column)?;
    let dst_index = find_column(&headers, &cfg.dst_column)?;
    let ty_index = match (&cfg.type_column, &cfg.static_type) {
        (Some(col), _) => Some(find_column(&headers, col)?),
        (None, Some(_)) => None,
        _ => None,
    };

    let mut skip = vec![src_index, dst_index];
    if let Some(idx) = ty_index {
        skip.push(idx);
    }
    let prop_columns = resolve_prop_columns(&headers, &cfg.prop_columns, &cfg.prop_types, &skip)?;

    let validator = cfg.trusted_endpoints.then(|| {
        Box::new(SnapshotEdgeValidator::new(
            Arc::clone(&handle.pager),
            Arc::clone(&handle.graph),
        )) as Box<dyn BulkEdgeValidator>
    });
    let writer_opts = CreateEdgeOptions {
        trusted_endpoints: cfg.trusted_endpoints,
        exists_cache_capacity: cfg.exists_cache_capacity,
    };
    let mut writer = GraphWriter::try_new(handle.graph.as_ref(), writer_opts, validator)?;

    let mut batch: Vec<EdgeInsert> = Vec::with_capacity(EDGE_BATCH_SIZE);
    let mut imported = 0u64;

    for result in reader.records() {
        let record = result?;
        let src_ext = get_required(&record, src_index, &cfg.src_column)?;
        let dst_ext = get_required(&record, dst_index, &cfg.dst_column)?;
        let src = *id_map.get(src_ext).ok_or_else(|| {
            CliError::Message(format!("edge references unknown src id '{src_ext}'"))
        })?;
        let dst = *id_map.get(dst_ext).ok_or_else(|| {
            CliError::Message(format!("edge references unknown dst id '{dst_ext}'"))
        })?;

        let ty_value = match (&cfg.static_type, ty_index) {
            (Some(value), _) => value.clone(),
            (None, Some(idx)) => record
                .get(idx)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| CliError::Message("edge type column is empty".into()))?,
            _ => unreachable!(),
        };

        let props = build_props(&record, &prop_columns)?;
        batch.push(EdgeInsert {
            src,
            dst,
            ty: ty_value,
            props,
        });

        if batch.len() >= EDGE_BATCH_SIZE {
            imported += flush_edge_batch(handle, cfg, &mut writer, &mut batch)?;
        }
    }

    imported += flush_edge_batch(handle, cfg, &mut writer, &mut batch)?;
    Ok(imported)
}

fn flush_node_batch(
    handle: &GraphHandle,
    batch: &mut Vec<NodeInsert>,
    id_map: &mut ExtIdMap,
) -> Result<u64, CliError> {
    if batch.is_empty() {
        return Ok(0);
    }
    let mut write = handle.pager.begin_write()?;
    let mut labels_buf: Vec<LabelId> = Vec::new();
    let mut created = 0u64;
    for node in batch.drain(..) {
        labels_buf.clear();
        resolve_labels(&handle.dict, &mut write, &node.labels, &mut labels_buf)?;
        let prop_storage = collect_prop_storage(&handle.dict, &mut write, &node.props)?;
        let prop_entries: Vec<PropEntry<'_>> = prop_storage
            .iter()
            .map(|(prop, owned)| PropEntry::new(*prop, prop_value_ref(owned)))
            .collect();
        let spec = NodeSpec {
            labels: &labels_buf,
            props: &prop_entries,
        };
        let node_id = handle.graph.create_node(&mut write, spec)?;
        id_map.insert(node.ext_id, node_id.0);
        created += 1;
    }
    handle.pager.commit(write)?;
    Ok(created)
}

fn flush_edge_batch(
    handle: &GraphHandle,
    cfg: &EdgeImportConfig,
    writer: &mut GraphWriter<'_>,
    batch: &mut Vec<EdgeInsert>,
) -> Result<u64, CliError> {
    if batch.is_empty() {
        return Ok(0);
    }
    if cfg.trusted_endpoints {
        let pairs: Vec<(NodeId, NodeId)> = batch
            .iter()
            .map(|edge| (NodeId(edge.src), NodeId(edge.dst)))
            .collect();
        writer.validate_trusted_batch(&pairs)?;
    }
    let mut write = handle.pager.begin_write()?;
    let mut created = 0u64;
    for edge in batch.drain(..) {
        let ty_id = resolve_type(&handle.dict, &mut write, &edge.ty)?;
        let prop_storage = collect_prop_storage(&handle.dict, &mut write, &edge.props)?;
        let prop_entries: Vec<PropEntry<'_>> = prop_storage
            .iter()
            .map(|(prop, owned)| PropEntry::new(*prop, prop_value_ref(owned)))
            .collect();
        let spec = EdgeSpec {
            src: NodeId(edge.src),
            dst: NodeId(edge.dst),
            ty: ty_id,
            props: &prop_entries,
        };
        let _edge_id = writer.create_edge(&mut write, spec)?;
        created += 1;
    }
    handle.pager.commit(write)?;
    Ok(created)
}

struct SnapshotEdgeValidator {
    pager: Arc<Pager>,
    graph: Arc<Graph>,
}

impl SnapshotEdgeValidator {
    fn new(pager: Arc<Pager>, graph: Arc<Graph>) -> Self {
        Self { pager, graph }
    }
}

impl BulkEdgeValidator for SnapshotEdgeValidator {
    fn validate_batch(&self, edges: &[(NodeId, NodeId)]) -> crate::types::Result<()> {
        if edges.is_empty() {
            return Ok(());
        }
        let read = self.pager.begin_latest_committed_read()?;
        for (src, dst) in edges {
            if !self.graph.node_exists(&read, *src)? {
                return Err(SombraError::Invalid(
                    "trusted source endpoint missing during validation",
                ));
            }
            if !self.graph.node_exists(&read, *dst)? {
                return Err(SombraError::Invalid(
                    "trusted destination endpoint missing during validation",
                ));
            }
        }
        Ok(())
    }
}

fn resolve_labels(
    dict: &Arc<Dict>,
    write: &mut WriteGuard<'_>,
    labels: &[String],
    out: &mut Vec<LabelId>,
) -> Result<(), CliError> {
    for label in labels {
        let id = dict.intern(write, label)?;
        out.push(LabelId(id.0));
    }
    Ok(())
}

fn resolve_type(
    dict: &Arc<Dict>,
    write: &mut WriteGuard<'_>,
    ty: &str,
) -> Result<TypeId, CliError> {
    let id = dict.intern(write, ty)?;
    Ok(TypeId(id.0))
}

fn collect_prop_storage(
    dict: &Arc<Dict>,
    write: &mut WriteGuard<'_>,
    props: &[PropInput],
) -> Result<Vec<(PropId, PropValueOwned)>, CliError> {
    let mut storage = Vec::with_capacity(props.len());
    for prop in props {
        let id = dict.intern(write, &prop.name)?;
        storage.push((PropId(id.0), prop.value.clone()));
    }
    Ok(storage)
}

fn drop_all_property_indexes(handle: &GraphHandle) -> Result<Vec<IndexDef>, CliError> {
    let defs = handle.graph.all_property_indexes()?;
    if defs.is_empty() {
        return Ok(defs);
    }
    let mut write = handle.pager.begin_write()?;
    for def in &defs {
        handle
            .graph
            .drop_property_index(&mut write, def.label, def.prop)?;
    }
    handle.pager.commit(write)?;
    Ok(defs)
}

fn rebuild_property_indexes(handle: &GraphHandle, defs: &[IndexDef]) -> Result<(), CliError> {
    if defs.is_empty() {
        return Ok(());
    }
    let mut write = handle.pager.begin_write()?;
    for def in defs {
        handle.graph.create_property_index(&mut write, *def)?;
    }
    handle.pager.commit(write)?;
    Ok(())
}

fn resolve_prop_columns(
    headers: &StringRecord,
    requested: &Option<Vec<String>>,
    types: &HashMap<String, PropertyType>,
    skip: &[usize],
) -> Result<Vec<ColumnSpec>, CliError> {
    if let Some(list) = requested {
        let mut cols = Vec::with_capacity(list.len());
        for name in list {
            let idx = find_column(headers, name)?;
            cols.push(ColumnSpec {
                name: name.clone(),
                index: idx,
                prop_type: types
                    .get(&name.to_ascii_lowercase())
                    .copied()
                    .unwrap_or_default(),
            });
        }
        Ok(cols)
    } else {
        let mut cols = Vec::new();
        for (idx, header) in headers.iter().enumerate() {
            if skip.contains(&idx) {
                continue;
            }
            cols.push(ColumnSpec {
                name: header.to_string(),
                index: idx,
                prop_type: types
                    .get(&header.to_ascii_lowercase())
                    .copied()
                    .unwrap_or_default(),
            });
        }
        Ok(cols)
    }
}

fn find_column(headers: &StringRecord, name: &str) -> Result<usize, CliError> {
    headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case(name))
        .ok_or_else(|| CliError::Message(format!("column '{name}' not found")))
}

fn get_required<'a>(record: &'a StringRecord, idx: usize, name: &str) -> Result<&'a str, CliError> {
    record
        .get(idx)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CliError::Message(format!("missing value for column '{name}'")))
}

fn parse_labels(raw: &str) -> Vec<String> {
    raw.split('|')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn build_props(record: &StringRecord, columns: &[ColumnSpec]) -> Result<Vec<PropInput>, CliError> {
    let mut props = Vec::new();
    for col in columns {
        if let Some(raw) = record.get(col.index) {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            if let Some(value) = parse_literal(raw, col.prop_type)? {
                props.push(PropInput {
                    name: col.name.clone(),
                    value,
                });
            }
        }
    }
    Ok(props)
}

fn parse_literal(raw: &str, ty: PropertyType) -> Result<Option<PropValueOwned>, CliError> {
    if !matches!(ty, PropertyType::String) && raw.eq_ignore_ascii_case("null") {
        return Ok(None);
    }
    let value = match ty {
        PropertyType::Auto => return parse_auto_literal(raw),
        PropertyType::String => PropValueOwned::Str(raw.to_string()),
        PropertyType::Bool => {
            let parsed = parse_bool_literal(raw)
                .ok_or_else(|| CliError::Message(format!("invalid boolean literal '{raw}'")))?;
            PropValueOwned::Bool(parsed)
        }
        PropertyType::Int => {
            let parsed = raw
                .parse::<i64>()
                .map_err(|_| CliError::Message(format!("invalid integer literal '{raw}'")))?;
            PropValueOwned::Int(parsed)
        }
        PropertyType::Float => {
            let parsed = raw
                .parse::<f64>()
                .map_err(|_| CliError::Message(format!("invalid float literal '{raw}'")))?;
            PropValueOwned::Float(parsed)
        }
        PropertyType::Date => {
            if let Ok(days) = raw.parse::<i64>() {
                PropValueOwned::Date(days)
            } else {
                PropValueOwned::Date(parse_iso_date(raw)?)
            }
        }
        PropertyType::DateTime => {
            if let Ok(ms) = raw.parse::<i64>() {
                PropValueOwned::DateTime(ms)
            } else {
                PropValueOwned::DateTime(parse_iso_datetime(raw)?)
            }
        }
        PropertyType::Bytes => {
            let bytes = parse_bytes_literal(raw)?;
            PropValueOwned::Bytes(bytes)
        }
    };
    Ok(Some(value))
}

fn parse_auto_literal(raw: &str) -> Result<Option<PropValueOwned>, CliError> {
    if raw.eq_ignore_ascii_case("null") {
        return Ok(None);
    }
    if let Some(val) = parse_bool_literal(raw) {
        return Ok(Some(PropValueOwned::Bool(val)));
    }
    if let Ok(int_val) = raw.parse::<i64>() {
        return Ok(Some(PropValueOwned::Int(int_val)));
    }
    if let Ok(float_val) = raw.parse::<f64>() {
        return Ok(Some(PropValueOwned::Float(float_val)));
    }
    if raw.starts_with("0x") || raw.starts_with("0X") {
        let bytes = parse_bytes_literal(raw)?;
        return Ok(Some(PropValueOwned::Bytes(bytes)));
    }
    if looks_like_datetime(raw) {
        let millis = parse_iso_datetime(raw)?;
        return Ok(Some(PropValueOwned::DateTime(millis)));
    }
    if looks_like_date(raw) {
        let days = parse_iso_date(raw)?;
        return Ok(Some(PropValueOwned::Date(days)));
    }
    Ok(Some(PropValueOwned::Str(raw.to_string())))
}

fn parse_bool_literal(raw: &str) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_bytes_literal(raw: &str) -> Result<Vec<u8>, CliError> {
    let body = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .ok_or_else(|| CliError::Message("byte literal must start with 0x".into()))?;
    if body.len() % 2 != 0 {
        return Err(CliError::Message(
            "byte literal must contain an even number of hex digits".into(),
        ));
    }
    hex::decode(body)
        .map_err(|_| CliError::Message(format!("byte literal '{raw}' contains non-hex characters")))
}

fn looks_like_date(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    bytes.len() >= 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

fn looks_like_datetime(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    if bytes.len() < 16 || !looks_like_date(raw) {
        return false;
    }
    matches!(bytes[10], b'T' | b' ')
}

fn parse_iso_date(raw: &str) -> Result<i64, CliError> {
    let date = Date::parse(raw, DATE_FMT)
        .map_err(|_| CliError::Message(format!("invalid date literal '{raw}'")))?;
    let epoch = Date::from_calendar_date(1970, Month::January, 1)
        .expect("1970-01-01 is a valid calendar date");
    Ok((date - epoch).whole_days())
}

fn parse_iso_datetime(raw: &str) -> Result<i64, CliError> {
    if let Ok(dt) = OffsetDateTime::parse(raw, &Rfc3339) {
        return nanos_to_millis(dt.unix_timestamp_nanos());
    }
    if let Ok(dt) = PrimitiveDateTime::parse(raw, DATETIME_FMT_T) {
        return nanos_to_millis(dt.assume_utc().unix_timestamp_nanos());
    }
    if let Ok(dt) = PrimitiveDateTime::parse(raw, DATETIME_FMT_T_FRAC) {
        return nanos_to_millis(dt.assume_utc().unix_timestamp_nanos());
    }
    if let Ok(dt) = PrimitiveDateTime::parse(raw, DATETIME_FMT_SPACE) {
        return nanos_to_millis(dt.assume_utc().unix_timestamp_nanos());
    }
    if let Ok(dt) = PrimitiveDateTime::parse(raw, DATETIME_FMT_SPACE_FRAC) {
        return nanos_to_millis(dt.assume_utc().unix_timestamp_nanos());
    }
    Err(CliError::Message(format!(
        "invalid datetime literal '{raw}', expected RFC3339 or 'YYYY-MM-DD HH:MM:SS'"
    )))
}

fn nanos_to_millis(nanos: i128) -> Result<i64, CliError> {
    let millis = nanos / 1_000_000;
    if millis < i64::MIN as i128 || millis > i64::MAX as i128 {
        return Err(CliError::Message(
            "datetime literal is outside the supported range".into(),
        ));
    }
    Ok(millis as i64)
}

fn prop_value_ref(value: &PropValueOwned) -> PropValue<'_> {
    match value {
        PropValueOwned::Null => PropValue::Null,
        PropValueOwned::Bool(v) => PropValue::Bool(*v),
        PropValueOwned::Int(v) => PropValue::Int(*v),
        PropValueOwned::Float(v) => PropValue::Float(*v),
        PropValueOwned::Str(v) => PropValue::Str(v.as_str()),
        PropValueOwned::Bytes(v) => PropValue::Bytes(v.as_slice()),
        PropValueOwned::Date(v) => PropValue::Date(*v),
        PropValueOwned::DateTime(v) => PropValue::DateTime(*v),
    }
}

fn export_nodes(
    handle: &GraphHandle,
    read: &ReadGuard,
    path: &Path,
    props: &[String],
) -> Result<u64, CliError> {
    let mut writer = WriterBuilder::new().from_path(path)?;
    let mut header = Vec::with_capacity(2 + props.len());
    header.push("id".to_string());
    header.push("labels".to_string());
    header.extend(props.iter().cloned());
    writer.write_record(&header)?;

    let nodes = handle.graph.scan_all_nodes(read)?;
    for (node_id, data) in nodes.iter() {
        let labels = format_labels(handle, read, &data.labels)?;
        let prop_map = materialize_props(handle, read, &data.props)?;
        let mut row = Vec::with_capacity(header.len());
        row.push(node_id.0.to_string());
        row.push(labels);
        for prop in props {
            row.push(prop_map.get(prop).cloned().unwrap_or_default());
        }
        writer.write_record(&row)?;
    }
    writer.flush()?;
    Ok(nodes.len() as u64)
}

fn export_edges(
    handle: &GraphHandle,
    read: &ReadGuard,
    path: &Path,
    props: &[String],
) -> Result<u64, CliError> {
    let mut writer = WriterBuilder::new().from_path(path)?;
    let mut header = Vec::with_capacity(3 + props.len());
    header.push("src".to_string());
    header.push("dst".to_string());
    header.push("type".to_string());
    header.extend(props.iter().cloned());
    writer.write_record(&header)?;

    let edges = handle.graph.scan_all_edges(read)?;
    for (_, data) in edges.iter() {
        let ty_name = resolve_name(handle, read, data.ty.0, "TYPE")?;
        let prop_map = materialize_props(handle, read, &data.props)?;
        let mut row = Vec::with_capacity(header.len());
        row.push(data.src.0.to_string());
        row.push(data.dst.0.to_string());
        row.push(ty_name);
        for prop in props {
            row.push(prop_map.get(prop).cloned().unwrap_or_default());
        }
        writer.write_record(&row)?;
    }
    writer.flush()?;
    Ok(edges.len() as u64)
}

fn format_labels(
    handle: &GraphHandle,
    read: &ReadGuard,
    labels: &[crate::types::LabelId],
) -> Result<String, CliError> {
    let mut resolved = Vec::with_capacity(labels.len());
    for label in labels {
        resolved.push(resolve_name(handle, read, label.0, "LABEL")?);
    }
    Ok(resolved.join("|"))
}

fn materialize_props(
    handle: &GraphHandle,
    read: &ReadGuard,
    props: &[(crate::types::PropId, PropValueOwned)],
) -> Result<HashMap<String, String>, CliError> {
    let mut map = HashMap::new();
    for (prop, value) in props {
        let name = resolve_name(handle, read, prop.0, "PROP")?;
        map.insert(name, format_prop_value(value));
    }
    Ok(map)
}

fn resolve_name(
    handle: &GraphHandle,
    read: &ReadGuard,
    raw: u32,
    prefix: &str,
) -> Result<String, CliError> {
    match handle.dict.resolve(read, StrId(raw)) {
        Ok(val) => Ok(val),
        Err(_) => Ok(format!("{prefix}#{raw}")),
    }
}

fn format_prop_value(value: &PropValueOwned) -> String {
    match value {
        PropValueOwned::Null => String::new(),
        PropValueOwned::Bool(v) => v.to_string(),
        PropValueOwned::Int(v) => v.to_string(),
        PropValueOwned::Float(v) => v.to_string(),
        PropValueOwned::Str(v) => v.clone(),
        PropValueOwned::Bytes(bytes) => format!("0x{}", hex::encode(bytes)),
        PropValueOwned::Date(v) => v.to_string(),
        PropValueOwned::DateTime(v) => v.to_string(),
    }
}
