//! Binary entry point for the Sombra administrative CLI.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::error::Error;
use std::net::IpAddr;
use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use sombra::{
    admin::{
        checkpoint, stats, vacuum_into, verify, AdminOpenOptions, CheckpointMode, PagerOptions,
        VacuumOptions, VerifyLevel,
    },
    cli::import_export::{
        run_export, run_import, CliError, EdgeImportConfig, ExportConfig, ImportConfig,
        NodeImportConfig, PropertyType,
    },
    dashboard::{self, DashboardOptions as DashboardServeOptions},
    ffi::{self, DatabaseOptions},
    primitives::pager::Synchronous,
};

#[derive(Parser, Debug)]
#[command(
    name = "sombra",
    version,
    about = "Administrative CLI for the Sombra database",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(flatten)]
    open: OpenArgs,

    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = OutputFormat::Text,
        help = "Output format for structured responses"
    )]
    format: OutputFormat,

    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Debug)]
struct OpenArgs {
    #[arg(
        long,
        help = "Override pager page size (bytes) when creating a database"
    )]
    page_size: Option<u32>,

    #[arg(long, help = "Override pager cache size (pages)")]
    cache_pages: Option<usize>,

    #[arg(long, value_enum, help = "Pager synchronous mode override")]
    synchronous: Option<SynchronousArg>,

    #[arg(
        long,
        help = "Default distinct-neighbors behavior for storage accessors"
    )]
    distinct_neighbors_default: bool,
}

#[derive(Args, Debug)]
struct ImportCmd {
    #[arg(value_name = "DB")]
    db_path: PathBuf,

    #[arg(long, value_name = "FILE", help = "CSV file containing nodes")]
    nodes: Option<PathBuf>,

    #[arg(long, value_name = "FILE", help = "CSV file containing edges")]
    edges: Option<PathBuf>,

    #[arg(long, default_value = "id", help = "Node id column name")]
    node_id_column: String,

    #[arg(long, help = "Column containing pipe-separated labels")]
    node_label_column: Option<String>,

    #[arg(long, value_name = "LABEL|LABEL", help = "Constant labels to assign")]
    node_labels: Option<String>,

    #[arg(
        long,
        value_name = "col1,col2",
        help = "Explicit node property columns"
    )]
    node_props: Option<String>,

    #[arg(
        long,
        value_name = "col:type",
        help = "Comma-separated node property type mapping (e.g. birth:date)"
    )]
    node_prop_types: Option<String>,

    #[arg(long, default_value = "src", help = "Edge source column name")]
    edge_src_column: String,

    #[arg(long, default_value = "dst", help = "Edge destination column name")]
    edge_dst_column: String,

    #[arg(long, help = "Column containing edge types")]
    edge_type_column: Option<String>,

    #[arg(long, help = "Constant edge type if no column is provided")]
    edge_type: Option<String>,

    #[arg(
        long,
        value_name = "col1,col2",
        help = "Explicit edge property columns"
    )]
    edge_props: Option<String>,

    #[arg(
        long,
        value_name = "col:type",
        help = "Comma-separated edge property type mapping"
    )]
    edge_prop_types: Option<String>,

    #[arg(long, help = "Trust edge endpoints after each validated batch")]
    trusted_endpoints: bool,

    #[arg(
        long,
        value_name = "ENTRIES",
        default_value_t = 1024,
        help = "Endpoint existence cache size (0 disables caching)"
    )]
    edge_exists_cache: usize,

    #[arg(long, help = "Create the database if it does not exist")]
    create: bool,

    #[arg(long, help = "Drop existing property indexes before importing")]
    disable_indexes: bool,

    #[arg(
        long,
        help = "Rebuild property indexes after import (implies --disable-indexes)"
    )]
    build_indexes: bool,
}

#[derive(Args, Debug)]
struct ExportCmd {
    #[arg(value_name = "DB")]
    db_path: PathBuf,

    #[arg(long, value_name = "FILE", help = "Output CSV for nodes")]
    nodes: Option<PathBuf>,

    #[arg(long, value_name = "FILE", help = "Output CSV for edges")]
    edges: Option<PathBuf>,

    #[arg(
        long,
        value_name = "col1,col2",
        help = "Node property columns to include"
    )]
    node_props: Option<String>,

    #[arg(
        long,
        value_name = "col1,col2",
        help = "Edge property columns to include"
    )]
    edge_props: Option<String>,
}

#[derive(Args, Debug)]
struct SeedDemoCmd {
    #[arg(value_name = "DB")]
    db_path: PathBuf,

    #[arg(long, help = "Create the database if it does not exist")]
    create: bool,
}

#[derive(Args, Debug)]
struct DashboardCmd {
    #[arg(value_name = "DB")]
    db_path: PathBuf,

    #[arg(
        long,
        value_name = "HOST",
        default_value = "127.0.0.1",
        help = "Bind address host"
    )]
    host: IpAddr,

    #[arg(long, value_name = "PORT", default_value_t = 7654, help = "Bind port")]
    port: u16,

    #[arg(
        long,
        value_name = "DIR",
        help = "Directory containing dashboard assets"
    )]
    assets: Option<PathBuf>,

    #[arg(long, help = "Disable mutating/admin endpoints")]
    read_only: bool,

    #[arg(long, help = "Open the dashboard in the default browser")]
    open_browser: bool,

    #[arg(
        long = "allow-origin",
        value_name = "ORIGIN",
        action = ArgAction::Append,
        help = "Additional CORS origin to allow (repeatable)"
    )]
    allow_origins: Vec<String>,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(about = "Print pager/storage statistics")]
    Stats {
        #[arg(value_name = "DB")]
        db_path: PathBuf,
    },

    #[command(about = "Force a checkpoint on the database")]
    Checkpoint {
        #[arg(value_name = "DB")]
        db_path: PathBuf,

        #[arg(
            long,
            value_enum,
            default_value_t = CheckpointModeArg::Force,
            help = "Checkpoint mode"
        )]
        mode: CheckpointModeArg,
    },

    #[command(about = "Copy the database into a compacted file")]
    Vacuum {
        #[arg(value_name = "DB")]
        db_path: PathBuf,

        #[arg(long = "into", value_name = "PATH", required = true)]
        into: PathBuf,

        #[arg(long, help = "Also run ANALYZE after vacuum (deferred)")]
        analyze: bool,
    },

    #[command(about = "Verify on-disk structures")]
    Verify {
        #[arg(value_name = "DB")]
        db_path: PathBuf,

        #[arg(
            long,
            value_enum,
            default_value_t = VerifyLevelArg::Fast,
            help = "Verification level"
        )]
        level: VerifyLevelArg,
    },

    #[command(about = "Import nodes/edges from CSV files")]
    Import(ImportCmd),

    #[command(about = "Export nodes/edges to CSV files")]
    Export(ExportCmd),

    #[command(about = "Serve the experimental web dashboard")]
    Dashboard(DashboardCmd),

    #[command(about = "Populate demo nodes/edges (Ada, Grace, Alan)")]
    SeedDemo(SeedDemoCmd),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum SynchronousArg {
    Full,
    Normal,
    Off,
}

impl From<SynchronousArg> for Synchronous {
    fn from(mode: SynchronousArg) -> Self {
        match mode {
            SynchronousArg::Full => Synchronous::Full,
            SynchronousArg::Normal => Synchronous::Normal,
            SynchronousArg::Off => Synchronous::Off,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum CheckpointModeArg {
    Force,
    #[value(name = "best-effort")]
    BestEffort,
}

impl From<CheckpointModeArg> for CheckpointMode {
    fn from(mode: CheckpointModeArg) -> Self {
        match mode {
            CheckpointModeArg::Force => CheckpointMode::Force,
            CheckpointModeArg::BestEffort => CheckpointMode::BestEffort,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum VerifyLevelArg {
    Fast,
    Full,
}

impl From<VerifyLevelArg> for VerifyLevel {
    fn from(level: VerifyLevelArg) -> Self {
        match level {
            VerifyLevelArg::Fast => VerifyLevel::Fast,
            VerifyLevelArg::Full => VerifyLevel::Full,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let open_opts = build_open_options(&cli.open);

    match cli.command {
        Command::Stats { db_path } => {
            let report = stats(&db_path, &open_opts)?;
            emit(&cli.format, &report, |fmt| print_stats_text(fmt, &report))?;
        }
        Command::Checkpoint { db_path, mode } => {
            let report = checkpoint(&db_path, &open_opts, mode.into())?;
            emit(&cli.format, &report, |fmt| {
                print_checkpoint_text(fmt, &report)
            })?;
        }
        Command::Vacuum {
            db_path,
            into,
            analyze,
        } => {
            let vacuum_opts = VacuumOptions { analyze };
            let report = vacuum_into(&db_path, into, &open_opts, &vacuum_opts)?;
            emit(&cli.format, &report, |fmt| print_vacuum_text(fmt, &report))?;
        }
        Command::Verify { db_path, level } => {
            let report = verify(&db_path, &open_opts, level.into())?;
            emit(&cli.format, &report, |fmt| print_verify_text(fmt, &report))?;
            if !report.success {
                std::process::exit(2);
            }
        }
        Command::Import(cmd) => {
            let mut opts = open_opts.clone();
            opts.create_if_missing = cmd.create;
            let import_cfg = build_import_config(&cmd)?;
            let result = run_import(&import_cfg, &opts).map_err(into_boxed_error)?;
            println!(
                "Imported {} nodes and {} edges",
                result.nodes_imported, result.edges_imported
            );
        }
        Command::Export(cmd) => {
            let export_cfg = build_export_config(&cmd)?;
            let result = run_export(&export_cfg, &open_opts).map_err(into_boxed_error)?;
            println!(
                "Exported {} nodes and {} edges",
                result.nodes_exported, result.edges_exported
            );
        }
        Command::SeedDemo(cmd) => {
            run_seed_demo(&cmd, &open_opts)?;
        }
        Command::Dashboard(cmd) => {
            let dashboard_opts = build_dashboard_options(cmd, open_opts);
            if let Err(err) = dashboard::serve(dashboard_opts).await {
                eprintln!("dashboard server terminated: {err}");
                return Err(Box::new(err));
            }
        }
    }

    Ok(())
}

fn build_open_options(args: &OpenArgs) -> AdminOpenOptions {
    let mut opts = AdminOpenOptions::default();
    let mut pager_opts = PagerOptions::default();

    if let Some(page_size) = args.page_size {
        pager_opts.page_size = page_size;
    }
    if let Some(cache_pages) = args.cache_pages {
        pager_opts.cache_pages = cache_pages;
    }
    if let Some(mode) = args.synchronous {
        pager_opts.synchronous = mode.into();
    }

    opts.pager = pager_opts;
    opts.distinct_neighbors_default = args.distinct_neighbors_default;
    opts
}

fn build_dashboard_options(
    cmd: DashboardCmd,
    open_opts: AdminOpenOptions,
) -> DashboardServeOptions {
    DashboardServeOptions {
        db_path: cmd.db_path,
        open_opts,
        host: cmd.host,
        port: cmd.port,
        assets_dir: cmd.assets,
        read_only: cmd.read_only,
        open_browser: cmd.open_browser,
        allow_origins: cmd.allow_origins,
    }
}

fn run_seed_demo(cmd: &SeedDemoCmd, open_opts: &AdminOpenOptions) -> Result<(), Box<dyn Error>> {
    let mut db_opts = DatabaseOptions::default();
    db_opts.create_if_missing = cmd.create;
    db_opts.pager = open_opts.pager.clone();
    db_opts.distinct_neighbors_default = open_opts.distinct_neighbors_default;
    let db = ffi::Database::open(&cmd.db_path, db_opts)?;
    db.seed_demo()?;
    let check_spec = serde_json::json!({
        "$schemaVersion": 1,
        "matches": [
            { "var": "follower", "label": "User" },
            { "var": "followee", "label": "User" }
        ],
        "edges": [
            {
                "from": "follower",
                "to": "followee",
                "edge_type": "FOLLOWS",
                "direction": "out"
            }
        ],
        "projections": [
            { "kind": "var", "var": "follower" },
            { "kind": "var", "var": "followee" }
        ]
    });
    let response = db.execute_json(&check_spec)?;
    let rows = response
        .get("rows")
        .and_then(|value| value.as_array())
        .map(|rows| rows.len())
        .unwrap_or(0);
    println!(
        "Demo data inserted into {} ({} relationship rows)",
        cmd.db_path.display(),
        rows
    );
    drop(db);
    checkpoint(&cmd.db_path, open_opts, CheckpointMode::Force)?;
    println!("Checkpoint completed to persist seeded data.");
    Ok(())
}

fn build_import_config(cmd: &ImportCmd) -> Result<ImportConfig, CliError> {
    let nodes_path = cmd
        .nodes
        .clone()
        .ok_or_else(|| CliError::Message("--nodes is required".into()))?;
    if cmd.edge_type.is_some() && cmd.edge_type_column.is_some() {
        return Err(CliError::Message(
            "use either --edge-type or --edge-type-column, not both".into(),
        ));
    }

    let node_cfg = NodeImportConfig {
        path: nodes_path,
        id_column: cmd.node_id_column.clone(),
        label_column: cmd.node_label_column.clone(),
        static_labels: parse_labels_list(&cmd.node_labels),
        prop_columns: parse_prop_option(&cmd.node_props),
        prop_types: parse_prop_types(&cmd.node_prop_types)?,
    };

    let edge_cfg = if let Some(path) = &cmd.edges {
        Some(EdgeImportConfig {
            path: path.clone(),
            src_column: cmd.edge_src_column.clone(),
            dst_column: cmd.edge_dst_column.clone(),
            type_column: cmd.edge_type_column.clone(),
            static_type: cmd.edge_type.as_ref().map(|s| s.trim().to_string()),
            prop_columns: parse_prop_option(&cmd.edge_props),
            trusted_endpoints: cmd.trusted_endpoints,
            exists_cache_capacity: cmd.edge_exists_cache,
            prop_types: parse_prop_types(&cmd.edge_prop_types)?,
        })
    } else {
        None
    };

    Ok(ImportConfig {
        db_path: cmd.db_path.clone(),
        create_if_missing: cmd.create,
        disable_indexes: cmd.disable_indexes,
        build_indexes: cmd.build_indexes,
        nodes: Some(node_cfg),
        edges: edge_cfg,
    })
}

fn build_export_config(cmd: &ExportCmd) -> Result<ExportConfig, CliError> {
    if cmd.nodes.is_none() && cmd.edges.is_none() {
        return Err(CliError::Message(
            "export requires --nodes and/or --edges paths".into(),
        ));
    }
    Ok(ExportConfig {
        db_path: cmd.db_path.clone(),
        nodes_out: cmd.nodes.clone(),
        edges_out: cmd.edges.clone(),
        node_props: parse_props_list(&cmd.node_props),
        edge_props: parse_props_list(&cmd.edge_props),
    })
}

fn parse_prop_option(raw: &Option<String>) -> Option<Vec<String>> {
    raw.as_ref().map(|value| split_list(value, ','))
}

fn parse_prop_types(raw: &Option<String>) -> Result<HashMap<String, PropertyType>, CliError> {
    let mut map = HashMap::new();
    let Some(spec) = raw.as_ref() else {
        return Ok(map);
    };
    for entry in spec.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (name_raw, ty_raw) = trimmed.split_once(':').ok_or_else(|| {
            CliError::Message(format!(
                "invalid property type mapping '{}', expected name:type",
                trimmed
            ))
        })?;
        let name = name_raw.trim();
        if name.is_empty() {
            return Err(CliError::Message(
                "property type mapping requires a column name".into(),
            ));
        }
        let prop_type = parse_property_type_token(ty_raw)?;
        map.insert(name.to_ascii_lowercase(), prop_type);
    }
    Ok(map)
}

fn parse_property_type_token(token: &str) -> Result<PropertyType, CliError> {
    let lowered = token.trim().to_ascii_lowercase();
    let ty = match lowered.as_str() {
        "" => {
            return Err(CliError::Message(
                "property type mapping requires a type name".into(),
            ))
        }
        "auto" => PropertyType::Auto,
        "string" | "str" => PropertyType::String,
        "bool" | "boolean" => PropertyType::Bool,
        "int" | "integer" => PropertyType::Int,
        "float" | "double" => PropertyType::Float,
        "date" => PropertyType::Date,
        "datetime" | "timestamp" => PropertyType::DateTime,
        "bytes" => PropertyType::Bytes,
        other => {
            return Err(CliError::Message(format!(
                "unsupported property type '{}'",
                other
            )))
        }
    };
    Ok(ty)
}

fn parse_props_list(raw: &Option<String>) -> Vec<String> {
    raw.as_ref()
        .map(|value| split_list(value, ','))
        .unwrap_or_default()
}

fn parse_labels_list(raw: &Option<String>) -> Vec<String> {
    raw.as_ref()
        .map(|value| split_list(value, '|'))
        .unwrap_or_default()
}

fn split_list(input: &str, delim: char) -> Vec<String> {
    input
        .split(delim)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn into_boxed_error(err: CliError) -> Box<dyn Error> {
    Box::new(err)
}

fn emit<T, F>(format: &OutputFormat, value: &T, printer: F) -> Result<(), Box<dyn Error>>
where
    T: serde::Serialize,
    F: Fn(OutputFormat),
{
    match format {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(value)?;
            println!("{json}");
        }
        OutputFormat::Text => printer(OutputFormat::Text),
    }
    Ok(())
}

fn print_stats_text(_: OutputFormat, report: &sombra::admin::StatsReport) {
    println!("Pager:");
    println!(
        "  page_size={} cache_pages={} hits={} misses={} evictions={} dirty_writebacks={}",
        report.pager.page_size,
        report.pager.cache_pages,
        report.pager.hits,
        report.pager.misses,
        report.pager.evictions,
        report.pager.dirty_writebacks
    );
    println!("  last_checkpoint_lsn={}", report.pager.last_checkpoint_lsn);
    println!();
    println!(
        "WAL: exists={} size={} path={}",
        report.wal.exists, report.wal.size_bytes, report.wal.path
    );
    println!();
    println!(
        "Storage: next_node_id={} next_edge_id={} inline_blob={} inline_value={} flags=0x{:08x}",
        report.storage.next_node_id,
        report.storage.next_edge_id,
        report.storage.inline_prop_blob,
        report.storage.inline_prop_value,
        report.storage.storage_flags
    );
    println!(
        "          est_nodes={} est_edges={} distinct_neighbors_default={}",
        report.storage.estimated_node_count,
        report.storage.estimated_edge_count,
        report.storage.distinct_neighbors_default
    );
    println!();
    println!(
        "Filesystem: db_size={} wal_size={} db_path={} wal_path={}",
        report.filesystem.db_size_bytes,
        report.filesystem.wal_size_bytes,
        report.filesystem.db_path,
        report.filesystem.wal_path
    );
}

fn print_checkpoint_text(_: OutputFormat, report: &sombra::admin::CheckpointReport) {
    println!(
        "Checkpoint ({}) completed in {:.2} ms at LSN {}",
        report.mode, report.duration_ms, report.last_checkpoint_lsn
    );
}

fn print_vacuum_text(_: OutputFormat, report: &sombra::admin::VacuumReport) {
    println!(
        "Vacuum finished in {:.2} ms (copied {} bytes, checkpoint_lsn={}, analyze_performed={})",
        report.duration_ms, report.copied_bytes, report.checkpoint_lsn, report.analyze_performed
    );
    if let Some(summary) = &report.analyze_summary {
        println!("Analyze summary (labels):");
        for stat in &summary.label_counts {
            let name = stat
                .label_name
                .as_deref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("Label#{}", stat.label_id));
            println!("  {} (id={}): nodes={}", name, stat.label_id, stat.nodes);
        }
    }
}

fn print_verify_text(_: OutputFormat, report: &sombra::admin::VerifyReport) {
    println!(
        "Verify ({:?}) => success={} nodes_found={} edges_found={} adjacency_entries={} adjacency_nodes={}",
        report.level,
        report.success,
        report.counts.nodes_found,
        report.counts.edges_found,
        report.counts.adjacency_entries,
        report.counts.adjacency_nodes_touched,
    );
    for finding in &report.findings {
        println!("- {:?}: {}", finding.severity, finding.message);
    }
}
