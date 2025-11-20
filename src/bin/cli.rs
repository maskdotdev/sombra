//! Binary entry point for the Sombra administrative CLI.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, ArgGroup, Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell as CompletionShell;
use sombra::{
    admin::{
        checkpoint, mvcc_status, stats, vacuum_into, verify, AdminOpenOptions, CheckpointMode,
        MvccStatusReport, PagerOptions, VacuumOptions, VerifyLevel,
    },
    cli::import_export::{
        run_export, run_import, CliError, EdgeImportConfig, ExportConfig, ImportConfig,
        NodeImportConfig, PropertyType,
    },
    dashboard::{self, DashboardOptions as DashboardServeOptions},
    ffi::{self, DatabaseOptions},
    primitives::pager::Synchronous,
};

#[path = "cli/config.rs"]
mod config;
#[path = "cli/ui.rs"]
mod ui;

use config::{CliConfig, Profile, ProfileUpdate};
use ui::{Theme as UiTheme, Ui};

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
        value_name = "FILE",
        env = "SOMBRA_CONFIG",
        help = "Path to CLI config file (defaults to ~/.config/sombra/cli.toml)"
    )]
    config: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        value_name = "DB",
        env = "SOMBRA_DATABASE",
        help = "Default database path for commands that take a --db argument"
    )]
    database: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        value_name = "PROFILE",
        env = "SOMBRA_PROFILE",
        help = "Profile name to load pager/cache/database defaults from"
    )]
    profile: Option<String>,

    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = OutputFormat::Text,
        help = "Output format for structured responses"
    )]
    format: OutputFormat,

    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = ThemeArg::Auto,
        help = "Color theme for text output"
    )]
    theme: ThemeArg,

    #[arg(
        long,
        global = true,
        action = ArgAction::SetTrue,
        help = "Reduce decorative output and color usage"
    )]
    quiet: bool,

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

    #[arg(
        long = "pager-group-commit-max-writers",
        value_name = "WRITERS",
        help = "Maximum writers batched per WAL group commit"
    )]
    pager_group_commit_max_writers: Option<usize>,

    #[arg(
        long = "pager-group-commit-max-frames",
        value_name = "FRAMES",
        help = "Maximum WAL frames per group commit"
    )]
    pager_group_commit_max_frames: Option<usize>,

    #[arg(
        long = "pager-group-commit-max-wait-ms",
        value_name = "MS",
        help = "Time window in milliseconds to wait for group commits"
    )]
    pager_group_commit_max_wait_ms: Option<u64>,

    #[arg(
        long = "pager-async-fsync",
        value_enum,
        value_name = "on|off",
        help = "Enable async fsync handling (on/off)"
    )]
    pager_async_fsync: Option<ToggleArg>,

    #[arg(
        long = "pager-wal-segment-bytes",
        value_name = "BYTES",
        help = "Preferred WAL segment size (bytes)"
    )]
    pager_wal_segment_bytes: Option<u64>,

    #[arg(
        long = "pager-wal-preallocate-segments",
        value_name = "COUNT",
        help = "Number of WAL segments to preallocate"
    )]
    pager_wal_preallocate_segments: Option<u32>,
}

#[derive(Args, Debug)]
struct ImportCmd {
    #[arg(
        value_name = "DB",
        help = "Database path (defaults to --database or config)"
    )]
    db_path: Option<PathBuf>,

    #[arg(
        long,
        value_name = "FILE",
        help = "CSV file containing nodes",
        required = true
    )]
    nodes: PathBuf,

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

    #[arg(
        long,
        help = "Column containing edge types",
        conflicts_with = "edge_type"
    )]
    edge_type_column: Option<String>,

    #[arg(
        long,
        help = "Constant edge type if no column is provided",
        conflicts_with = "edge_type_column"
    )]
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
#[command(
    group(
        ArgGroup::new("targets")
            .required(true)
            .multiple(true)
            .args(["nodes", "edges"])
    )
)]
struct ExportCmd {
    #[arg(
        value_name = "DB",
        help = "Database path (defaults to --database or config)"
    )]
    db_path: Option<PathBuf>,

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
    #[arg(
        value_name = "DB",
        help = "Database path (defaults to --database or config)"
    )]
    db_path: Option<PathBuf>,

    #[arg(long, help = "Create the database if it does not exist")]
    create: bool,
}

#[derive(Args, Debug)]
struct InitCmd {
    #[arg(
        value_name = "DB",
        help = "Database path (defaults to --database or config)"
    )]
    db_path: Option<PathBuf>,

    #[arg(long, action = ArgAction::SetTrue, help = "Open the dashboard after init")]
    open_dashboard: bool,

    #[arg(long, action = ArgAction::SetTrue, help = "Skip seeding demo data")]
    skip_demo: bool,
}

#[derive(Args, Debug)]
struct DoctorCmd {
    #[arg(
        value_name = "DB",
        help = "Database path (defaults to --database or config)"
    )]
    db_path: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = VerifyLevelArg::Fast)]
    verify_level: VerifyLevelArg,

    #[arg(
        long,
        action = ArgAction::SetTrue,
        help = "Emit JSON report instead of formatted text"
    )]
    json: bool,
}

#[derive(Args, Debug)]
struct DashboardCmd {
    #[arg(
        value_name = "DB",
        help = "Database path (defaults to --database or config)"
    )]
    db_path: Option<PathBuf>,

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
enum ProfileCommand {
    #[command(about = "List configured profiles")]
    List,
    #[command(about = "Create or update a profile")]
    Save(ProfileSaveCmd),
    #[command(about = "Show details for a single profile")]
    Show(ProfileNameArg),
    #[command(about = "Delete a profile")]
    Delete(ProfileNameArg),
}

#[derive(Args, Debug)]
struct ProfileNameArg {
    #[arg(value_name = "NAME")]
    name: String,
}

#[derive(Args, Debug)]
struct ProfileSaveCmd {
    #[arg(value_name = "NAME")]
    name: String,

    #[arg(
        long,
        value_name = "DB",
        help = "Default database path for this profile"
    )]
    database: Option<PathBuf>,

    #[arg(
        long,
        value_name = "BYTES",
        help = "Pager page size override for this profile"
    )]
    page_size: Option<u32>,

    #[arg(
        long,
        value_name = "PAGES",
        help = "Pager cache size override for this profile"
    )]
    cache_pages: Option<usize>,

    #[arg(long, value_enum, help = "Pager synchronous mode for this profile")]
    synchronous: Option<SynchronousArg>,

    #[arg(
        long = "pager-group-commit-max-writers",
        value_name = "WRITERS",
        help = "Maximum writers batched per WAL group commit"
    )]
    pager_group_commit_max_writers: Option<usize>,

    #[arg(
        long = "pager-group-commit-max-frames",
        value_name = "FRAMES",
        help = "Maximum WAL frames per group commit"
    )]
    pager_group_commit_max_frames: Option<usize>,

    #[arg(
        long = "pager-group-commit-max-wait-ms",
        value_name = "MS",
        help = "Time window in milliseconds to wait for group commits"
    )]
    pager_group_commit_max_wait_ms: Option<u64>,

    #[arg(
        long = "pager-async-fsync",
        value_enum,
        value_name = "on|off",
        help = "Enable async fsync handling (on/off)"
    )]
    pager_async_fsync: Option<ToggleArg>,

    #[arg(
        long = "pager-wal-segment-bytes",
        value_name = "BYTES",
        help = "Preferred WAL segment size (bytes)"
    )]
    pager_wal_segment_bytes: Option<u64>,

    #[arg(
        long = "pager-wal-preallocate-segments",
        value_name = "COUNT",
        help = "Number of WAL segments to preallocate"
    )]
    pager_wal_preallocate_segments: Option<u32>,

    #[arg(
        long = "distinct-neighbors-default",
        action = ArgAction::SetTrue,
        conflicts_with = "no_distinct_neighbors_default",
        help = "Enable distinct-neighbors by default in this profile"
    )]
    distinct_neighbors_default: bool,

    #[arg(
        long = "no-distinct-neighbors-default",
        action = ArgAction::SetTrue,
        conflicts_with = "distinct_neighbors_default",
        help = "Disable distinct-neighbors by default in this profile"
    )]
    no_distinct_neighbors_default: bool,

    #[arg(long, action = ArgAction::SetTrue, help = "Mark this profile as the default")]
    set_default: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(about = "Print pager/storage statistics")]
    Stats {
        #[arg(
            value_name = "DB",
            help = "Database path (defaults to --database or config)"
        )]
        db_path: Option<PathBuf>,
    },

    #[command(about = "Show MVCC commit table status")]
    MvccStatus {
        #[arg(
            value_name = "DB",
            help = "Database path (defaults to --database or config)"
        )]
        db_path: Option<PathBuf>,
    },

    #[command(about = "Force a checkpoint on the database")]
    Checkpoint {
        #[arg(
            value_name = "DB",
            help = "Database path (defaults to --database or config)"
        )]
        db_path: Option<PathBuf>,

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
        #[arg(
            value_name = "DB",
            help = "Database path (defaults to --database or config)"
        )]
        db_path: Option<PathBuf>,

        #[arg(long = "into", value_name = "PATH", required = true)]
        into: PathBuf,

        #[arg(long, help = "Also run ANALYZE after vacuum (deferred)")]
        analyze: bool,
    },

    #[command(about = "Verify on-disk structures")]
    Verify {
        #[arg(
            value_name = "DB",
            help = "Database path (defaults to --database or config)"
        )]
        db_path: Option<PathBuf>,

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

    #[command(about = "Initialize a new database with demo data and optional dashboard assets")]
    Init(InitCmd),

    #[command(about = "Run diagnostics (verify + stats + filesystem) on a database")]
    Doctor(DoctorCmd),

    #[command(about = "Generate shell completions (bash, zsh, fish, etc.)")]
    Completions {
        #[arg(value_enum, help = "Target shell to generate completions for")]
        shell: CompletionShell,
    },

    #[command(about = "Manage CLI profiles", subcommand)]
    Profile(ProfileCommand),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum ThemeArg {
    Auto,
    Light,
    Dark,
    Plain,
}

impl From<ThemeArg> for UiTheme {
    fn from(theme: ThemeArg) -> Self {
        match theme {
            ThemeArg::Auto => UiTheme::Auto,
            ThemeArg::Light => UiTheme::Light,
            ThemeArg::Dark => UiTheme::Dark,
            ThemeArg::Plain => UiTheme::Plain,
        }
    }
}

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
enum ToggleArg {
    On,
    Off,
}

impl ToggleArg {
    fn as_bool(self) -> bool {
        matches!(self, ToggleArg::On)
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
    let mut config = CliConfig::load(cli.config.clone())?;
    let profile_name = cli
        .profile
        .clone()
        .or_else(|| config.default_profile_name().map(|name| name.to_string()));
    let profile = if let Some(ref name) = profile_name {
        Some(
            config
                .profile(name)
                .cloned()
                .ok_or_else(|| format!("profile '{name}' not found"))?,
        )
    } else {
        None
    };
    let default_db = cli
        .database
        .clone()
        .or_else(|| profile.as_ref().and_then(|p| p.database.clone()))
        .or_else(|| config.default_db_path().cloned());
    let ui = Ui::new(cli.theme.into(), cli.quiet);
    let open_opts = build_open_options(&cli.open, profile.as_ref());

    match cli.command {
        Command::Stats { db_path } => {
            let db_path = resolve_db_path(db_path, default_db.as_ref(), "stats")?;
            let report = stats(&db_path, &open_opts)?;
            emit(cli.format, &ui, &report, print_stats_text)?;
        }
        Command::MvccStatus { db_path } => {
            let db_path = resolve_db_path(db_path, default_db.as_ref(), "mvcc-status")?;
            let report = mvcc_status(&db_path, &open_opts)?;
            emit(cli.format, &ui, &report, print_mvcc_status_text)?;
        }
        Command::Checkpoint { db_path, mode } => {
            let db_path = resolve_db_path(db_path, default_db.as_ref(), "checkpoint")?;
            let report = checkpoint(&db_path, &open_opts, mode.into())?;
            emit(cli.format, &ui, &report, print_checkpoint_text)?;
        }
        Command::Vacuum {
            db_path,
            into,
            analyze,
        } => {
            let db_path = resolve_db_path(db_path, default_db.as_ref(), "vacuum")?;
            let task = ui.task("Vacuuming database");
            let vacuum_opts = VacuumOptions { analyze };
            let report = vacuum_into(&db_path, into, &open_opts, &vacuum_opts)?;
            let elapsed = task.finish();
            emit(cli.format, &ui, &report, print_vacuum_text)?;
            if matches!(cli.format, OutputFormat::Text) {
                ui.info(&format!(
                    "Vacuum completed in {}",
                    format_duration_pretty(elapsed)
                ));
            }
        }
        Command::Verify { db_path, level } => {
            let db_path = resolve_db_path(db_path, default_db.as_ref(), "verify")?;
            let task = ui.task("Verifying on-disk structures");
            let report = verify(&db_path, &open_opts, level.into())?;
            let elapsed = task.finish();
            emit(cli.format, &ui, &report, print_verify_text)?;
            if matches!(cli.format, OutputFormat::Text) {
                ui.info(&format!(
                    "Verify finished in {}",
                    format_duration_pretty(elapsed)
                ));
            }
            if !report.success {
                std::process::exit(2);
            }
        }
        Command::Import(cmd) => {
            let db_path = resolve_db_path(cmd.db_path.clone(), default_db.as_ref(), "import")?;
            let mut opts = open_opts.clone();
            opts.create_if_missing = cmd.create;
            let import_cfg = build_import_config(&cmd, db_path)?;
            let task = ui.task("Importing data");
            let result = run_import(&import_cfg, &opts).map_err(into_boxed_error)?;
            let elapsed = task.finish();
            ui.success(&format!(
                "Imported {} nodes and {} edges in {}",
                format_count(result.nodes_imported as u64),
                format_count(result.edges_imported as u64),
                format_duration_pretty(elapsed)
            ));
        }
        Command::Export(cmd) => {
            let db_path = resolve_db_path(cmd.db_path.clone(), default_db.as_ref(), "export")?;
            let export_cfg = build_export_config(&cmd, db_path)?;
            let task = ui.task("Exporting CSV data");
            let result = run_export(&export_cfg, &open_opts).map_err(into_boxed_error)?;
            let elapsed = task.finish();
            ui.success(&format!(
                "Exported {} nodes and {} edges in {}",
                format_count(result.nodes_exported as u64),
                format_count(result.edges_exported as u64),
                format_duration_pretty(elapsed)
            ));
        }
        Command::SeedDemo(cmd) => {
            let db_path = resolve_db_path(cmd.db_path.clone(), default_db.as_ref(), "seed-demo")?;
            run_seed_demo(&cmd, db_path, &open_opts, &ui)?;
        }
        Command::Init(cmd) => {
            let db_path = resolve_db_path(cmd.db_path.clone(), default_db.as_ref(), "init")?;
            run_init(&cmd, db_path, &open_opts, &ui)?;
        }
        Command::Doctor(cmd) => {
            let db_path = resolve_db_path(cmd.db_path.clone(), default_db.as_ref(), "doctor")?;
            run_doctor(&cmd, db_path, &open_opts, &ui)?;
        }
        Command::Dashboard(cmd) => {
            let db_path = resolve_db_path(cmd.db_path.clone(), default_db.as_ref(), "dashboard")?;
            let dashboard_opts = build_dashboard_options(cmd, db_path, open_opts);
            if let Err(err) = dashboard::serve(dashboard_opts).await {
                eprintln!("dashboard server terminated: {err}");
                return Err(Box::new(err));
            }
        }
        Command::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(shell, &mut cmd, "sombra", &mut io::stdout());
        }
        Command::Profile(ProfileCommand::List) => {
            let mut entries = config.profiles().cloned().collect::<Vec<_>>();
            if entries.is_empty() {
                ui.info("No profiles configured. Use `sombra profile save <name>` to create one.");
            } else {
                entries.sort_by(|a, b| a.name.cmp(&b.name));
                let default = config.default_profile_name();
                for profile in entries {
                    let title = if Some(profile.name.as_str()) == default {
                        format!("Profile '{}' (default)", profile.name)
                    } else {
                        format!("Profile '{}'", profile.name)
                    };
                    let rows = profile_rows(&profile);
                    ui.section(&title, rows);
                    ui.spacer();
                }
            }
        }
        Command::Profile(ProfileCommand::Save(cmd)) => {
            let distinct_pref = if cmd.distinct_neighbors_default {
                Some(true)
            } else if cmd.no_distinct_neighbors_default {
                Some(false)
            } else {
                None
            };
            let mut update = ProfileUpdate::default();
            update.database = cmd.database.clone();
            update.page_size = cmd.page_size;
            update.cache_pages = cmd.cache_pages;
            update.synchronous = cmd.synchronous;
            update.group_commit_max_writers = cmd.pager_group_commit_max_writers;
            update.group_commit_max_frames = cmd.pager_group_commit_max_frames;
            update.group_commit_max_wait_ms = cmd.pager_group_commit_max_wait_ms;
            update.async_fsync = cmd.pager_async_fsync.map(|toggle| toggle.as_bool());
            update.wal_segment_size_bytes = cmd.pager_wal_segment_bytes;
            update.wal_preallocate_segments = cmd.pager_wal_preallocate_segments;
            update.distinct_neighbors_default = distinct_pref;
            config.upsert_profile(&cmd.name, update)?;
            if cmd.set_default {
                config.set_default_profile(Some(&cmd.name))?;
            }
            let path = config.persist()?;
            ui.success(&format!(
                "Profile '{}' saved to {}",
                cmd.name,
                path.display()
            ));
        }
        Command::Profile(ProfileCommand::Show(cmd)) => {
            let profile = config
                .profile(&cmd.name)
                .cloned()
                .ok_or_else(|| format!("profile '{}' not found", cmd.name))?;
            let default_name = config.default_profile_name();
            let title = if Some(profile.name.as_str()) == default_name {
                format!("Profile '{}' (default)", profile.name)
            } else {
                format!("Profile '{}'", profile.name)
            };
            ui.section(&title, profile_rows(&profile));
        }
        Command::Profile(ProfileCommand::Delete(cmd)) => {
            config.delete_profile(&cmd.name)?;
            let path = config.persist()?;
            ui.success(&format!(
                "Profile '{}' deleted from {}",
                cmd.name,
                path.display()
            ));
        }
    }

    Ok(())
}

fn resolve_db_path(
    provided: Option<PathBuf>,
    fallback: Option<&PathBuf>,
    command: &str,
) -> Result<PathBuf, Box<dyn Error>> {
    if let Some(path) = provided {
        return Ok(path);
    }
    if let Some(global) = fallback {
        return Ok(global.clone());
    }
    Err(format!(
        "command '{command}' requires a database path. Provide <DB>, use --database, set SOMBRA_DATABASE, or configure cli.toml"
    )
    .into())
}

fn build_open_options(args: &OpenArgs, profile: Option<&Profile>) -> AdminOpenOptions {
    let mut opts = AdminOpenOptions::default();
    let mut pager_opts = PagerOptions::default();

    if let Some(profile) = profile {
        if let Some(page_size) = profile.page_size {
            pager_opts.page_size = page_size;
        }
        if let Some(cache_pages) = profile.cache_pages {
            pager_opts.cache_pages = cache_pages;
        }
        if let Some(mode) = profile.synchronous {
            pager_opts.synchronous = mode.into();
        }
        if let Some(max_writers) = profile.group_commit_max_writers {
            pager_opts.group_commit_max_writers = max_writers;
        }
        if let Some(max_frames) = profile.group_commit_max_frames {
            pager_opts.group_commit_max_frames = max_frames;
        }
        if let Some(wait_ms) = profile.group_commit_max_wait_ms {
            pager_opts.group_commit_max_wait_ms = wait_ms;
        }
        if let Some(async_fsync) = profile.async_fsync {
            pager_opts.async_fsync = async_fsync;
        }
        if let Some(segment_bytes) = profile.wal_segment_size_bytes {
            pager_opts.wal_segment_size_bytes = segment_bytes;
        }
        if let Some(preallocate) = profile.wal_preallocate_segments {
            pager_opts.wal_preallocate_segments = preallocate;
        }
        if let Some(distinct) = profile.distinct_neighbors_default {
            opts.distinct_neighbors_default = distinct;
        }
    }

    if let Some(page_size) = args.page_size {
        pager_opts.page_size = page_size;
    }
    if let Some(cache_pages) = args.cache_pages {
        pager_opts.cache_pages = cache_pages;
    }
    if let Some(mode) = args.synchronous {
        pager_opts.synchronous = mode.into();
    }
    if let Some(max_writers) = args.pager_group_commit_max_writers {
        pager_opts.group_commit_max_writers = max_writers;
    }
    if let Some(max_frames) = args.pager_group_commit_max_frames {
        pager_opts.group_commit_max_frames = max_frames;
    }
    if let Some(wait_ms) = args.pager_group_commit_max_wait_ms {
        pager_opts.group_commit_max_wait_ms = wait_ms;
    }
    if let Some(async_fsync) = args.pager_async_fsync {
        pager_opts.async_fsync = async_fsync.as_bool();
    }
    if let Some(segment_bytes) = args.pager_wal_segment_bytes {
        pager_opts.wal_segment_size_bytes = segment_bytes;
    }
    if let Some(preallocate) = args.pager_wal_preallocate_segments {
        pager_opts.wal_preallocate_segments = preallocate;
    }

    opts.pager = pager_opts;
    if args.distinct_neighbors_default {
        opts.distinct_neighbors_default = true;
    }
    opts
}

fn build_dashboard_options(
    cmd: DashboardCmd,
    db_path: PathBuf,
    open_opts: AdminOpenOptions,
) -> DashboardServeOptions {
    DashboardServeOptions {
        db_path,
        open_opts,
        host: cmd.host,
        port: cmd.port,
        assets_dir: cmd.assets,
        read_only: cmd.read_only,
        open_browser: cmd.open_browser,
        allow_origins: cmd.allow_origins,
    }
}

fn run_seed_demo(
    cmd: &SeedDemoCmd,
    db_path: PathBuf,
    open_opts: &AdminOpenOptions,
    ui: &Ui,
) -> Result<(), Box<dyn Error>> {
    let task = ui.task("Seeding demo data");
    let mut db_opts = DatabaseOptions::default();
    db_opts.create_if_missing = cmd.create;
    db_opts.pager = open_opts.pager.clone();
    db_opts.distinct_neighbors_default = open_opts.distinct_neighbors_default;
    let db = ffi::Database::open(&db_path, db_opts)?;
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
    drop(db);
    checkpoint(&db_path, open_opts, CheckpointMode::Force)?;
    let elapsed = task.finish();
    ui.success(&format!(
        "Demo data inserted into {} ({} relationship rows) in {}",
        db_path.display(),
        format_count(rows as u64),
        format_duration_pretty(elapsed)
    ));
    ui.info("Checkpoint completed to persist seeded data.");
    Ok(())
}

fn build_import_config(cmd: &ImportCmd, db_path: PathBuf) -> Result<ImportConfig, CliError> {
    let node_cfg = NodeImportConfig {
        path: cmd.nodes.clone(),
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
        db_path,
        create_if_missing: cmd.create,
        disable_indexes: cmd.disable_indexes,
        build_indexes: cmd.build_indexes,
        nodes: Some(node_cfg),
        edges: edge_cfg,
    })
}

fn build_export_config(cmd: &ExportCmd, db_path: PathBuf) -> Result<ExportConfig, CliError> {
    Ok(ExportConfig {
        db_path,
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

fn emit<T, F>(format: OutputFormat, ui: &Ui, value: &T, printer: F) -> Result<(), Box<dyn Error>>
where
    T: serde::Serialize,
    F: Fn(&Ui, &T),
{
    match format {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(value)?;
            println!("{json}");
        }
        OutputFormat::Text => printer(ui, value),
    }
    Ok(())
}

fn print_stats_text(ui: &Ui, report: &sombra::admin::StatsReport) {
    ui.section(
        "Pager",
        [
            ("page_size", format_bytes(u64::from(report.pager.page_size))),
            ("cache_pages", format_count(report.pager.cache_pages as u64)),
            ("hits", format_count(report.pager.hits)),
            ("misses", format_count(report.pager.misses)),
            ("evictions", format_count(report.pager.evictions)),
            (
                "dirty_writebacks",
                format_count(report.pager.dirty_writebacks),
            ),
            (
                "last_checkpoint_lsn",
                format_count(report.pager.last_checkpoint_lsn),
            ),
        ],
    );
    ui.spacer();
    ui.section(
        "WAL",
        [
            ("exists", format_bool(report.wal.exists)),
            ("size", format_bytes(report.wal.size_bytes)),
            ("path", report.wal.path.clone()),
            (
                "segment_size_bytes",
                format_bytes(report.wal.segment_size_bytes),
            ),
            (
                "preallocate_segments",
                format_count(report.wal.preallocate_segments.into()),
            ),
            (
                "ready_segments",
                format_count(report.wal.ready_segments as u64),
            ),
            (
                "recycle_segments",
                format_count(report.wal.recycle_segments as u64),
            ),
            (
                "allocation_error",
                report
                    .wal
                    .allocation_error
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "last_checkpoint_lsn",
                format_count(report.wal.last_checkpoint_lsn),
            ),
        ],
    );
    ui.spacer();
    ui.section(
        "Storage",
        [
            ("next_node_id", format_count(report.storage.next_node_id)),
            ("next_edge_id", format_count(report.storage.next_edge_id)),
            (
                "estimated_nodes",
                format_count(report.storage.estimated_node_count),
            ),
            (
                "estimated_edges",
                format_count(report.storage.estimated_edge_count),
            ),
            (
                "inline_prop_blob",
                format_bytes(u64::from(report.storage.inline_prop_blob)),
            ),
            (
                "inline_prop_value",
                format_bytes(u64::from(report.storage.inline_prop_value)),
            ),
            (
                "storage_flags",
                format!("0x{:08x}", report.storage.storage_flags),
            ),
            (
                "distinct_neighbors_default",
                format_bool(report.storage.distinct_neighbors_default),
            ),
        ],
    );
    ui.spacer();
    ui.section(
        "Filesystem",
        [
            ("db_path", report.filesystem.db_path.clone()),
            ("db_size", format_bytes(report.filesystem.db_size_bytes)),
            ("wal_path", report.filesystem.wal_path.clone()),
            ("wal_size", format_bytes(report.filesystem.wal_size_bytes)),
        ],
    );
}

fn print_mvcc_status_text(ui: &Ui, report: &MvccStatusReport) {
    ui.section(
        "Version Log",
        [
            ("entries", format_count(report.version_log_entries)),
            ("bytes", format_bytes(report.version_log_bytes)),
            (
                "retention_window",
                format_duration_ms(report.retention_window_ms as f64),
            ),
        ],
    );
    ui.spacer();

    if report.latest_committed_lsn.is_some()
        || report.durable_lsn.is_some()
        || report.acked_not_durable_commits.is_some()
    {
        let latest = report
            .latest_committed_lsn
            .map(format_count)
            .unwrap_or_else(|| "-".into());
        let durable = report
            .durable_lsn
            .map(format_count)
            .unwrap_or_else(|| "-".into());
        let acked = report
            .acked_not_durable_commits
            .map(format_count)
            .or_else(|| match (report.latest_committed_lsn, report.durable_lsn) {
                (Some(latest), Some(durable)) => Some(format_count(latest.saturating_sub(durable))),
                _ => None,
            })
            .unwrap_or_else(|| "-".into());
        ui.section(
            "Durability",
            [
                ("latest_committed_lsn", latest),
                ("durable_lsn", durable),
                ("acked_not_durable_commits", acked),
            ],
        );
        ui.spacer();
    }

    ui.section(
        "Vacuum",
        [
            ("mode", report.vacuum_mode.as_str()),
            (
                "horizon",
                report
                    .vacuum_horizon
                    .map(format_count)
                    .unwrap_or_else(|| "-".into())
                    .as_str(),
            ),
        ],
    );
    ui.spacer();

    if let Some(backlog) = &report.wal_backlog {
        ui.section(
            "WAL Backlog",
            [
                (
                    "pending_commits",
                    format_count(backlog.pending_commits as u64),
                ),
                (
                    "pending_frames",
                    format_count(backlog.pending_frames as u64),
                ),
                ("worker_running", format_bool(backlog.worker_running)),
            ],
        );
        ui.spacer();
    }

    if let Some(async_fsync) = &report.async_fsync {
        ui.section(
            "Async fsync",
            [
                ("pending_lsn", format_count(async_fsync.pending_lsn)),
                ("durable_lsn_cookie", format_count(async_fsync.durable_lsn)),
                ("pending_lag", format_count(async_fsync.pending_lag)),
            ],
        );
        if let Some(err) = &async_fsync.last_error {
            ui.info(&format!("last_error={err}"));
        }
        ui.spacer();
    }

    if let Some(pool) = &report.snapshot_pool {
        ui.section(
            "Snapshot Pool",
            [
                ("capacity", format_count(pool.capacity as u64)),
                ("available", format_count(pool.available as u64)),
            ],
        );
        ui.spacer();
    }

    if let Some(allocator) = &report.wal_allocator {
        ui.section(
            "WAL Allocator",
            [
                ("segment_size", format_bytes(allocator.segment_size_bytes)),
                (
                    "preallocate_segments",
                    format_count(allocator.preallocate_segments.into()),
                ),
                ("ready_segments", format_count(allocator.ready_segments)),
                ("recycle_segments", format_count(allocator.recycle_segments)),
            ],
        );
        if let Some(err) = &allocator.allocation_error {
            ui.info(&format!("allocation_error={err}"));
        }
        ui.spacer();
    }

    match &report.commit_table {
        Some(table) => {
            let reader = &table.reader_snapshot;
            ui.section(
                "Commit Table",
                [
                    ("released_up_to", format_count(table.released_up_to)),
                    ("oldest_visible", format_count(table.oldest_visible)),
                    (
                        "acked_not_durable",
                        format_count(table.acked_not_durable),
                    ),
                    ("entries", format_count(table.entries.len() as u64)),
                    ("active_readers", format_count(reader.active)),
                    (
                        "oldest_reader",
                        reader
                            .oldest_snapshot
                            .map(format_count)
                            .unwrap_or_else(|| "-".into()),
                    ),
                    (
                        "newest_reader",
                        reader
                            .newest_snapshot
                            .map(format_count)
                            .unwrap_or_else(|| "-".into()),
                    ),
                    (
                        "max_reader_age",
                        format_duration_ms(reader.max_age_ms as f64),
                    ),
                ],
            );
            ui.spacer();

            if table.entries.is_empty() {
                ui.info("No outstanding commit entries (all released).");
            } else {
                let max_rows = 12usize;
                let mut rows = Vec::new();
                for entry in table.entries.iter().take(max_rows) {
                    let status = match entry.status {
                        sombra::admin::CommitStatusKind::Pending => "pending",
                        sombra::admin::CommitStatusKind::Committed => "committed",
                        sombra::admin::CommitStatusKind::Durable => "durable",
                    };
                    let age = entry
                        .committed_ms_ago
                        .map(|ms| format_duration_ms(ms as f64))
                        .unwrap_or_else(|| "-".into());
                    rows.push(format!(
                        "commit={} status={} readers={} age={}",
                        format_count(entry.id),
                        status,
                        entry.reader_refs,
                        age,
                    ));
                }
                if table.entries.len() > max_rows {
                    rows.push(format!(
                        "... {} more pending entries",
                        table.entries.len() - max_rows
                    ));
                }
                ui.list("Outstanding commits", rows);
                ui.spacer();
            }

            if reader.slow_readers.is_empty() {
                ui.info("No slow readers registered.");
            } else {
                let rows: Vec<String> = reader
                    .slow_readers
                    .iter()
                    .map(|slow| {
                        format!(
                            "reader={} snapshot={} age={} thread={}",
                            slow.reader_id,
                            format_count(slow.snapshot_commit),
                            format_duration_ms(slow.age_ms as f64),
                            slow.thread
                        )
                    })
                    .collect();
                ui.list("Slow readers", rows);
            }
        }
        None => {
            ui.info("Commit table is unavailable (MVCC disabled for this database).");
        }
    }
}

fn print_checkpoint_text(ui: &Ui, report: &sombra::admin::CheckpointReport) {
    ui.section(
        "Checkpoint",
        [
            ("mode", format!("{:?}", report.mode)),
            ("duration", format_duration_ms(report.duration_ms)),
            (
                "last_checkpoint_lsn",
                format_count(report.last_checkpoint_lsn),
            ),
        ],
    );
}

fn print_vacuum_text(ui: &Ui, report: &sombra::admin::VacuumReport) {
    ui.section(
        "Vacuum",
        [
            ("duration", format_duration_ms(report.duration_ms)),
            ("copied", format_bytes(report.copied_bytes)),
            ("checkpoint_lsn", format_count(report.checkpoint_lsn)),
            ("analyze", format_bool(report.analyze_performed)),
        ],
    );
    if let Some(summary) = &report.analyze_summary {
        ui.spacer();
        let rows = summary.label_counts.iter().map(|stat| {
            let name = stat
                .label_name
                .as_deref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("Label#{}", stat.label_id));
            format!(
                "{} (id={}): {} nodes",
                name,
                stat.label_id,
                format_count(stat.nodes as u64)
            )
        });
        ui.list("Analyze summary", rows.collect::<Vec<_>>());
    }
}

fn print_verify_text(ui: &Ui, report: &sombra::admin::VerifyReport) {
    ui.section(
        "Verify",
        [
            ("level", format!("{:?}", report.level)),
            ("success", format_bool(report.success)),
            ("nodes_found", format_count(report.counts.nodes_found)),
            ("edges_found", format_count(report.counts.edges_found)),
            (
                "adjacency_entries",
                format_count(report.counts.adjacency_entries),
            ),
            (
                "adjacency_nodes",
                format_count(report.counts.adjacency_nodes_touched),
            ),
        ],
    );
    if report.findings.is_empty() && report.success {
        ui.success("All on-disk structures validated cleanly.");
    } else if !report.findings.is_empty() {
        ui.spacer();
        let messages = report
            .findings
            .iter()
            .map(|finding| format!("{:?}: {}", finding.severity, finding.message))
            .collect::<Vec<_>>();
        ui.list("Findings", messages);
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else {
        format!("{:.0}ms", ms)
    }
}

fn format_bool(flag: bool) -> String {
    if flag {
        "yes".to_string()
    } else {
        "no".to_string()
    }
}

fn format_count(value: u64) -> String {
    let s = value.to_string();
    let bytes = s.as_bytes();
    let mut formatted = String::with_capacity(bytes.len() + bytes.len() / 3);
    for (idx, ch) in bytes.iter().rev().enumerate() {
        if idx != 0 && idx % 3 == 0 {
            formatted.push('_');
        }
        formatted.push(*ch as char);
    }
    formatted.chars().rev().collect()
}

fn profile_rows(profile: &Profile) -> Vec<(&'static str, String)> {
    let mut rows = Vec::new();
    rows.push((
        "database",
        profile
            .database
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "page_size",
        profile
            .page_size
            .map(|v| format_bytes(v.into()))
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "cache_pages",
        profile
            .cache_pages
            .map(|v| format_count(v as u64))
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "synchronous",
        profile
            .synchronous
            .map(|mode| format!("{mode:?}"))
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "distinct_neighbors_default",
        profile
            .distinct_neighbors_default
            .map(|v| format_bool(v))
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "group_commit_max_writers",
        profile
            .group_commit_max_writers
            .map(|v| format_count(v as u64))
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "group_commit_max_frames",
        profile
            .group_commit_max_frames
            .map(|v| format_count(v as u64))
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "group_commit_max_wait_ms",
        profile
            .group_commit_max_wait_ms
            .map(format_count)
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "async_fsync",
        profile
            .async_fsync
            .map(|flag| format_bool(flag))
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "wal_segment_size_bytes",
        profile
            .wal_segment_size_bytes
            .map(format_bytes)
            .unwrap_or_else(|| "-".into()),
    ));
    rows.push((
        "wal_preallocate_segments",
        profile
            .wal_preallocate_segments
            .map(|v| format_count(v as u64))
            .unwrap_or_else(|| "-".into()),
    ));
    rows
}

fn format_duration_pretty(duration: Duration) -> String {
    if duration.as_secs() >= 1 {
        format!("{:.2}s", duration.as_secs_f64())
    } else {
        format!("{:.0}ms", duration.as_secs_f64() * 1_000.0)
    }
}
fn run_init(
    cmd: &InitCmd,
    db_path: PathBuf,
    open_opts: &AdminOpenOptions,
    ui: &Ui,
) -> Result<(), Box<dyn Error>> {
    if !cmd.skip_demo {
        let seed_cmd = SeedDemoCmd {
            db_path: Some(db_path.clone()),
            create: true,
        };
        run_seed_demo(&seed_cmd, db_path.clone(), open_opts, ui)?;
    } else if !db_path.exists() {
        ui.warn("Database file does not exist; use --skip-demo only after creating the DB.");
        return Ok(());
    } else {
        ui.info("Skipping demo seed; using existing database contents.");
    }

    if cmd.open_dashboard {
        let dash_cmd = DashboardCmd {
            db_path: Some(db_path.clone()),
            host: "127.0.0.1".parse().unwrap(),
            port: 7654,
            assets: None,
            read_only: false,
            open_browser: true,
            allow_origins: vec![],
        };
        let dash_opts = build_dashboard_options(dash_cmd, db_path.clone(), open_opts.clone());
        tokio::spawn(async move {
            if let Err(err) = dashboard::serve(dash_opts).await {
                eprintln!("dashboard server terminated: {err}");
            }
        });
        ui.info("Dashboard launching in background (CTRL+C to stop).");
    }

    ui.success(&format!(
        "Initialization complete. Database ready at {}",
        db_path.display()
    ));
    Ok(())
}

fn run_doctor(
    cmd: &DoctorCmd,
    db_path: PathBuf,
    open_opts: &AdminOpenOptions,
    ui: &Ui,
) -> Result<(), Box<dyn Error>> {
    let task = ui.task("Collecting stats and running verify");
    let stats_report = stats(&db_path, open_opts)?;
    let verify_report = verify(&db_path, open_opts, cmd.verify_level.into())?;
    let fs_meta = std::fs::metadata(&db_path)?;
    let wal_meta = stats_report.filesystem.wal_size_bytes;
    let elapsed = task.finish();

    if cmd.json {
        #[derive(serde::Serialize)]
        struct DoctorReport<'a> {
            stats: &'a sombra::admin::StatsReport,
            verify: &'a sombra::admin::VerifyReport,
            db_size_bytes: u64,
            wal_size_bytes: u64,
        }
        let report = DoctorReport {
            stats: &stats_report,
            verify: &verify_report,
            db_size_bytes: fs_meta.len(),
            wal_size_bytes: wal_meta,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        ui.section(
            "Database",
            [
                ("path", db_path.display().to_string()),
                ("size", format_bytes(fs_meta.len())),
                ("wal_size", format_bytes(wal_meta)),
                (
                    "page_size",
                    format_bytes(stats_report.pager.page_size.into()),
                ),
                (
                    "cache_pages",
                    format_count(stats_report.pager.cache_pages as u64),
                ),
            ],
        );
        ui.spacer();
        print_verify_text(ui, &verify_report);
        if !verify_report.success {
            ui.warn("Doctor detected issues during verify; address findings above.");
        } else {
            ui.success(&format!(
                "Doctor check passed in {}.",
                format_duration_pretty(elapsed)
            ));
        }
    }

    if !verify_report.success {
        std::process::exit(2);
    }

    Ok(())
}
