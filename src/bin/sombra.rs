use sombra::pager::DEFAULT_PAGE_SIZE;
use sombra::{Config, GraphDB, GraphError, IntegrityOptions, Result};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::process;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn print_main_usage() {
    eprintln!("┌─────────────────────────────────────────────┐");
    eprintln!("│            Sombra Database CLI              │");
    eprintln!("│               Version {VERSION:<23} │");
    eprintln!("└─────────────────────────────────────────────┘");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    sombra <command> [args]");
    eprintln!();
    eprintln!("COMMANDS:");
    eprintln!("    inspect      Inspect database information");
    eprintln!("    repair       Repair and optimize database");
    eprintln!("    verify       Verify database integrity");
    eprintln!("    version      Show version information");
    eprintln!("    help         Show this help message");
    eprintln!();
    eprintln!("Run 'sombra <command> --help' for more information on a command.");
    eprintln!();
}

fn print_inspect_usage() {
    eprintln!("┌─────────────────────────────────────────────┐");
    eprintln!("│         Sombra Database Inspector           │");
    eprintln!("└─────────────────────────────────────────────┘");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    sombra inspect <database> <command>");
    eprintln!();
    eprintln!("COMMANDS:");
    eprintln!("    info         Show database metadata");
    eprintln!("    stats        Show detailed statistics");
    eprintln!("    verify       Run integrity check");
    eprintln!("    header       Show raw header contents");
    eprintln!("    wal-info     Show WAL status");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    sombra inspect graph.db info");
    eprintln!("    sombra inspect graph.db verify");
    eprintln!();
}

fn print_repair_usage() {
    eprintln!("┌─────────────────────────────────────────────┐");
    eprintln!("│          Sombra Database Repair             │");
    eprintln!("└─────────────────────────────────────────────┘");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    sombra repair <database> <command>");
    eprintln!();
    eprintln!("COMMANDS:");
    eprintln!("    checkpoint       Force WAL checkpoint");
    eprintln!("    vacuum           Compact database");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    sombra repair graph.db checkpoint");
    eprintln!("    sombra repair graph.db vacuum");
    eprintln!();
    eprintln!("WARNING:");
    eprintln!("    Always backup your database before repair!");
    eprintln!();
}

fn print_verify_usage() {
    eprintln!("┌─────────────────────────────────────────────┐");
    eprintln!("│         Sombra Database Verification        │");
    eprintln!("└─────────────────────────────────────────────┘");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    sombra verify [OPTIONS] <database>");
    eprintln!();
    eprintln!("OPTIONS:");
    eprintln!("    --checksum-only       Verify only page checksums");
    eprintln!("    --skip-indexes        Skip index consistency validation");
    eprintln!("    --skip-adjacency      Skip adjacency validation");
    eprintln!("    --max-errors=N        Limit the number of reported issues (default 16)");
    eprintln!("    -h, --help            Show this help message");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    sombra verify graph.db");
    eprintln!("    sombra verify --checksum-only graph.db");
    eprintln!("    sombra verify --max-errors=100 graph.db");
    eprintln!();
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn print_header(title: &str) {
    let width = 60;
    let padding = (width - title.len() - 2) / 2;
    println!();
    println!("╔{}╗", "═".repeat(width));
    println!(
        "║{}{title}{}║",
        " ".repeat(padding),
        " ".repeat(width - padding - title.len())
    );
    println!("╚{}╝", "═".repeat(width));
    println!();
}

fn print_section(title: &str) {
    println!();
    println!("─── {} {}", title, "─".repeat(55 - title.len()));
}

fn print_field(name: &str, value: impl std::fmt::Display) {
    println!("  {name:.<30} {value}");
}

fn cmd_inspect_info(db_path: &str) -> Result<()> {
    print_header("DATABASE INFO");

    let config = Config::balanced();
    let db = GraphDB::open_with_config(db_path, config)?;

    let file_size = std::fs::metadata(db_path)?.len();

    print_section("General");
    print_field("Database Path", db_path);
    print_field("File Size", format_bytes(file_size));
    print_field("Page Size", format!("{DEFAULT_PAGE_SIZE} bytes"));

    let header_state = &db.header;

    print_section("Graph Statistics");
    print_field("Total Nodes", header_state.next_node_id);
    print_field("Total Edges", header_state.next_edge_id);

    print_section("Storage");
    if let Some(free_head) = header_state.free_page_head {
        print_field("Free Page List Head", free_head);
    } else {
        print_field("Free Page List Head", "None");
    }
    if let Some(last_record) = header_state.last_record_page {
        print_field("Last Record Page", last_record);
    } else {
        print_field("Last Record Page", "None");
    }

    print_section("Index");
    if let Some(btree_page) = header_state.btree_index_page {
        print_field("BTree Index Root Page", btree_page);
        print_field(
            "BTree Index Size",
            format!("{} entries", header_state.btree_index_size),
        );
    } else {
        print_field("BTree Index", "Not initialized");
    }

    print_section("Transactions");
    print_field("Last Committed TX ID", header_state.last_committed_tx_id);

    println!();
    println!("✓ Database opened successfully");
    println!();

    Ok(())
}

fn cmd_inspect_stats(db_path: &str) -> Result<()> {
    print_header("DATABASE STATISTICS");

    let config = Config::balanced();
    let db = GraphDB::open_with_config(db_path, config)?;

    let metrics = &db.metrics;

    print_section("Performance Metrics");
    print_field("Cache Hits", metrics.cache_hits);
    print_field("Cache Misses", metrics.cache_misses);

    let total_accesses = metrics.cache_hits + metrics.cache_misses;
    if total_accesses > 0 {
        let hit_rate = (metrics.cache_hits as f64 / total_accesses as f64) * 100.0;
        print_field("Cache Hit Rate", format!("{hit_rate:.2}%"));
    }

    print_field("Node Lookups", metrics.node_lookups);
    print_field("Edge Traversals", metrics.edge_traversals);

    print_section("Write-Ahead Log");
    print_field("WAL Bytes Written", format_bytes(metrics.wal_bytes_written));
    print_field("WAL Syncs", metrics.wal_syncs);
    print_field("Checkpoints", metrics.checkpoints_performed);
    print_field("Page Evictions", metrics.page_evictions);

    print_section("Transactions");
    print_field("Transactions Committed", metrics.transactions_committed);
    print_field("Transactions Rolled Back", metrics.transactions_rolled_back);

    println!();

    Ok(())
}

fn cmd_inspect_verify(db_path: &str) -> Result<()> {
    print_header("INTEGRITY VERIFICATION");

    let config = Config::balanced();
    let mut db = GraphDB::open_with_config(db_path, config)?;

    println!("  Running integrity checks...");
    println!();

    let options = IntegrityOptions {
        checksum_only: false,
        max_errors: 100,
        verify_indexes: true,
        verify_adjacency: true,
    };

    let report = db.verify_integrity(options)?;

    print_section("Verification Results");
    print_field("Pages Checked", report.checked_pages);
    print_field("Checksum Failures", report.checksum_failures);
    print_field("Record Errors", report.record_errors);
    print_field("Index Errors", report.index_errors);
    print_field("Adjacency Errors", report.adjacency_errors);

    let total_errors = report.checksum_failures
        + report.record_errors
        + report.index_errors
        + report.adjacency_errors;

    println!();

    if total_errors == 0 {
        println!("  ✓ No issues found - database is healthy!");
        println!();
        println!("  Status: PASS");
    } else {
        println!("  ✗ Found {total_errors} issue(s)");

        if !report.errors.is_empty() {
            print_section("Error Details");
            for (i, error) in report.errors.iter().enumerate() {
                println!("  {}. {}", i + 1, error);
            }
        }

        println!();
        println!("  Status: FAIL");
    }

    println!();

    Ok(())
}

fn cmd_inspect_header(db_path: &str) -> Result<()> {
    print_header("RAW HEADER CONTENTS");

    let config = Config::balanced();
    let db = GraphDB::open_with_config(db_path, config)?;

    let header_state = &db.header;

    print_section("Header Fields");
    print_field("next_node_id", header_state.next_node_id);
    print_field("next_edge_id", header_state.next_edge_id);
    print_field(
        "free_page_head",
        format!("{:?}", header_state.free_page_head),
    );
    print_field(
        "last_record_page",
        format!("{:?}", header_state.last_record_page),
    );
    print_field("last_committed_tx_id", header_state.last_committed_tx_id);
    print_field(
        "btree_index_page",
        format!("{:?}", header_state.btree_index_page),
    );
    print_field("btree_index_size", header_state.btree_index_size);

    println!();

    Ok(())
}

fn cmd_inspect_wal_info(db_path: &str) -> Result<()> {
    print_header("WAL INFORMATION");

    let wal_path = format!("{db_path}-wal");

    match std::fs::metadata(&wal_path) {
        Ok(metadata) => {
            let size = metadata.len();

            print_section("WAL Status");
            print_field("WAL File", &wal_path);
            print_field("WAL Size", format_bytes(size));
            print_field("Status", "Active");

            if size == 0 {
                println!();
                println!("  ℹ WAL file exists but is empty (clean state)");
            } else {
                let frame_size = 4096 + 24;
                let estimated_frames = size / frame_size;
                print_field("Estimated Frames", estimated_frames);

                println!();
                println!("  ⚠ WAL contains uncommitted changes");
                println!("    Run checkpoint to merge changes into main database");
            }
        }
        Err(_) => {
            print_section("WAL Status");
            print_field("WAL File", "Not found");
            print_field("Status", "No active WAL");

            println!();
            println!("  ✓ Database is in clean state (no WAL)");
        }
    }

    println!();

    Ok(())
}

fn cmd_repair_checkpoint(db_path: &str) -> Result<()> {
    print_header("CHECKPOINT WAL");

    let wal_path = format!("{db_path}-wal");

    let wal_size_before = match fs::metadata(&wal_path) {
        Ok(meta) => Some(meta.len()),
        Err(_) => None,
    };

    if let Some(size) = wal_size_before {
        println!("  WAL size before: {}", format_bytes(size));
    } else {
        println!("  No WAL file found");
    }

    println!();
    println!("  Performing checkpoint...");

    let config = Config::balanced();
    let mut db = GraphDB::open_with_config(db_path, config)?;
    db.checkpoint()?;

    drop(db);

    let wal_size_after = match fs::metadata(&wal_path) {
        Ok(meta) => Some(meta.len()),
        Err(_) => None,
    };

    print_section("Results");

    if let Some(size) = wal_size_after {
        println!("  WAL size after: {}", format_bytes(size));

        if let Some(before) = wal_size_before {
            let saved = before.saturating_sub(size);
            if saved > 0 {
                println!("  Space reclaimed: {}", format_bytes(saved));
            }
        }
    } else {
        println!("  WAL file removed (clean state)");
    }

    println!();
    println!("  ✓ Checkpoint completed successfully");
    println!();

    Ok(())
}

fn cmd_repair_vacuum(db_path: &str) -> Result<()> {
    print_header("VACUUM DATABASE");

    let size_before = fs::metadata(db_path)?.len();

    println!("  Database size before: {}", format_bytes(size_before));
    println!();
    println!("  Compacting database...");
    println!("  (This may take a while for large databases)");
    println!();

    let config = Config::balanced();
    let mut db = GraphDB::open_with_config(db_path, config)?;

    db.checkpoint()?;

    print_section("Results");

    let size_after = fs::metadata(db_path)?.len();
    println!("  Database size after: {}", format_bytes(size_after));

    if size_before > size_after {
        let saved = size_before - size_after;
        let percent = (saved as f64 / size_before as f64) * 100.0;
        println!(
            "  Space reclaimed: {} ({:.1}%)",
            format_bytes(saved),
            percent
        );
    } else {
        println!("  No space reclaimed (database already compact)");
    }

    println!();
    println!("  ✓ Vacuum completed successfully");
    println!();

    Ok(())
}

fn cmd_verify(args: Vec<String>) -> Result<()> {
    let mut checksum_only = false;
    let mut verify_indexes = true;
    let mut verify_adjacency = true;
    let mut max_errors: Option<usize> = None;
    let mut path: Option<String> = None;

    for arg in args {
        if arg == "--help" || arg == "-h" {
            print_verify_usage();
            process::exit(0);
        }
        if arg == "--checksum-only" {
            checksum_only = true;
            continue;
        }
        if arg == "--skip-indexes" {
            verify_indexes = false;
            continue;
        }
        if arg == "--skip-adjacency" {
            verify_adjacency = false;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--max-errors=") {
            max_errors = Some(
                value
                    .parse()
                    .map_err(|_| GraphError::Corruption("Invalid max-errors value".to_string()))?,
            );
            continue;
        }
        if path.is_none() {
            path = Some(arg);
        } else {
            eprintln!("Error: unexpected extra argument: {arg}");
            print_verify_usage();
            process::exit(1);
        }
    }

    let path = match path {
        Some(p) => p,
        None => {
            print_verify_usage();
            process::exit(1);
        }
    };

    print_header("INTEGRITY VERIFICATION");

    let config = Config::balanced();
    let mut db = GraphDB::open_with_config(&path, config)?;

    println!("  Running integrity checks...");
    println!();

    let options = IntegrityOptions {
        checksum_only,
        verify_indexes,
        verify_adjacency,
        max_errors: max_errors.unwrap_or(16),
    };

    let report = db.verify_integrity(options)?;

    print_section("Verification Results");
    print_field("Pages Checked", report.checked_pages);
    print_field("Checksum Failures", report.checksum_failures);
    print_field("Record Errors", report.record_errors);
    print_field("Index Errors", report.index_errors);
    print_field("Adjacency Errors", report.adjacency_errors);

    if !report.errors.is_empty() {
        print_section("Error Details");
        for (idx, message) in report.errors.iter().enumerate() {
            println!("  {}. {}", idx + 1, message);
        }
    }

    println!();

    if report.is_clean() {
        println!("  ✓ No issues found - database is healthy!");
        println!();
        println!("  Status: PASS");
        println!();
        Ok(())
    } else {
        println!("  ✗ Integrity violations detected");
        println!();
        println!("  Status: FAIL");
        println!();
        Err(GraphError::Corruption(
            "integrity violations detected".to_string(),
        ))
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.is_empty() {
        print_main_usage();
        process::exit(1);
    }

    let command = &args[0];

    let result = match command.as_str() {
        "inspect" => {
            if args.len() < 3 {
                print_inspect_usage();
                process::exit(1);
            }
            let db_path = &args[1];
            let subcommand = &args[2];
            match subcommand.as_str() {
                "info" => cmd_inspect_info(db_path),
                "stats" => cmd_inspect_stats(db_path),
                "verify" => cmd_inspect_verify(db_path),
                "header" => cmd_inspect_header(db_path),
                "wal-info" => cmd_inspect_wal_info(db_path),
                "--help" | "-h" => {
                    print_inspect_usage();
                    process::exit(0);
                }
                _ => {
                    eprintln!("Error: Unknown inspect command '{subcommand}'");
                    eprintln!();
                    print_inspect_usage();
                    process::exit(1);
                }
            }
        }
        "repair" => {
            if args.len() < 2 {
                print_repair_usage();
                process::exit(1);
            }
            if args[1] == "--help" || args[1] == "-h" {
                print_repair_usage();
                process::exit(0);
            }
            if args.len() < 3 {
                print_repair_usage();
                process::exit(1);
            }
            let db_path = &args[1];

            if !std::path::Path::new(db_path).exists() {
                eprintln!();
                eprintln!("╔══════════════════════════════════════════════════════════╗");
                eprintln!("║                         ERROR                            ║");
                eprintln!("╚══════════════════════════════════════════════════════════╝");
                eprintln!();
                eprintln!("  Database file not found: {db_path}");
                eprintln!();
                process::exit(1);
            }

            println!();
            println!("  ⚠  WARNING: Always backup your database before repair!");
            println!();
            print!("  Continue? [y/N] ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();

            if !input.trim().eq_ignore_ascii_case("y") {
                println!();
                println!("  Aborted.");
                println!();
                process::exit(0);
            }

            let subcommand = &args[2];
            match subcommand.as_str() {
                "checkpoint" => cmd_repair_checkpoint(db_path),
                "vacuum" => cmd_repair_vacuum(db_path),
                _ => {
                    eprintln!();
                    eprintln!("Error: Unknown repair command '{subcommand}'");
                    eprintln!();
                    print_repair_usage();
                    process::exit(1);
                }
            }
        }
        "verify" => cmd_verify(args[1..].to_vec()),
        "version" | "--version" | "-v" => {
            println!("sombra {VERSION}");
            process::exit(0);
        }
        "help" | "--help" | "-h" => {
            print_main_usage();
            process::exit(0);
        }
        _ => {
            eprintln!("Error: Unknown command '{command}'");
            eprintln!();
            print_main_usage();
            process::exit(1);
        }
    };

    if let Err(e) = result {
        eprintln!();
        eprintln!("╔══════════════════════════════════════════════════════════╗");
        eprintln!("║                         ERROR                            ║");
        eprintln!("╚══════════════════════════════════════════════════════════╝");
        eprintln!();
        eprintln!("  {e}");
        eprintln!();
        process::exit(1);
    }
}
