use sombra::pager::DEFAULT_PAGE_SIZE;
use sombra::{Config, GraphDB, IntegrityOptions, Result};
use std::env;
use std::process;

fn print_usage() {
    eprintln!("┌─────────────────────────────────────────────┐");
    eprintln!("│         Sombra Database Inspector           │");
    eprintln!("└─────────────────────────────────────────────┘");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    sombra-inspect <database> <command>");
    eprintln!();
    eprintln!("COMMANDS:");
    eprintln!("    info         Show database metadata");
    eprintln!("    stats        Show detailed statistics");
    eprintln!("    verify       Run integrity check");
    eprintln!("    header       Show raw header contents");
    eprintln!("    wal-info     Show WAL status");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    sombra-inspect graph.db info");
    eprintln!("    sombra-inspect graph.db verify");
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
        format!("{} B", bytes)
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
    println!("  {:.<30} {}", name, value);
}

fn cmd_info(db_path: &str) -> Result<()> {
    print_header("DATABASE INFO");

    let config = Config::balanced();
    let db = GraphDB::open_with_config(db_path, config)?;

    let file_size = std::fs::metadata(db_path)?.len();

    print_section("General");
    print_field("Database Path", db_path);
    print_field("File Size", format_bytes(file_size));
    print_field("Page Size", format!("{} bytes", DEFAULT_PAGE_SIZE));

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

fn cmd_stats(db_path: &str) -> Result<()> {
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
        print_field("Cache Hit Rate", format!("{:.2}%", hit_rate));
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

fn cmd_verify(db_path: &str) -> Result<()> {
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
        println!("  Status: {}", "PASS".to_string());
    } else {
        println!("  ✗ Found {} issue(s)", total_errors);

        if !report.errors.is_empty() {
            print_section("Error Details");
            for (i, error) in report.errors.iter().enumerate() {
                println!("  {}. {}", i + 1, error);
            }
        }

        println!();
        println!("  Status: {}", "FAIL".to_string());
    }

    println!();

    Ok(())
}

fn cmd_header(db_path: &str) -> Result<()> {
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

fn cmd_wal_info(db_path: &str) -> Result<()> {
    print_header("WAL INFORMATION");

    let wal_path = format!("{}-wal", db_path);

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

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        print_usage();
        process::exit(1);
    }

    let db_path = &args[1];
    let command = &args[2];

    let result = match command.as_str() {
        "info" => cmd_info(db_path),
        "stats" => cmd_stats(db_path),
        "verify" => cmd_verify(db_path),
        "header" => cmd_header(db_path),
        "wal-info" => cmd_wal_info(db_path),
        _ => {
            eprintln!("Error: Unknown command '{}'", command);
            eprintln!();
            print_usage();
            process::exit(1);
        }
    };

    if let Err(e) = result {
        eprintln!();
        eprintln!("╔══════════════════════════════════════════════════════════╗");
        eprintln!("║                         ERROR                            ║");
        eprintln!("╚══════════════════════════════════════════════════════════╝");
        eprintln!();
        eprintln!("  {}", e);
        eprintln!();
        process::exit(1);
    }
}
