#![allow(clippy::uninlined_format_args)]

use sombra::{Config, GraphDB, Result};
use std::env;
use std::fs;
use std::process;

fn print_usage() {
    eprintln!("┌─────────────────────────────────────────────┐");
    eprintln!("│          Sombra Database Repair             │");
    eprintln!("└─────────────────────────────────────────────┘");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    sombra-repair <database> <command>");
    eprintln!();
    eprintln!("COMMANDS:");
    eprintln!("    checkpoint       Force WAL checkpoint");
    eprintln!("    vacuum           Compact database");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    sombra-repair graph.db checkpoint");
    eprintln!("    sombra-repair graph.db vacuum");
    eprintln!();
    eprintln!("WARNING:");
    eprintln!("    Always backup your database before repair!");
    eprintln!();
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

fn cmd_checkpoint(db_path: &str) -> Result<()> {
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

fn cmd_vacuum(db_path: &str) -> Result<()> {
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

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        print_usage();
        process::exit(1);
    }

    let db_path = &args[1];
    let command = &args[2];

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

    use std::io::{self, Write};
    io::stdout().flush().unwrap();

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    if !input.trim().eq_ignore_ascii_case("y") {
        println!();
        println!("  Aborted.");
        println!();
        process::exit(0);
    }

    let result = match command.as_str() {
        "checkpoint" => cmd_checkpoint(db_path),
        "vacuum" => cmd_vacuum(db_path),
        _ => {
            eprintln!();
            eprintln!("Error: Unknown command '{command}'");
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
        eprintln!("  {e}");
        eprintln!();
        process::exit(1);
    }
}
