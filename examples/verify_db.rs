use sombra::{Config, GraphDB, IntegrityOptions};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::process;

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let mut checksum_only = false;
    let mut verify_indexes = true;
    let mut verify_adjacency = true;
    let mut max_errors: Option<usize> = None;
    let mut path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
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
            let parsed = value.parse::<usize>()?;
            max_errors = Some(parsed);
            continue;
        }
        if arg == "--help" || arg == "-h" {
            print_usage();
            return Ok(());
        }
        if path.is_none() {
            path = Some(PathBuf::from(arg));
        } else {
            eprintln!("unexpected extra argument: {arg}");
            print_usage();
            process::exit(1);
        }
    }

    let path = match path {
        Some(p) => p,
        None => {
            print_usage();
            process::exit(1);
        }
    };

    let mut db = GraphDB::open_with_config(&path, Config::production())?;

    let mut options = IntegrityOptions::default();
    options.checksum_only = checksum_only;
    options.verify_indexes = verify_indexes;
    options.verify_adjacency = verify_adjacency;
    if let Some(max) = max_errors {
        options.max_errors = max;
    }

    let report = db.verify_integrity(options)?;

    println!("Checked pages: {}", report.checked_pages);
    println!("Checksum failures: {}", report.checksum_failures);
    println!("Record errors: {}", report.record_errors);
    println!("Index errors: {}", report.index_errors);
    println!("Adjacency errors: {}", report.adjacency_errors);

    if !report.errors.is_empty() {
        println!("First {} reported issues:", report.errors.len());
        for (idx, message) in report.errors.iter().enumerate() {
            println!("  {}. {}", idx + 1, message);
        }
    }

    if report.is_clean() {
        println!("Integrity check passed.");
        Ok(())
    } else {
        Err("integrity violations detected".into())
    }
}

fn print_usage() {
    println!("Usage: verify_db [OPTIONS] <database_path>");
    println!();
    println!("Options:");
    println!("  --checksum-only       Verify only page checksums");
    println!("  --skip-indexes        Skip index consistency validation");
    println!("  --skip-adjacency      Skip adjacency validation");
    println!("  --max-errors=N        Limit the number of reported issues (default 16)");
    println!("  -h, --help            Show this help message");
}
