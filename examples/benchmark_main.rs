use sombra::benchmark_suite::BenchmarkRunner;
use clap::{Arg, Command};

fn main() {
    let matches = Command::new("Sombra Benchmark Suite")
        .version("1.0")
        .about("Benchmark Sombra graph database performance")
        .arg(
            Arg::new("operation")
                .help("Specific operation to benchmark")
                .long_help("Run only specific benchmark operations:
  all          - Run all benchmarks (default)
  inserts      - Node and edge insert performance
  queries      - Node and edge query performance  
  bulk         - Bulk insert operations
  stress       - Stress testing
  small        - Small dataset only (100 nodes)
  medium       - Medium dataset only (1000 nodes)
  large        - Large dataset only (10000 nodes)")
                .index(1)
                .value_parser([
                    "all", "inserts", "queries", "bulk", "stress", 
                    "small", "medium", "large"
                ])
        )
        .arg(
            Arg::new("duration")
                .help("Stress test duration in seconds")
                .long("duration")
                .short('d')
                .value_parser(clap::value_parser!(u64))
                .default_value("30")
        )
        .arg(
            Arg::new("output")
                .help("Output CSV file")
                .long("output")
                .short('o')
                .default_value("benchmark_results.csv")
        )
        .get_matches();

    let mut runner = BenchmarkRunner::new();
    let default_op = "all".to_string();
    let operation = matches.get_one::<String>("operation").unwrap_or(&default_op);
    let duration = *matches.get_one::<u64>("duration").unwrap();
    let output_file = matches.get_one::<String>("output").unwrap();

    println!("Running Sombra Benchmark Suite");
    println!("Operation: {}", operation);
    println!("Output file: {}", output_file);
    println!("");

    match operation.as_str() {
        "all" => {
            runner.run_all_benchmarks();
            runner.run_stress_test(duration);
        }
        "inserts" => {
            runner.run_insert_benchmarks();
        }
        "queries" => {
            runner.run_query_benchmarks();
        }
        "bulk" => {
            runner.run_bulk_benchmarks();
        }
        "stress" => {
            runner.run_stress_test(duration);
        }
        "small" => {
            runner.run_small_dataset_benchmarks();
        }
        "medium" => {
            runner.run_medium_dataset_benchmarks();
        }
        "large" => {
            runner.run_large_dataset_benchmarks();
        }
        _ => {
            eprintln!("Unknown operation: {}", operation);
            std::process::exit(1);
        }
    }

    // Print results and export
    runner.print_results();
    if let Err(e) = runner.export_results(output_file) {
        eprintln!("Failed to export CSV: {}", e);
    }
    
    println!("\nBenchmark completed! Results saved to {}", output_file);
}