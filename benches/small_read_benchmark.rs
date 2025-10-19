use sombra::benchmark_suite::BenchmarkRunner;

fn main() {
    let mut runner = BenchmarkRunner::new();
    
    println!("Running small, medium, and large read benchmarks...\n");
    
    runner.run_small_dataset_reads();
    runner.run_medium_dataset_reads();
    runner.run_large_dataset_reads();
    
    runner.suite.print_summary();
    runner.suite.print_detailed();
    
    if let Err(e) = runner.suite.export_csv("read_benchmark_results.csv") {
        eprintln!("Failed to export CSV: {}", e);
    }
    
    println!("\n✓ Read benchmark completed! Results saved to read_benchmark_results.csv");
}
