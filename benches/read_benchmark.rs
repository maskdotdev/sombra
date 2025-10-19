use sombra::benchmark_suite::BenchmarkRunner;

fn main() {
    let mut runner = BenchmarkRunner::new();
    
    runner.run_read_benchmarks();
    
    println!("\n✓ Read benchmark completed! Results saved to read_benchmark_results.csv");
}
