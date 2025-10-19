use sombra::benchmark_suite::BenchmarkRunner;

fn main() {
    let mut runner = BenchmarkRunner::new();
    
    // Run comprehensive benchmarks
    runner.run_all_benchmarks();
    
    // Run stress test for 30 seconds
    runner.run_stress_test(30);
    
    println!("\nBenchmark completed! Results saved to benchmark_results.csv");
}