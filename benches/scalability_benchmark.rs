use sombra::benchmark_suite::BenchmarkRunner;

fn main() {
    println!("===========================================");
    println!("  Sombra Scalability Benchmark Suite");
    println!("  Testing 100K+ Nodes with Optimizations");
    println!("===========================================\n");
    
    let mut runner = BenchmarkRunner::new();
    
    runner.run_scalability_benchmarks();
    
    println!("\n✓ Scalability benchmarks completed!");
    println!("  Results saved to scalability_benchmark_results.csv");
}
