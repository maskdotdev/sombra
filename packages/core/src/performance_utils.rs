#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::new_without_default)]
#![allow(clippy::needless_borrows_for_generic_args)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::never_loop)]
#![allow(clippy::collapsible_if)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub operation: String,
    pub duration: Duration,
    pub memory_usage_mb: f64,
    pub operations_per_second: f64,
    pub metadata: HashMap<String, String>,
    pub latencies: Option<LatencyStats>,
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
}

impl BenchmarkResult {
    pub fn new(operation: String, duration: Duration, count: u64) -> Self {
        let ops_per_sec = if duration.as_secs_f64() > 0.0 {
            count as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        Self {
            operation,
            duration,
            memory_usage_mb: 0.0,
            operations_per_second: ops_per_sec,
            metadata: HashMap::new(),
            latencies: None,
        }
    }

    pub fn with_memory(mut self, memory_mb: f64) -> Self {
        self.memory_usage_mb = memory_mb;
        self
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn with_latencies(mut self, latencies: LatencyStats) -> Self {
        self.latencies = Some(latencies);
        self
    }
}

impl LatencyStats {
    pub fn from_durations(mut durations: Vec<Duration>) -> Self {
        durations.sort();
        let len = durations.len();

        let p50 = durations[len * 50 / 100];
        let p95 = durations[len * 95 / 100];
        let p99 = durations[len * 99 / 100];
        let min = durations[0];
        let max = durations[len - 1];

        let total: Duration = durations.iter().sum();
        let mean = total / len as u32;

        Self {
            p50,
            p95,
            p99,
            min,
            max,
            mean,
        }
    }
}

pub struct BenchmarkTimer {
    start_time: Instant,
    operation: String,
}

impl BenchmarkTimer {
    pub fn new(operation: String) -> Self {
        Self {
            start_time: Instant::now(),
            operation,
        }
    }

    pub fn finish(self, count: u64) -> BenchmarkResult {
        let duration = self.start_time.elapsed();
        BenchmarkResult::new(self.operation, duration, count)
    }
}

pub struct MemoryTracker {
    initial_memory: usize,
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self {
            initial_memory: Self::get_memory_usage(),
        }
    }

    pub fn current_usage_mb(&self) -> f64 {
        let current = Self::get_memory_usage();
        (current.saturating_sub(self.initial_memory)) as f64 / 1024.0 / 1024.0
    }

    #[cfg(target_os = "linux")]
    fn get_memory_usage() -> usize {
        use std::fs;
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            return kb * 1024; // Convert KB to bytes
                        }
                    }
                }
            }
        }
        0
    }

    #[cfg(target_os = "macos")]
    fn get_memory_usage() -> usize {
        use std::process::Command;
        if let Ok(output) = Command::new("ps")
            .args(&["-o", "rss=", "-p"])
            .arg(std::process::id().to_string())
            .output()
        {
            if let Ok(kb_str) = String::from_utf8(output.stdout) {
                if let Ok(kb) = kb_str.trim().parse::<usize>() {
                    return kb * 1024; // Convert KB to bytes
                }
            }
        }
        0
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn get_memory_usage() -> usize {
        0 // Fallback for unsupported platforms
    }
}

pub struct BenchmarkSuite {
    results: Vec<BenchmarkResult>,
    memory_tracker: MemoryTracker,
}

impl BenchmarkSuite {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            memory_tracker: MemoryTracker::new(),
        }
    }

    pub fn run_benchmark<F, R>(
        &mut self,
        operation: String,
        count: u64,
        f: F,
    ) -> Option<&BenchmarkResult>
    where
        F: FnOnce() -> R,
    {
        let timer = BenchmarkTimer::new(operation.clone());
        let _result = f();
        let mut benchmark_result = timer.finish(count);
        benchmark_result.memory_usage_mb = self.memory_tracker.current_usage_mb();

        self.results.push(benchmark_result);
        self.results.last()
    }

    pub fn run_latency_benchmark<F, R>(
        &mut self,
        operation: String,
        count: u64,
        mut f: F,
    ) -> Option<&BenchmarkResult>
    where
        F: FnMut() -> R,
    {
        let start_time = Instant::now();
        let mut latencies = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let op_start = Instant::now();
            let _result = f();
            latencies.push(op_start.elapsed());
        }

        let total_duration = start_time.elapsed();
        let latency_stats = LatencyStats::from_durations(latencies);

        let benchmark_result = BenchmarkResult::new(operation, total_duration, count)
            .with_latencies(latency_stats)
            .with_memory(self.memory_tracker.current_usage_mb());

        self.results.push(benchmark_result);
        self.results.last()
    }

    pub fn run_timed_benchmark<F, R>(
        &mut self,
        operation: String,
        duration_secs: u64,
        f: F,
    ) -> Option<&BenchmarkResult>
    where
        F: Fn() -> R,
    {
        let start_time = Instant::now();
        let mut count = 0u64;

        while start_time.elapsed().as_secs() < duration_secs {
            let _result = f();
            count += 1;
        }

        let elapsed = start_time.elapsed();
        let mut benchmark_result = BenchmarkResult::new(operation, elapsed, count);
        benchmark_result.memory_usage_mb = self.memory_tracker.current_usage_mb();

        self.results.push(benchmark_result);
        self.results.last()
    }

    pub fn results(&self) -> &[BenchmarkResult] {
        &self.results
    }

    pub fn print_summary(&self) {
        println!(
            "
=== Benchmark Summary ==="
        );
        println!(
            "{:<30} {:<15} {:<15} {:<12} {:<12} {:<12}",
            "Operation", "Duration (ms)", "Ops/sec", "P50 (µs)", "P95 (µs)", "P99 (µs)"
        );
        println!("{}", "-".repeat(105));

        for result in &self.results {
            if let Some(ref latencies) = result.latencies {
                println!(
                    "{:<30} {:<15.2} {:<15.0} {:<12.2} {:<12.2} {:<12.2}",
                    result.operation,
                    result.duration.as_millis(),
                    result.operations_per_second,
                    latencies.p50.as_micros(),
                    latencies.p95.as_micros(),
                    latencies.p99.as_micros()
                );
            } else {
                println!(
                    "{:<30} {:<15.2} {:<15.0} {:<12} {:<12} {:<12}",
                    result.operation,
                    result.duration.as_millis(),
                    result.operations_per_second,
                    "-",
                    "-",
                    "-"
                );
            }
        }
    }

    pub fn print_detailed(&self) {
        println!(
            "
=== Detailed Benchmark Results ==="
        );
        for result in &self.results {
            println!(
                "
Operation: {}",
                result.operation
            );
            println!("  Duration: {:.2} ms", result.duration.as_millis());
            println!("  Operations/sec: {:.0}", result.operations_per_second);
            println!("  Memory usage: {:.2} MB", result.memory_usage_mb);

            if let Some(ref latencies) = result.latencies {
                println!("  Latency Statistics:");
                println!("    Min:  {:.2} µs", latencies.min.as_micros());
                println!("    P50:  {:.2} µs", latencies.p50.as_micros());
                println!("    P95:  {:.2} µs", latencies.p95.as_micros());
                println!("    P99:  {:.2} µs", latencies.p99.as_micros());
                println!("    Max:  {:.2} µs", latencies.max.as_micros());
                println!("    Mean: {:.2} µs", latencies.mean.as_micros());
            }

            if !result.metadata.is_empty() {
                println!("  Metadata:");
                for (key, value) in &result.metadata {
                    println!("    {}: {}", key, value);
                }
            }
        }
    }

    pub fn export_csv(&self, path: &str) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;
        writeln!(file, "operation,duration_ms,ops_per_sec,memory_mb,count,p50_us,p95_us,p99_us,min_us,max_us,mean_us")?;

        for result in &self.results {
            let count = (result.operations_per_second * result.duration.as_secs_f64()) as u64;

            if let Some(ref latencies) = result.latencies {
                writeln!(
                    file,
                    "{},{:.2},{:.0},{:.2},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                    result.operation,
                    result.duration.as_millis(),
                    result.operations_per_second,
                    result.memory_usage_mb,
                    count,
                    latencies.p50.as_micros(),
                    latencies.p95.as_micros(),
                    latencies.p99.as_micros(),
                    latencies.min.as_micros(),
                    latencies.max.as_micros(),
                    latencies.mean.as_micros()
                )?;
            } else {
                writeln!(
                    file,
                    "{},{:.2},{:.0},{:.2},{},,,,,,",
                    result.operation,
                    result.duration.as_millis(),
                    result.operations_per_second,
                    result.memory_usage_mb,
                    count
                )?;
            }
        }

        Ok(())
    }

    pub fn clear(&mut self) {
        self.results.clear();
    }
}

impl Default for BenchmarkSuite {
    fn default() -> Self {
        Self::new()
    }
}
