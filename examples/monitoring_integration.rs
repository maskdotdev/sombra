//! Monitoring Integration Example
//!
//! This example demonstrates integrating Sombra with monitoring systems,
//! including metrics collection, health checks, and alerting.

use serde::{Deserialize, Serialize};
use sombra::{Edge, GraphDB, Node, PropertyValue, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricPoint {
    timestamp: i64,
    value: f64,
    tags: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct TimeSeries {
    name: String,
    points: Vec<MetricPoint>,
    max_points: usize,
}

impl TimeSeries {
    fn new(name: String, max_points: usize) -> Self {
        Self {
            name,
            points: Vec::new(),
            max_points,
        }
    }

    fn add_point(&mut self, value: f64, tags: HashMap<String, String>) {
        let point = MetricPoint {
            timestamp: chrono::Utc::now().timestamp(),
            value,
            tags,
        };

        self.points.push(point);

        // Keep only the most recent points
        if self.points.len() > self.max_points {
            self.points.remove(0);
        }
    }

    fn get_latest(&self) -> Option<&MetricPoint> {
        self.points.last()
    }

    fn average(&self) -> f64 {
        if self.points.is_empty() {
            return 0.0;
        }

        let sum: f64 = self.points.iter().map(|p| p.value).sum();
        sum / self.points.len() as f64
    }

    fn percentile(&self, p: f64) -> f64 {
        if self.points.is_empty() {
            return 0.0;
        }

        let mut values: Vec<f64> = self.points.iter().map(|p| p.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let index = ((values.len() as f64 - 1.0) * p / 100.0) as usize;
        values[index]
    }
}

#[derive(Debug)]
struct MetricsCollector {
    series: Arc<Mutex<HashMap<String, TimeSeries>>>,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            series: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn record_metric(&self, name: &str, value: f64, tags: HashMap<String, String>) {
        let mut series = self.series.lock().unwrap();
        let ts = series
            .entry(name.to_string())
            .or_insert_with(|| TimeSeries::new(name.to_string(), 1000));
        ts.add_point(value, tags);
    }

    fn get_metric(&self, name: &str) -> Option<TimeSeries> {
        let series = self.series.lock().unwrap();
        series.get(name).cloned()
    }

    fn get_all_metrics(&self) -> HashMap<String, TimeSeries> {
        let series = self.series.lock().unwrap();
        series.clone()
    }

    fn export_prometheus(&self) -> String {
        let series = self.series.lock().unwrap();
        let mut output = String::new();

        for (name, ts) in series.iter() {
            if let Some(latest) = ts.get_latest() {
                let mut metric_line = format!("{} {}", name, latest.value);

                // Add tags as labels
                if !latest.tags.is_empty() {
                    let tags: Vec<String> = latest
                        .tags
                        .iter()
                        .map(|(k, v)| format!("{}=\"{}\"", k, v))
                        .collect();
                    metric_line = format!("{}{{{}}} {}", name, tags.join(","), latest.value);
                }

                output.push_str(&metric_line);
                output.push('\n');
            }
        }

        output
    }

    fn export_json(&self) -> String {
        let series = self.series.lock().unwrap();
        let mut metrics = HashMap::new();

        for (name, ts) in series.iter() {
            let metric_data = serde_json::json!({
                "name": name,
                "latest": ts.get_latest(),
                "average": ts.average(),
                "p50": ts.percentile(50.0),
                "p95": ts.percentile(95.0),
                "p99": ts.percentile(99.0),
                "count": ts.points.len()
            });
            metrics.insert(name, metric_data);
        }

        serde_json::to_string_pretty(&metrics).unwrap()
    }
}

#[derive(Debug, Clone)]
struct HealthCheck {
    name: String,
    status: HealthStatus,
    message: String,
    last_check: i64,
    duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

struct HealthMonitor {
    checks: Arc<Mutex<Vec<HealthCheck>>>,
}

impl HealthMonitor {
    fn new() -> Self {
        Self {
            checks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn add_check(&self, check: HealthCheck) {
        let mut checks = self.checks.lock().unwrap();
        checks.push(check);
    }

    fn get_overall_status(&self) -> HealthStatus {
        let checks = self.checks.lock().unwrap();

        if checks.iter().any(|c| c.status == HealthStatus::Unhealthy) {
            HealthStatus::Unhealthy
        } else if checks.iter().any(|c| c.status == HealthStatus::Degraded) {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }

    fn get_checks(&self) -> Vec<HealthCheck> {
        let checks = self.checks.lock().unwrap();
        checks.clone()
    }

    fn export_json(&self) -> String {
        let checks = self.checks.lock().unwrap();
        let health_data = serde_json::json!({
            "status": self.get_overall_status(),
            "timestamp": chrono::Utc::now().timestamp(),
            "checks": checks
        });

        serde_json::to_string_pretty(&health_data).unwrap()
    }
}

struct DatabaseMonitor {
    db: GraphDB,
    metrics: MetricsCollector,
    health: HealthMonitor,
}

impl DatabaseMonitor {
    fn new(db_path: &str) -> Result<Self> {
        let db = GraphDB::open(db_path)?;
        Ok(Self {
            db,
            metrics: MetricsCollector::new(),
            health: HealthMonitor::new(),
        })
    }

    async fn start_monitoring(&self, interval_secs: u64) {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            // Collect database metrics
            if let Err(e) = self.collect_metrics() {
                eprintln!("Error collecting metrics: {}", e);
            }

            // Run health checks
            if let Err(e) = self.run_health_checks() {
                eprintln!("Error running health checks: {}", e);
            }
        }
    }

    fn collect_metrics(&self) -> Result<()> {
        let start_time = Instant::now();

        // Get database performance metrics
        let db_metrics = self.db.get_performance_metrics();

        // Record basic metrics
        self.metrics.record_metric(
            "sombra.cache_hit_rate",
            db_metrics.cache_hit_rate,
            HashMap::new(),
        );

        self.metrics.record_metric(
            "sombra.transactions_per_second",
            db_metrics.transactions_per_second,
            HashMap::new(),
        );

        self.metrics.record_metric(
            "sombra.reads_per_second",
            db_metrics.reads_per_second,
            HashMap::new(),
        );

        self.metrics.record_metric(
            "sombra.writes_per_second",
            db_metrics.writes_per_second,
            HashMap::new(),
        );

        self.metrics.record_metric(
            "sombra.dirty_pages",
            db_metrics.dirty_pages as f64,
            HashMap::new(),
        );

        self.metrics.record_metric(
            "sombra.wal_size_bytes",
            db_metrics.wal_size_bytes as f64,
            HashMap::new(),
        );

        // Record latency metrics
        self.metrics.record_metric(
            "sombra.commit_latency_p50",
            db_metrics.p50_commit_latency(),
            HashMap::new(),
        );

        self.metrics.record_metric(
            "sombra.commit_latency_p95",
            db_metrics.p95_commit_latency(),
            HashMap::new(),
        );

        self.metrics.record_metric(
            "sombra.commit_latency_p99",
            db_metrics.p99_commit_latency(),
            HashMap::new(),
        );

        // Record collection time
        let collection_time = start_time.elapsed().as_millis() as f64;
        self.metrics.record_metric(
            "sombra.metrics_collection_time_ms",
            collection_time,
            HashMap::new(),
        );

        // Count nodes and edges
        let tx = self.db.begin_transaction()?;
        let nodes = tx.find_nodes_by_label("User")?;
        let edges = tx.get_all_edges()?;

        self.metrics
            .record_metric("sombra.node_count", nodes.len() as f64, HashMap::new());

        self.metrics
            .record_metric("sombra.edge_count", edges.len() as f64, HashMap::new());

        Ok(())
    }

    fn run_health_checks(&self) -> Result<()> {
        let start_time = Instant::now();

        // Database connectivity check
        let connectivity_status = match self.db.health_check() {
            Ok(health) => {
                if health.status == sombra::HealthStatus::Healthy {
                    HealthStatus::Healthy
                } else if health.status == sombra::HealthStatus::Degraded {
                    HealthStatus::Degraded
                } else {
                    HealthStatus::Unhealthy
                }
            }
            Err(_) => HealthStatus::Unhealthy,
        };

        self.health.add_check(HealthCheck {
            name: "database_connectivity".to_string(),
            status: connectivity_status,
            message: "Database connectivity check".to_string(),
            last_check: chrono::Utc::now().timestamp(),
            duration_ms: start_time.elapsed().as_millis() as u64,
        });

        // Cache hit rate check
        let db_metrics = self.db.get_performance_metrics();
        let cache_status = if db_metrics.cache_hit_rate >= 0.9 {
            HealthStatus::Healthy
        } else if db_metrics.cache_hit_rate >= 0.7 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        self.health.add_check(HealthCheck {
            name: "cache_performance".to_string(),
            status: cache_status,
            message: format!("Cache hit rate: {:.2}%", db_metrics.cache_hit_rate * 100.0),
            last_check: chrono::Utc::now().timestamp(),
            duration_ms: start_time.elapsed().as_millis() as u64,
        });

        // WAL size check
        let wal_status = if db_metrics.wal_size_bytes < 50 * 1024 * 1024 {
            // 50MB
            HealthStatus::Healthy
        } else if db_metrics.wal_size_bytes < 100 * 1024 * 1024 {
            // 100MB
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        self.health.add_check(HealthCheck {
            name: "wal_size".to_string(),
            status: wal_status,
            message: format!("WAL size: {}MB", db_metrics.wal_size_bytes / 1024 / 1024),
            last_check: chrono::Utc::now().timestamp(),
            duration_ms: start_time.elapsed().as_millis() as u64,
        });

        // Dirty pages check
        let dirty_pages_status = if db_metrics.dirty_pages < 100 {
            HealthStatus::Healthy
        } else if db_metrics.dirty_pages < 500 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        self.health.add_check(HealthCheck {
            name: "dirty_pages".to_string(),
            status: dirty_pages_status,
            message: format!("Dirty pages: {}", db_metrics.dirty_pages),
            last_check: chrono::Utc::now().timestamp(),
            duration_ms: start_time.elapsed().as_millis() as u64,
        });

        Ok(())
    }

    fn simulate_load(&self, duration_secs: u64) -> Result<()> {
        println!(
            "🔄 Simulating database load for {} seconds...",
            duration_secs
        );

        let start_time = Instant::now();
        let mut operations = 0;

        while start_time.elapsed().as_secs() < duration_secs {
            let tx = self.db.begin_transaction()?;

            // Create a test node
            let node = tx.create_node(
                "TestNode",
                vec![
                    (
                        "timestamp".into(),
                        PropertyValue::Integer(chrono::Utc::now().timestamp()),
                    ),
                    (
                        "operation".into(),
                        PropertyValue::Integer(operations as i64),
                    ),
                ],
            )?;

            // Create some test edges
            if operations > 0 {
                tx.create_edge(
                    operations as u64 - 1,
                    node.id,
                    "TEST_EDGE",
                    vec![(
                        "created_at".into(),
                        PropertyValue::Integer(chrono::Utc::now().timestamp()),
                    )],
                )?;
            }

            tx.commit()?;
            operations += 1;

            // Small delay to simulate realistic load
            std::thread::sleep(Duration::from_millis(10));
        }

        println!("✅ Completed {} operations", operations);
        Ok(())
    }

    fn start_http_server(&self) -> Result<()> {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::thread;

        let metrics = self.metrics.clone();
        let health = self.health.clone();

        thread::spawn(move || {
            let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
            println!("🌐 Monitoring server started on http://127.0.0.1:8080");

            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        let metrics = metrics.clone();
                        let health = health.clone();

                        thread::spawn(move || {
                            let mut buffer = [0; 1024];
                            stream.read(&mut buffer).unwrap();

                            let request = String::from_utf8_lossy(&buffer[..]);
                            let response = if request.contains("/metrics") {
                                let prometheus = metrics.export_prometheus();
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n{}",
                                    prometheus
                                )
                            } else if request.contains("/health") {
                                let health_json = health.export_json();
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}",
                                    health_json
                                )
                            } else if request.contains("/metrics-json") {
                                let metrics_json = metrics.export_json();
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}",
                                    metrics_json
                                )
                            } else {
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n\
                                    <html><body>\
                                    <h1>Sombra Monitoring</h1>\
                                    <ul>\
                                    <li><a href=\"/metrics\">Prometheus Metrics</a></li>\
                                    <li><a href=\"/health\">Health Status</a></li>\
                                    <li><a href=\"/metrics-json\">JSON Metrics</a></li>\
                                    </ul>\
                                    </body></html>"
                                )
                            };

                            stream.write_all(response.as_bytes()).unwrap();
                            stream.flush().unwrap();
                        });
                    }
                    Err(e) => {
                        eprintln!("Connection failed: {}", e);
                    }
                }
            }
        });

        Ok(())
    }
}

// Implement Clone for the monitoring structs
impl Clone for MetricsCollector {
    fn clone(&self) -> Self {
        Self {
            series: Arc::clone(&self.series),
        }
    }
}

impl Clone for HealthMonitor {
    fn clone(&self) -> Self {
        Self {
            checks: Arc::clone(&self.checks),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("📊 Monitoring Integration Example with Sombra");

    // Initialize database monitor
    let monitor = DatabaseMonitor::new("monitoring_example.db")?;

    // Start HTTP server for metrics
    monitor.start_http_server()?;

    // Create some initial data
    println!("📝 Creating initial test data...");
    let tx = monitor.db.begin_transaction()?;

    for i in 0..10 {
        tx.create_node(
            "TestNode",
            vec![
                ("id".into(), PropertyValue::Integer(i)),
                ("name".into(), PropertyValue::String(format!("Node{}", i))),
                (
                    "created_at".into(),
                    PropertyValue::Integer(chrono::Utc::now().timestamp()),
                ),
            ],
        )?;
    }

    tx.commit()?;
    println!("✅ Created initial test data");

    // Start background monitoring
    let monitor_clone = monitor.clone();
    tokio::spawn(async move {
        monitor_clone.start_monitoring(5).await;
    });

    // Simulate some load
    monitor.simulate_load(30)?;

    // Wait a bit for metrics to be collected
    sleep(Duration::from_secs(10)).await;

    // Display current metrics
    println!("\n📈 Current Metrics:");
    let metrics = monitor.metrics.get_all_metrics();

    for (name, ts) in metrics {
        if let Some(latest) = ts.get_latest() {
            println!(
                "  {}: {:.2} (avg: {:.2}, p95: {:.2})",
                name,
                latest.value,
                ts.average(),
                ts.percentile(95.0)
            );
        }
    }

    // Display health status
    println!("\n🏥 Health Status:");
    let overall_status = monitor.health.get_overall_status();
    println!("Overall: {:?}", overall_status);

    let checks = monitor.health.get_checks();
    for check in checks {
        println!(
            "  {}: {:?} - {} ({}ms)",
            check.name, check.status, check.message, check.duration_ms
        );
    }

    // Export metrics in different formats
    println!("\n📤 Export Formats:");
    println!("Prometheus metrics available at: http://127.0.0.1:8080/metrics");
    println!("Health status available at: http://127.0.0.1:8080/health");
    println!("JSON metrics available at: http://127.0.0.1:8080/metrics-json");

    // Show sample Prometheus output
    println!("\n📊 Sample Prometheus Output:");
    println!("{}", monitor.metrics.export_prometheus());

    // Show sample health JSON
    println!("\n🏥 Sample Health JSON:");
    println!("{}", monitor.health.export_json());

    println!("\n🎯 Monitoring integration example completed!");
    println!("Database saved to: monitoring_example.db");
    println!("HTTP server running on http://127.0.0.1:8080");
    println!("Press Ctrl+C to stop");

    // Keep the server running
    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
