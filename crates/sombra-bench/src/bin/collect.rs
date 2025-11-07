#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use clap::Parser;
use csv::Writer;
use serde::Serialize;
use sombra_bench::env::EnvMetadata;
use walkdir::WalkDir;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Collects Criterion results into bench artifacts"
)]
struct Args {
    #[arg(long, default_value = "target/criterion")]
    criterion: PathBuf,

    #[arg(long)]
    out_dir: Option<PathBuf>,

    #[arg(long, default_value = "bench-results")]
    results_root: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let out_dir = args
        .out_dir
        .unwrap_or_else(|| args.results_root.join(timestamp));
    fs::create_dir_all(&out_dir)?;

    let env = EnvMetadata::collect(&out_dir);
    let env_path = out_dir.join("env.json");
    fs::write(&env_path, serde_json::to_vec_pretty(&env)?)?;

    let records = collect_criterion(&args.criterion)?;
    let summary_path = out_dir.join("results.json");
    fs::write(&summary_path, serde_json::to_vec_pretty(&records)?)?;
    write_csv(out_dir.join("results.csv"), &records)?;

    println!(
        "bench artifacts collected in {} ({} suites)",
        out_dir.display(),
        records.len()
    );
    Ok(())
}

fn collect_criterion(dir: &Path) -> Result<Vec<BenchRecord>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|res| res.ok())
        .filter(|e| e.file_name() == "benchmark.json")
    {
        let data = fs::read(entry.path())?;
        let bench: BenchmarkJson = serde_json::from_slice(&data)?;
        let estimate = bench.analysis.as_ref().map(|a| &a.estimates);
        let record = BenchRecord {
            suite: bench.group_id.clone(),
            function: bench.function_id.clone(),
            value_str: bench.value_str.clone(),
            mean_ns: estimate.map(|e| e.mean.point_estimate),
            median_ns: estimate.map(|e| e.median.point_estimate),
            std_dev_ns: estimate.map(|e| e.std_dev.point_estimate),
            throughput: bench.throughput.clone(),
        };
        out.push(record);
    }
    out.sort_by(|a, b| a.suite.cmp(&b.suite).then(a.function.cmp(&b.function)));
    Ok(out)
}

fn write_csv(path: PathBuf, rows: &[BenchRecord]) -> Result<()> {
    let mut writer = Writer::from_path(path)?;
    writer.write_record([
        "suite",
        "function",
        "value_str",
        "mean_ns",
        "median_ns",
        "std_dev_ns",
        "throughput_type",
        "throughput_value",
    ])?;
    for row in rows {
        let (th_type, th_value) = row
            .throughput
            .as_ref()
            .map(|tp| (tp.r#type.as_str(), tp.value.to_string()))
            .unwrap_or(("", "".to_string()));
        writer.write_record(&[
            row.suite.as_str(),
            row.function.as_str(),
            row.value_str.as_deref().unwrap_or(""),
            row.mean_ns
                .map(|v| format!("{v:.3}"))
                .as_deref()
                .unwrap_or(""),
            row.median_ns
                .map(|v| format!("{v:.3}"))
                .as_deref()
                .unwrap_or(""),
            row.std_dev_ns
                .map(|v| format!("{v:.3}"))
                .as_deref()
                .unwrap_or(""),
            th_type,
            th_value.as_str(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct BenchRecord {
    suite: String,
    function: String,
    value_str: Option<String>,
    mean_ns: Option<f64>,
    median_ns: Option<f64>,
    std_dev_ns: Option<f64>,
    throughput: Option<ThroughputRecord>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct ThroughputRecord {
    #[serde(rename = "type")]
    r#type: String,
    value: f64,
}

#[derive(serde::Deserialize)]
struct BenchmarkJson {
    #[serde(rename = "group_id")]
    group_id: String,
    #[serde(rename = "function_id")]
    function_id: String,
    #[serde(rename = "value_str")]
    value_str: Option<String>,
    throughput: Option<ThroughputRecord>,
    analysis: Option<Analysis>,
}

#[derive(serde::Deserialize)]
struct Analysis {
    #[serde(rename = "estimates")]
    estimates: Estimates,
}

#[derive(serde::Deserialize)]
struct Estimates {
    #[serde(rename = "Mean")]
    mean: Estimate,
    #[serde(rename = "Median")]
    median: Estimate,
    #[serde(rename = "StdDev")]
    std_dev: Estimate,
}

#[derive(serde::Deserialize)]
struct Estimate {
    #[serde(rename = "point_estimate")]
    point_estimate: f64,
}
