//! POST crawler `jobs.json` to offerTrackPlatform `POST /api/v1/admin/ingest/crawler-jobs`.
//! Replaces the former `scripts/push_to_offertrack.py`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(name = "offertrack-push")]
#[command(about = "POST crawler jobs.json to offerTrackPlatform admin ingest API.")]
struct Args {
    /// Path to crawler export (array of job objects)
    #[arg(value_name = "JOBS_FILE", default_value = "out/jobs.json")]
    jobs_file: PathBuf,
    /// Platform API origin (or set OFFERTRACK_API_URL)
    #[arg(long, env = "OFFERTRACK_API_URL", default_value = "http://localhost:3000")]
    base_url: String,
    /// X-Admin-Key when the platform sets ADMIN_API_KEY
    #[arg(long, env = "OFFERTRACK_ADMIN_KEY", default_value = "")]
    admin_key: String,
    /// Print payload stats only; do not POST
    #[arg(long)]
    dry_run: bool,
}

fn count_rows_missing_title_or_job_id(jobs: &[Value]) -> usize {
    jobs.iter()
        .filter(|r| {
            let obj = r.as_object();
            let title_ok = obj
                .and_then(|o| o.get("title"))
                .and_then(|v| v.as_str())
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            let id_ok = obj
                .and_then(|o| o.get("job_id"))
                .and_then(|v| v.as_str())
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            !(title_ok && id_ok)
        })
        .count()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let base = args.base_url.trim_end_matches('/').to_string();

    let raw = std::fs::read_to_string(&args.jobs_file)
        .with_context(|| format!("read {}", args.jobs_file.display()))?;
    let jobs: Vec<Value> = serde_json::from_str(&raw).context("JSON root must be a job array")?;

    let url = format!("{base}/api/v1/admin/ingest/crawler-jobs");

    if args.dry_run {
        println!("Would POST {} rows to {url}", jobs.len());
        let missing = count_rows_missing_title_or_job_id(&jobs);
        if missing > 0 {
            eprintln!("Rows missing title or job_id: {missing}");
        }
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;

    let body = serde_json::json!({ "jobs": jobs });
    let mut req = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body);
    if !args.admin_key.is_empty() {
        req = req.header("X-Admin-Key", &args.admin_key);
    }

    let resp = req.send().await.context("POST request")?;
    let status = resp.status();
    let text = resp.text().await.context("read response body")?;

    if !status.is_success() {
        anyhow::bail!("HTTP {}: {}", status, text);
    }

    let payload: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    let summary = serde_json::json!({
        "imported": payload.get("imported"),
        "updated": payload.get("updated"),
        "skipped": payload.get("skipped"),
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
