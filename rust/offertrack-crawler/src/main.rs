//! offerTrack job crawler (Rust) — `api_sources` + HTML `sites` (Schema.org) → same `jobs.json` + SQLite contract.

mod html;
mod http_fetch;
mod registry;
mod sources;
mod storage;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{Duration as ChDuration, Utc};
use clap::Parser;
use csv::Writer;
use serde_json::Value;
use url::Url;

use offertrack_crawler::job::{
    assign_canonical_job_ids, dedupe_merge_by_canonical_url, ExportRow, JobPosting, MinimalRow,
};

use crate::html::HtmlCrawlSettings;
use crate::sources::ApiSourceReport;
use crate::storage::JobStorage;

#[derive(Clone, Copy)]
enum MissingPostedDatePolicy {
    /// Same as legacy: missing `posted_date` passes freshness filter.
    Fresh,
    /// Drop listings with no `posted_date` when filtering.
    Stale,
    /// Use SQLite `first_seen_at` for that URL when `posted_date` is absent (new rows count as fresh).
    DbFirstSeen,
}

fn missing_posted_date_policy(cfg: &Value) -> MissingPostedDatePolicy {
    match cfg
        .get("missing_posted_date_policy")
        .and_then(|v| v.as_str())
        .unwrap_or("fresh")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "stale" | "exclude" | "drop" => MissingPostedDatePolicy::Stale,
        "db_first_seen" | "database_first_seen" | "first_seen" => MissingPostedDatePolicy::DbFirstSeen,
        _ => MissingPostedDatePolicy::Fresh,
    }
}

fn is_fresh_with_policy(
    job: &JobPosting,
    freshness_days: i64,
    policy: MissingPostedDatePolicy,
    storage: &JobStorage,
) -> anyhow::Result<bool> {
    let cutoff = Utc::now().naive_utc() - ChDuration::days(freshness_days);
    if let Some(posted) = job.posted_date {
        return Ok(posted >= cutoff);
    }
    match policy {
        MissingPostedDatePolicy::Fresh => Ok(true),
        MissingPostedDatePolicy::Stale => Ok(false),
        MissingPostedDatePolicy::DbFirstSeen => {
            let sig = offertrack_crawler::job::listing_signature_canonical_url(&job.url);
            if let Some(first) = storage.first_seen_for_signature(&sig)? {
                Ok(first >= cutoff)
            } else {
                Ok(true)
            }
        }
    }
}

fn rollup_api_source_totals(reports: &[ApiSourceReport]) -> Value {
    use serde_json::json;
    use std::collections::HashMap;
    #[derive(Default)]
    struct Agg {
        runs: u64,
        ok: u64,
        fail: u64,
        jobs: u64,
        errors: Vec<String>,
    }
    let mut m: HashMap<String, Agg> = HashMap::new();
    for r in reports {
        let a = m.entry(r.r#type.clone()).or_default();
        a.runs += 1;
        if r.ok {
            a.ok += 1;
        } else {
            a.fail += 1;
            if let Some(ref e) = r.error {
                if a.errors.len() < 5 && !e.is_empty() {
                    a.errors.push(e.clone());
                }
            }
        }
        a.jobs += r.jobs_fetched as u64;
    }
    let obj: serde_json::Map<String, Value> = m
        .into_iter()
        .map(|(k, a)| {
            (
                k,
                json!({
                    "runs": a.runs,
                    "ok_runs": a.ok,
                    "fail_runs": a.fail,
                    "jobs_fetched": a.jobs,
                    "sample_errors": a.errors,
                }),
            )
        })
        .collect();
    Value::Object(obj)
}

fn crawl_alerts_from_reports(reports: &[ApiSourceReport]) -> Vec<String> {
    let mut out = Vec::new();
    for r in reports {
        let Some(d) = &r.detail else { continue };
        if d.get("alert").and_then(|x| x.as_bool()) != Some(true) {
            continue;
        }
        let sum = d
            .get("alert_summary")
            .and_then(|x| x.as_str())
            .unwrap_or("see api_source_runs[].detail");
        out.push(format!("{} ({}) — {}", r.r#type, r.label, sum));
    }
    out
}

fn default_link_hints_for_source_type(t: &str) -> Vec<String> {
    match t {
        "icims" => vec!["/jobs/".into(), "jobdetails".into()],
        "workable" => vec!["/jobs/".into()],
        "bamboohr" => vec!["/careers/".into(), "jobopeningid".into()],
        _ => vec!["/jobs/".into(), "/careers/".into()],
    }
}

fn append_registry_html_fallback_sites(
    cfg: &Value,
    api_sources: &[Value],
    sites: &mut Vec<Value>,
) {
    let enabled = cfg
        .get("registry_html_fallback_from_api_sources")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !enabled {
        return;
    }
    let max_new = cfg
        .get("registry_html_fallback_max_sites")
        .and_then(|v| v.as_u64())
        .unwrap_or(30) as usize;
    if max_new == 0 {
        return;
    }

    let mut added = 0usize;
    for src in api_sources {
        if added >= max_new {
            break;
        }
        let Some(career_url) = src
            .get("_career_url")
            .or_else(|| src.get("career_url"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let Ok(u) = Url::parse(career_url) else {
            continue;
        };
        let Some(domain) = u.host_str() else {
            continue;
        };
        let t = src.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let hints = default_link_hints_for_source_type(t);
        sites.push(serde_json::json!({
            "enabled": true,
            "domain": domain,
            "start_urls": [career_url],
            "extractor": "GenericSchemaOrgExtractor",
            "job_link_href_contains": hints,
        }));
        added += 1;
    }
}

#[derive(Parser, Debug)]
#[command(name = "offertrack-crawl")]
#[command(about = "Crawl job API feeds and HTML sites (Schema.org) into jobs.json + SQLite.")]
struct Args {
    #[arg(long, default_value = "config/crawl_sites.json")]
    config: PathBuf,
    /// Merge employers registry JSON (overrides `registry` path in config when set)
    #[arg(long)]
    registry: Option<PathBuf>,
    #[arg(long, default_value = "out/jobs.json")]
    out: PathBuf,
    #[arg(long, default_value = "state/jobs.db")]
    db: PathBuf,
    #[arg(long, help = "Export job_id, jd, first_seen_at only (after DB upsert)")]
    minimal_export: bool,
    /// Write `<out-stem>.csv` next to JSON (default: off — only `jobs.json` in out/).
    #[arg(long, action = clap::ArgAction::SetTrue)]
    csv: bool,
    /// Include a full `jd` column in CSV (very large; default omits jd — use JSON for descriptions).
    #[arg(long, action = clap::ArgAction::SetTrue)]
    csv_include_jd: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg_text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("read config {}", args.config.display()))?;
    let cfg: Value = serde_json::from_str(&cfg_text)?;

    let freshness_days = cfg
        .get("freshness_days")
        .and_then(|v| v.as_i64())
        .unwrap_or(3);
    let api_max_concurrent = cfg
        .get("api_max_concurrent")
        .and_then(|v| v.as_u64())
        .unwrap_or(6)
        .max(1) as usize;
    let per_host_min_interval_ms = cfg
        .get("per_host_min_interval_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(400);
    let api_source_retry_attempts = cfg
        .get("api_source_retry_attempts")
        .and_then(|v| v.as_u64())
        .unwrap_or(2)
        .max(1) as u32;
    let posted_policy = missing_posted_date_policy(&cfg);

    let mut sites: Vec<Value> = cfg
        .get("sites")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut api_sources: Vec<Value> = cfg
        .get("api_sources")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let reg_path = args.registry.clone().or_else(|| {
        cfg.get("registry")
            .and_then(|v| v.as_str())
            .map(PathBuf::from)
    });
    let reg_resolved = reg_path.as_ref().map(|p| {
        if p.is_absolute() {
            p.clone()
        } else {
            args
                .config
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join(p)
        }
    });
    if let Some(ref p) = reg_resolved {
        if p.exists() {
            let (reg_api, reg_sites) = registry::load_and_expand(p)?;
            eprintln!(
                "[INFO] registry {} → +{} api_sources, +{} sites",
                p.display(),
                reg_api.len(),
                reg_sites.len()
            );
            api_sources.extend(reg_api);
            sites.extend(reg_sites);
            registry::dedupe_api_sources(&mut api_sources);
            registry::dedupe_sites(&mut sites);
        } else {
            eprintln!("[WARN] registry file missing: {}", p.display());
        }
    }
    append_registry_html_fallback_sites(&cfg, &api_sources, &mut sites);
    registry::dedupe_sites(&mut sites);

    if let Some(p) = args.db.parent() {
        std::fs::create_dir_all(p).ok();
    }
    let mut storage = JobStorage::open(&args.db)?;

    let html_settings = HtmlCrawlSettings::from_config(&cfg);
    let client = reqwest::Client::builder()
        .user_agent(&html_settings.user_agent)
        .timeout(Duration::from_secs(120))
        .build()?;
    let n_api_sources = api_sources.len();
    let n_sites = sites.len();

    let ((mut api_jobs, api_source_reports), html_jobs) = tokio::join!(
        sources::crawl_api_sources(
            &client,
            &api_sources,
            api_max_concurrent,
            per_host_min_interval_ms,
            api_source_retry_attempts,
        ),
        html::crawl_html_sites(&client, &html_settings, &sites),
    );
    let html_job_count = html_jobs.len();
    api_jobs.extend(html_jobs);
    api_jobs.retain(|j| match is_fresh_with_policy(j, freshness_days, posted_policy, &storage) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[WARN] freshness policy: {e}");
            true
        }
    });
    let total_before_dedup = api_jobs.len();
    let (mut api_jobs, dedup_removed) = dedupe_merge_by_canonical_url(api_jobs);
    assign_canonical_job_ids(&mut api_jobs);

    if let Some(p) = args.out.parent() {
        std::fs::create_dir_all(p).ok();
    }

    let stats = storage.upsert_jobs(&api_jobs)?;

    let output_json = if args.minimal_export {
        let mut rows: Vec<MinimalRow> = Vec::new();
        for j in &api_jobs {
            if let Some((first_seen, desc)) = storage.first_seen_and_description(&j.job_id)? {
                rows.push(MinimalRow {
                    job_id: j.job_id.clone(),
                    jd: desc.unwrap_or_else(|| j.description.clone().unwrap_or_default()),
                    first_seen_at: first_seen,
                });
            }
        }
        serde_json::to_string_pretty(&rows)?
    } else {
        let rows: Vec<ExportRow> = api_jobs.iter().map(ExportRow::from).collect();
        serde_json::to_string_pretty(&rows)?
    };

    std::fs::write(&args.out, output_json)?;

    let csv_path = args.out.with_extension("csv");
    let csv_written = if args.csv {
        write_merged_jobs_csv(&csv_path, &api_jobs, args.csv_include_jd)?;
        Some(csv_path.to_string_lossy().to_string())
    } else {
        None
    };

    let api_totals = rollup_api_source_totals(&api_source_reports);
    let crawl_alerts = crawl_alerts_from_reports(&api_source_reports);

    let summary = serde_json::json!({
        "runner": "rust-offertrack-crawl",
        "total_after_freshness_before_dedup": total_before_dedup,
        "dedup_removed_duplicate_urls": dedup_removed,
        "total_merged_unique_listings": api_jobs.len(),
        "total_crawled_after_fresh_filter": api_jobs.len(),
        "html_sites_jobs_before_fresh_filter": html_job_count,
        "missing_posted_date_policy": match posted_policy {
            MissingPostedDatePolicy::Fresh => "fresh",
            MissingPostedDatePolicy::Stale => "stale",
            MissingPostedDatePolicy::DbFirstSeen => "db_first_seen",
        },
        "api_sources_count": n_api_sources,
        "api_max_concurrent": api_max_concurrent,
        "per_host_min_interval_ms": per_host_min_interval_ms,
        "api_source_retry_attempts": api_source_retry_attempts,
        "api_source_totals_by_type": api_totals,
        "crawl_alerts": crawl_alerts,
        "api_source_runs": api_source_reports,
        "sites_count": n_sites,
        "registry_loaded": reg_resolved.as_ref().map(|p| p.exists()).unwrap_or(false),
        "registry_path": reg_resolved.as_ref().and_then(|p| p.to_str()),
        "db_inserted_new": stats.inserted,
        "db_updated_existing": stats.updated,
        "output_path": args.out.to_string_lossy(),
        "csv_path": csv_written,
        "db_path": args.db.to_string_lossy(),
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);

    Ok(())
}

fn write_merged_jobs_csv(path: &Path, jobs: &[JobPosting], include_jd: bool) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let mut wtr = Writer::from_path(path)
        .with_context(|| format!("open csv {}", path.display()))?;

    let mut header = vec!["job_id", "title", "company", "location", "url", "posted_date", "source"];
    if include_jd {
        header.push("jd");
    }
    wtr.write_record(&header)?;

    for j in jobs {
        let posted = j
            .posted_date
            .map(|d| d.format("%Y-%m-%dT%H:%M:%S").to_string())
            .unwrap_or_default();
        let mut rec = vec![
            j.job_id.clone(),
            j.title.clone(),
            j.company.clone(),
            j.location.clone().unwrap_or_default(),
            j.url.clone(),
            posted,
            j.source.clone().unwrap_or_default(),
        ];
        if include_jd {
            rec.push(j.description.clone().unwrap_or_default());
        }
        wtr.write_record(&rec)?;
    }
    wtr.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::ApiSourceReport;

    fn make_report(t: &str, label: &str, ok: bool, jobs: usize, detail: Option<Value>) -> ApiSourceReport {
        ApiSourceReport {
            r#type: t.into(),
            label: label.into(),
            jobs_fetched: jobs,
            ok,
            error: if ok { None } else { Some("err".into()) },
            detail,
        }
    }

    #[test]
    fn missing_posted_date_policy_fresh() {
        let cfg = serde_json::json!({});
        assert!(matches!(missing_posted_date_policy(&cfg), MissingPostedDatePolicy::Fresh));
    }

    #[test]
    fn missing_posted_date_policy_stale_variants() {
        for v in ["stale", "exclude", "drop"] {
            let cfg = serde_json::json!({ "missing_posted_date_policy": v });
            assert!(
                matches!(missing_posted_date_policy(&cfg), MissingPostedDatePolicy::Stale),
                "failed for '{v}'"
            );
        }
    }

    #[test]
    fn missing_posted_date_policy_db_first_seen_variants() {
        for v in ["db_first_seen", "database_first_seen", "first_seen"] {
            let cfg = serde_json::json!({ "missing_posted_date_policy": v });
            assert!(
                matches!(missing_posted_date_policy(&cfg), MissingPostedDatePolicy::DbFirstSeen),
                "failed for '{v}'"
            );
        }
    }

    #[test]
    fn missing_posted_date_policy_unknown_defaults_to_fresh() {
        let cfg = serde_json::json!({ "missing_posted_date_policy": "unknown_value" });
        assert!(matches!(missing_posted_date_policy(&cfg), MissingPostedDatePolicy::Fresh));
    }

    #[test]
    fn rollup_counts_by_type() {
        let reports = vec![
            make_report("greenhouse", "A", true, 10, None),
            make_report("greenhouse", "B", false, 0, None),
            make_report("lever", "C", true, 5, None),
        ];
        let totals = rollup_api_source_totals(&reports);
        let gh = &totals["greenhouse"];
        assert_eq!(gh["runs"], 2);
        assert_eq!(gh["ok_runs"], 1);
        assert_eq!(gh["fail_runs"], 1);
        assert_eq!(gh["jobs_fetched"], 10);

        let lv = &totals["lever"];
        assert_eq!(lv["runs"], 1);
        assert_eq!(lv["jobs_fetched"], 5);
    }

    #[test]
    fn rollup_empty_reports() {
        let totals = rollup_api_source_totals(&[]);
        assert!(totals.as_object().unwrap().is_empty());
    }

    #[test]
    fn crawl_alerts_empty_when_no_alert_flag() {
        let reports = vec![
            make_report("greenhouse", "A", true, 5, Some(serde_json::json!({ "some": "data" }))),
            make_report("lever", "B", false, 0, None),
        ];
        let alerts = crawl_alerts_from_reports(&reports);
        assert!(alerts.is_empty());
    }

    #[test]
    fn crawl_alerts_included_when_alert_true() {
        let detail = serde_json::json!({ "alert": true, "alert_summary": "zero rows on page 1" });
        let reports = vec![make_report("jobright", "MyCompany", true, 0, Some(detail))];
        let alerts = crawl_alerts_from_reports(&reports);
        assert_eq!(alerts.len(), 1);
        assert!(alerts[0].contains("zero rows on page 1"));
    }

    #[test]
    fn default_link_hints_icims() {
        let hints = default_link_hints_for_source_type("icims");
        assert!(hints.iter().any(|h| h.contains("/jobs/")));
        assert!(hints.iter().any(|h| h.contains("jobdetails")));
    }

    #[test]
    fn default_link_hints_unknown_type() {
        let hints = default_link_hints_for_source_type("greenhouse");
        assert!(hints.iter().any(|h| h.contains("/jobs/")));
        assert!(hints.iter().any(|h| h.contains("/careers/")));
    }

    #[test]
    fn write_csv_without_jd() {
        let job = offertrack_crawler::job::JobPosting {
            title: "SWE".into(),
            company: "Acme".into(),
            url: "https://example.com/jobs/1".into(),
            job_id: "uuid-1".into(),
            ..Default::default()
        };
        let dir = std::env::temp_dir();
        let path = dir.join("test_no_jd.csv");
        write_merged_jobs_csv(&path, &[job], false).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("SWE"));
        assert!(!content.contains("jd"), "jd column should not appear");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_csv_with_jd() {
        let mut job = offertrack_crawler::job::JobPosting {
            title: "PM".into(),
            company: "Corp".into(),
            url: "https://example.com/jobs/2".into(),
            job_id: "uuid-2".into(),
            ..Default::default()
        };
        job.description = Some("Great JD text".into());
        let dir = std::env::temp_dir();
        let path = dir.join("test_with_jd.csv");
        write_merged_jobs_csv(&path, &[job], true).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("Great JD text"));
        let _ = std::fs::remove_file(&path);
    }
}
