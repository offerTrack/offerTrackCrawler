//! Merge two `jobs.json`-shaped arrays (e.g. `offertrack-crawl` + SPA Playwright export), then canonical-URL dedupe + stable job_id (same rules as `offertrack-crawl`).

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{Duration as ChDuration, Utc};
use clap::Parser;
use csv::Writer;
use offertrack_crawler::job::{
    assign_canonical_job_ids, dedupe_merge_by_canonical_url, ExportRow, JobPosting,
};
#[derive(Parser, Debug)]
#[command(name = "offertrack-merge-jobs")]
#[command(about = "Merge two job JSON exports + dedupe by canonical URL (SPA / extra feeds + crawl).")]
struct Args {
    /// Primary file (e.g. `out/jobs.json` from offertrack-crawl)
    #[arg(long)]
    base: PathBuf,
    /// Extra jobs: file path, or `-` to read a JSON array from stdin (e.g. pipe from `crawl.mjs --stdout`). If omitted or the file path is missing, treated as an empty array.
    #[arg(long)]
    extra: Option<PathBuf>,
    /// Write merged JSON to `--base` (single artifact: overwrite crawl output). Mutually exclusive with `--out`.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    in_place: bool,
    /// Output JSON path (required unless `--in-place`).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Write `<out-stem>.csv` next to JSON
    #[arg(long, action = clap::ArgAction::SetTrue)]
    csv: bool,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    csv_include_jd: bool,
    /// Keep only listings whose `posted_date` is within N days (default 3).
    /// Listings with missing `posted_date` are kept unless `--drop-missing-posted-date`.
    #[arg(long, default_value_t = 3)]
    freshness_days: i64,
    /// When set, rows with missing `posted_date` are filtered out.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    drop_missing_posted_date: bool,
}

fn is_fresh(job: &JobPosting, freshness_days: i64, drop_missing_posted_date: bool) -> bool {
    let Some(posted) = job.posted_date else {
        return !drop_missing_posted_date;
    };
    let cutoff = Utc::now().naive_utc() - ChDuration::days(freshness_days.max(0));
    posted >= cutoff
}

fn read_export_rows(path: &Path) -> Result<Vec<ExportRow>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let rows: Vec<ExportRow> =
        serde_json::from_str(&text).with_context(|| format!("parse JSON {}", path.display()))?;
    Ok(rows)
}

fn extra_is_stdin(p: &Path) -> bool {
    p.as_os_str() == std::ffi::OsStr::new("-")
}

fn read_export_rows_stdin() -> Result<Vec<ExportRow>> {
    let mut text = String::new();
    std::io::stdin()
        .read_to_string(&mut text)
        .context("read extra JSON from stdin")?;
    let text = text.trim();
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<ExportRow> =
        serde_json::from_str(text).context("parse stdin JSON as ExportRow[]")?;
    Ok(rows)
}

fn read_extra_rows(extra: &Option<PathBuf>) -> Result<Vec<ExportRow>> {
    match extra {
        None => Ok(Vec::new()),
        Some(p) if extra_is_stdin(p) => read_export_rows_stdin(),
        Some(p) if !p.exists() => {
            eprintln!(
                "offertrack-merge-jobs: extra file missing ({}), using 0 rows",
                p.display()
            );
            Ok(Vec::new())
        }
        Some(p) => read_export_rows(p),
    }
}

fn write_merged_jobs_csv(path: &Path, jobs: &[JobPosting], include_jd: bool) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let mut wtr = Writer::from_path(path)
        .with_context(|| format!("open csv {}", path.display()))?;
    if include_jd {
        wtr.write_record([
            "job_id",
            "title",
            "company",
            "location",
            "url",
            "posted_date",
            "source",
            "jd",
        ])?;
        for j in jobs {
            wtr.write_record([
                &j.job_id,
                &j.title,
                &j.company,
                j.location.as_deref().unwrap_or(""),
                &j.url,
                &j.posted_date
                    .map(|d| d.format("%Y-%m-%dT%H:%M:%S").to_string())
                    .unwrap_or_default(),
                j.source.as_deref().unwrap_or(""),
                &j.description.clone().unwrap_or_default(),
            ])?;
        }
    } else {
        wtr.write_record([
            "job_id",
            "title",
            "company",
            "location",
            "url",
            "posted_date",
            "source",
        ])?;
        for j in jobs {
            wtr.write_record([
                &j.job_id,
                &j.title,
                &j.company,
                j.location.as_deref().unwrap_or(""),
                &j.url,
                &j.posted_date
                    .map(|d| d.format("%Y-%m-%dT%H:%M:%S").to_string())
                    .unwrap_or_default(),
                j.source.as_deref().unwrap_or(""),
            ])?;
        }
    }
    wtr.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let out_path: PathBuf = match (&args.out, args.in_place) {
        (_, true) if args.out.is_some() => {
            anyhow::bail!("--in-place cannot be used with --out");
        }
        (_, true) => args.base.clone(),
        (None, false) => {
            anyhow::bail!("specify --out PATH or use --in-place to overwrite --base");
        }
        (Some(p), false) => p.clone(),
    };

    let mut jobs: Vec<JobPosting> = read_export_rows(&args.base)?
        .into_iter()
        .map(ExportRow::into_job_posting)
        .collect();
    let n_base = jobs.len();
    let extra_rows = read_extra_rows(&args.extra)?;
    jobs.extend(
        extra_rows
            .into_iter()
            .map(ExportRow::into_job_posting),
    );
    let n_extra = jobs.len() - n_base;
    let before_dedup = jobs.len();
    let (mut jobs, dedup_removed) = dedupe_merge_by_canonical_url(jobs);
    let before_freshness = jobs.len();
    jobs.retain(|j| {
        is_fresh(
            j,
            args.freshness_days,
            args.drop_missing_posted_date,
        )
    });
    let freshness_filtered_out = before_freshness - jobs.len();
    assign_canonical_job_ids(&mut jobs);

    if let Some(dir) = out_path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let rows: Vec<ExportRow> = jobs.iter().map(ExportRow::from).collect();
    std::fs::write(
        &out_path,
        serde_json::to_string_pretty(&rows).context("serialize jobs")?,
    )
    .with_context(|| format!("write {}", out_path.display()))?;

    let csv_path = out_path.with_extension("csv");
    let csv_written = if args.csv {
        write_merged_jobs_csv(&csv_path, &jobs, args.csv_include_jd)?;
        Some(csv_path.to_string_lossy().to_string())
    } else {
        None
    };

    let summary = serde_json::json!({
        "runner": "rust-offertrack-merge-jobs",
        "base_rows": n_base,
        "extra_rows": n_extra,
        "concatenated_before_dedup": before_dedup,
        "dedup_removed_duplicate_urls": dedup_removed,
        "freshness_days": args.freshness_days,
        "drop_missing_posted_date": args.drop_missing_posted_date,
        "freshness_filtered_out_rows": freshness_filtered_out,
        "total_merged_unique_listings": jobs.len(),
        "output_path": out_path.to_string_lossy(),
        "csv_path": csv_written,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);

    Ok(())
}
