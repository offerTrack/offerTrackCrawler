mod amazon_jobs;
mod ashby;
mod bamboohr;
mod feed_source;
mod greenhouse;
mod icims;
mod jobright;
mod lever;
mod recruitee;
mod smartrecruiters;
mod workable;
mod workday;

use std::sync::Arc;

use anyhow::anyhow;
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::Semaphore;

use offertrack_crawler::job::JobPosting;

use crate::http_fetch::HostThrottle;

/// One API source run for crawl summary JSON.
#[derive(Debug, Clone, Serialize)]
pub struct ApiSourceReport {
    pub r#type: String,
    pub label: String,
    pub jobs_fetched: usize,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

/// Result from a single connector (jobs + optional stats object).
#[derive(Debug)]
pub struct CrawlOutput {
    pub jobs: Vec<JobPosting>,
    pub detail: Option<Value>,
}

impl CrawlOutput {
    pub fn jobs(jobs: Vec<JobPosting>) -> Self {
        Self {
            jobs,
            detail: None,
        }
    }
}

fn source_label(src: &Value) -> String {
    src.get("company")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            src.get("board")
                .or_else(|| src.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.chars().take(120).collect())
        })
        .unwrap_or_else(|| "(unknown)".into())
}

async fn run_one_source(
    client: reqwest::Client,
    src: Value,
    throttle: Arc<HostThrottle>,
) -> (ApiSourceReport, Vec<JobPosting>) {
    let enabled = src.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);
    if !enabled {
        return (
            ApiSourceReport {
                r#type: src
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string(),
                label: source_label(&src),
                jobs_fetched: 0,
                ok: true,
                error: None,
                detail: Some(Value::String("disabled".into())),
            },
            vec![],
        );
    }

    let Some(t) = src.get("type").and_then(|v| v.as_str()) else {
        return (
            ApiSourceReport {
                r#type: "?".into(),
                label: source_label(&src),
                jobs_fetched: 0,
                ok: false,
                error: Some("api_sources entry missing type".into()),
                detail: None,
            },
            vec![],
        );
    };

    let label = source_label(&src);
    let r = match t {
        "greenhouse" => greenhouse::crawl(&client, &src, &throttle).await,
        "lever" => lever::crawl(&client, &src, &throttle).await,
        "ashby" => ashby::crawl(&client, &src, &throttle).await,
        "rss" | "atom" => feed_source::crawl(&client, &src, &throttle).await,
        "jobright" => jobright::crawl(&client, &src, &throttle).await,
        "amazon_jobs" => amazon_jobs::crawl(&client, &src, &throttle).await,
        "workday" => workday::crawl(&client, &src, &throttle).await,
        "recruitee" => recruitee::crawl(&client, &src, &throttle).await,
        "smartrecruiters" => smartrecruiters::crawl(&client, &src, &throttle).await,
        "icims" => icims::crawl(&client, &src, &throttle).await,
        "bamboohr" => bamboohr::crawl(&client, &src, &throttle).await,
        "workable" => workable::crawl(&client, &src, &throttle).await,
        other => Err(anyhow!("unknown api source type: {other}")),
    };

    match r {
        Ok(out) => {
            let n = out.jobs.len();
            let min_expected_jobs = src
                .get("min_expected_jobs")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;
            let (ok, error) = if min_expected_jobs > 0 && n < min_expected_jobs {
                (
                    false,
                    Some(format!(
                        "jobs_fetched={} below min_expected_jobs={}",
                        n, min_expected_jobs
                    )),
                )
            } else {
                (true, None)
            };
            (
                ApiSourceReport {
                    r#type: t.to_string(),
                    label,
                    jobs_fetched: n,
                    ok,
                    error,
                    detail: out.detail,
                },
                out.jobs,
            )
        }
        Err(e) => (
            ApiSourceReport {
                r#type: t.to_string(),
                label,
                jobs_fetched: 0,
                ok: false,
                error: Some(e.to_string()),
                detail: None,
            },
            vec![],
        ),
    }
}

/// Crawl all `api_sources` with bounded concurrency, per-host pacing, and 429/5xx retries inside `http_fetch`.
pub async fn crawl_api_sources(
    client: &reqwest::Client,
    sources: &[Value],
    max_concurrent: usize,
    per_host_min_interval_ms: u64,
    retry_attempts_per_source: u32,
) -> (Vec<JobPosting>, Vec<ApiSourceReport>) {
    let throttle = Arc::new(HostThrottle::new(per_host_min_interval_ms));
    let sem = Arc::new(Semaphore::new(max_concurrent.max(1)));
    let mut handles = Vec::new();
    let attempts = retry_attempts_per_source.max(1);

    for src in sources {
        let src = src.clone();
        let c = client.clone();
        let sem = sem.clone();
        let th = throttle.clone();
        handles.push(tokio::spawn(async move {
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    return (
                        ApiSourceReport {
                            r#type: "?".into(),
                            label: "(semaphore)".into(),
                            jobs_fetched: 0,
                            ok: false,
                            error: Some("semaphore closed".into()),
                            detail: None,
                        },
                        vec![],
                    );
                }
            };
            let mut last = run_one_source(c.clone(), src.clone(), th.clone()).await;
            for a in 1..attempts {
                if last.0.ok {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2 * u64::from(a))).await;
                last = run_one_source(c.clone(), src.clone(), th.clone()).await;
            }
            last
        }));
    }

    let mut all_jobs = Vec::new();
    let mut reports = Vec::new();
    for h in handles {
        if let Ok(pair) = h.await {
            let (rep, mut jobs) = pair;
            reports.push(rep);
            all_jobs.append(&mut jobs);
        }
    }

    (all_jobs, reports)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_label_uses_company() {
        let v = serde_json::json!({ "company": "Stripe", "board": "stripe" });
        assert_eq!(source_label(&v), "Stripe");
    }

    #[test]
    fn source_label_falls_back_to_board() {
        let v = serde_json::json!({ "board": "my-board" });
        assert_eq!(source_label(&v), "my-board");
    }

    #[test]
    fn source_label_falls_back_to_url() {
        let v = serde_json::json!({ "url": "https://feeds.example.com/rss.xml" });
        assert_eq!(source_label(&v), "https://feeds.example.com/rss.xml");
    }

    #[test]
    fn source_label_unknown_fallback() {
        let v = serde_json::json!({});
        assert_eq!(source_label(&v), "(unknown)");
    }

    #[test]
    fn source_label_empty_company_falls_back() {
        let v = serde_json::json!({ "company": "  ", "board": "fallback" });
        assert_eq!(source_label(&v), "fallback");
    }

    #[test]
    fn crawl_output_jobs_constructor() {
        let jobs = vec![offertrack_crawler::job::JobPosting::default()];
        let out = CrawlOutput::jobs(jobs);
        assert_eq!(out.jobs.len(), 1);
        assert!(out.detail.is_none());
    }
}
