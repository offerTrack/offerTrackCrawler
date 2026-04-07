use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

static NEXT_DATA: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"<script id="__NEXT_DATA__" type="application/json">([\s\S]*?)</script>"#).unwrap()
});

fn next_data_json(html: &str) -> Option<Value> {
    let cap = NEXT_DATA.captures(html)?;
    let raw = cap.get(1)?.as_str();
    serde_json::from_str(raw).ok()
}

fn company_from_summary(summary: Option<&str>) -> Option<String> {
    let s = summary?.trim();
    let re = Regex::new(r"^([A-Z0-9][A-Za-z0-9.&'\-\s]{1,72}?)\s+is\s+").ok()?;
    re.captures(s)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string())
}

fn company_from_detail(payload: &Value) -> Option<String> {
    let cr = payload
        .get("props")?
        .get("pageProps")?
        .get("dataSource")?
        .get("companyResult")?;
    cr.get("companyName")
        .or_else(|| cr.get("name"))
        .and_then(|v| v.as_str())
        .map(String::from)
}

async fn fetch_company_detail(
    client: &reqwest::Client,
    job_url: &str,
    throttle: Option<&http_fetch::HostThrottle>,
) -> Option<String> {
    let base = job_url.split('?').next()?;
    let html = http_fetch::get_text(client, base, throttle).await.ok()?;
    let payload = next_data_json(&html)?;
    company_from_detail(&payload)
}

#[derive(Default, Serialize)]
struct JobrightStats {
    list_urls_attempted: usize,
    list_pages_fetched_ok: usize,
    list_fetch_errors: usize,
    list_pages_missing_next_data: usize,
    job_list_arrays_seen: usize,
    job_rows_emitted: usize,
    rows_skipped_empty_title_or_url: usize,
    /// Set when parse health looks degraded (empty __NEXT_DATA__, zero rows after OK pages, etc.).
    alert: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    alert_summary: String,
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let mut list_urls: Vec<String> = source
        .get("job_list_urls")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if let Some(u) = source.get("url").and_then(|v| v.as_str()) {
        list_urls.push(u.to_string());
    }

    list_urls.retain(|u| !u.is_empty() && u.contains("jobright.ai"));
    list_urls.sort();
    list_urls.dedup();

    let mut stats = JobrightStats {
        list_urls_attempted: list_urls.len(),
        ..Default::default()
    };

    if list_urls.is_empty() {
        return Ok(CrawlOutput {
            jobs: vec![],
            detail: Some(json!(stats)),
        });
    }

    let fetch_detail = source
        .get("fetch_company_detail")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let max_details = source
        .get("max_detail_requests")
        .and_then(|v| v.as_u64())
        .unwrap_or(20) as usize;
    let detail_concurrency = source
        .get("detail_concurrency")
        .and_then(|v| v.as_u64())
        .unwrap_or(4) as usize;

    let mut jobs: Vec<JobPosting> = Vec::new();

    for list_url in &list_urls {
        let html = match http_fetch::get_text(client, list_url.as_str(), Some(throttle.as_ref()))
            .await
        {
            Ok(t) => {
                stats.list_pages_fetched_ok += 1;
                t
            }
            Err(e) => {
                stats.list_fetch_errors += 1;
                eprintln!("[WARN] jobright list ({list_url}): {e}");
                continue;
            }
        };

        let Some(payload) = next_data_json(&html) else {
            stats.list_pages_missing_next_data += 1;
            eprintln!("[WARN] jobright: no __NEXT_DATA__ ({list_url})");
            continue;
        };

        let job_list = payload
            .get("props")
            .and_then(|p| p.get("pageProps"))
            .and_then(|p| p.get("jobList"))
            .and_then(|j| j.as_array())
            .cloned()
            .unwrap_or_default();

        if !job_list.is_empty() {
            stats.job_list_arrays_seen += 1;
        }

        for item in job_list {
            let jr = item.get("jobResult").cloned().unwrap_or(Value::Null);
            let title = jr
                .get("jobTitle")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            let detail_url = jr
                .get("url")
                .or_else(|| jr.get("applyLink"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if title.is_empty() || detail_url.is_empty() {
                stats.rows_skipped_empty_title_or_url += 1;
                continue;
            }

            let summary = jr.get("jobSummary").and_then(|x| x.as_str());
            let company = company_from_summary(summary)
                .unwrap_or_else(|| "Jobright (company via detail or summary)".into());
            let loc = jr
                .get("jobLocation")
                .and_then(|x| x.as_str())
                .map(String::from);
            let posted = parse_date(jr.get("publishTime").and_then(|x| x.as_str()));

            stats.job_rows_emitted += 1;
            jobs.push(JobPosting {
                title,
                company,
                url: detail_url,
                location: loc,
                description: summary.map(String::from),
                posted_date: posted,
                source: Some("jobright.ai".into()),
                job_id: String::new(),
                raw: serde_json::json!({ "list_url": list_url, "jobResult": jr }),
            });
        }
    }

    if fetch_detail && !jobs.is_empty() {
        let n = jobs.len().min(max_details);
        let sem = std::sync::Arc::new(Semaphore::new(detail_concurrency.max(1)));
        let mut handles = Vec::new();
        for i in 0..n {
            let url = jobs[i].url.clone();
            let c = client.clone();
            let sem = sem.clone();
            let th = Arc::clone(throttle);
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok();
                fetch_company_detail(&c, &url, Some(th.as_ref())).await
            }));
        }
        for (i, h) in (0..n).zip(handles) {
            if let Ok(Some(co)) = h.await {
                if !co.is_empty() {
                    let j = &mut jobs[i];
                    j.company = co;
                    if let Value::Object(ref mut m) = j.raw {
                        m.insert(
                            "companyResolvedFrom".into(),
                            Value::String("detail".into()),
                        );
                    }
                }
            }
        }
    }

    let label = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or("jobright");
    let mut reasons: Vec<String> = Vec::new();
    if stats.list_fetch_errors > 0 {
        reasons.push(format!(
            "{} list page HTTP error(s)",
            stats.list_fetch_errors
        ));
    }
    if stats.list_pages_missing_next_data > 0 {
        reasons.push(format!(
            "{} list page(s) missing __NEXT_DATA__",
            stats.list_pages_missing_next_data
        ));
    }
    if stats.list_pages_fetched_ok > 0 && stats.job_rows_emitted == 0 {
        reasons.push(
            "fetched list HTML but emitted zero job rows (likely frontend schema drift)".into(),
        );
    }
    stats.alert = !reasons.is_empty();
    stats.alert_summary = reasons.join("; ");
    if stats.alert {
        eprintln!(
            "[ALERT] jobright ({label}): {}",
            stats.alert_summary
        );
    }

    Ok(CrawlOutput {
        jobs,
        detail: Some(serde_json::to_value(&stats)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_NEXT_DATA: &str = r#"<!DOCTYPE html><html><body>
<script id="__NEXT_DATA__" type="application/json">{
  "props": {
    "pageProps": {
      "jobList": [
        {
          "jobResult": {
            "jobTitle": "Staff Nurse",
            "url": "https://jobright.ai/jobs/abc",
            "jobSummary": "Acme Hospital is hiring nurses in Austin.",
            "jobLocation": "Austin, TX",
            "publishTime": "2026-01-15T12:00:00Z"
          }
        },
        {
          "jobResult": {
            "jobTitle": "",
            "url": "https://jobright.ai/jobs/bad"
          }
        }
      ]
    }
  }
}</script></body></html>"#;

    #[test]
    fn next_data_extracts_job_list() {
        let v = next_data_json(SAMPLE_NEXT_DATA).expect("parse __NEXT_DATA__");
        let list = v
            .get("props")
            .and_then(|p| p.get("pageProps"))
            .and_then(|p| p.get("jobList"))
            .and_then(|j| j.as_array())
            .expect("jobList");
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn company_from_summary_acme() {
        let c = company_from_summary(Some("Acme Hospital is hiring nurses."));
        assert_eq!(c.as_deref(), Some("Acme Hospital"));
    }

    const BAD_LIST_HTML: &str = "<!DOCTYPE html><html><body><p>no next data</p></body></html>";

    #[test]
    fn fixture_list_without_next_data_parses_no_payload() {
        assert!(next_data_json(BAD_LIST_HTML).is_none());
    }
}
