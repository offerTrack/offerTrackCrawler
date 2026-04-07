//! Workday Candidate Experience (CXS) list API — same JSON POST the careers SPA uses.
//! Optional GET `.../wday/cxs/{org}/{site}{externalPath}` per job for HTML description.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, ACCEPT, ORIGIN, REFERER};
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

/// Best-effort posted time from Workday CXS list item or `jobPostingInfo` detail blob.
fn posted_from_workday_job_blob(v: &Value) -> Option<chrono::NaiveDateTime> {
    for key in [
        "postedOn",
        "postedDate",
        "timeApplicationPosted",
        "startDate",
        "jobPostingStartDate",
        "createdOn",
    ] {
        if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
            if let Some(d) = parse_date(Some(s)) {
                return Some(d);
            }
        }
    }
    None
}

#[derive(Default)]
struct WorkdayDetailPatch {
    description: Option<String>,
    posted_date: Option<chrono::NaiveDateTime>,
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let company = source
        .get("company")
        .and_then(|v| v.as_str())
        .context("workday: missing company")?;

    let workday_host = source
        .get("workday_host")
        .and_then(|v| v.as_str())
        .map(|s| {
            s.trim()
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/')
        })
        .context("workday: missing workday_host (e.g. blackstone.wd1.myworkdayjobs.com)")?;

    let cxs_org = source
        .get("cxs_org")
        .and_then(|v| v.as_str())
        .context("workday: missing cxs_org (first path segment in /wday/cxs/ORG/SITE/jobs)")?;

    let cxs_site = source
        .get("cxs_site")
        .and_then(|v| v.as_str())
        .context("workday: missing cxs_site (second path segment, often from /en-US/SITE/...)")?;

    let locale = source
        .get("locale")
        .and_then(|v| v.as_str())
        .unwrap_or("en-US");

    let api_url = source
        .get("cxs_jobs_url")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| {
            format!(
                "https://{}/wday/cxs/{}/{}/jobs",
                workday_host, cxs_org, cxs_site
            )
        });

    let referer = format!("https://{}/{}/{}", workday_host, locale, cxs_site);
    let origin = format!("https://{}", workday_host);

    let page_limit = source
        .get("page_limit")
        .or_else(|| source.get("result_limit"))
        .and_then(|v| v.as_u64())
        .unwrap_or(20)
        .clamp(1, 100) as usize;

    let max_jobs = source
        .get("max_jobs")
        .and_then(|v| v.as_u64())
        .unwrap_or(500)
        .max(1) as usize;

    let page_delay_ms = source
        .get("page_delay_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(400);

    let fetch_descriptions = source
        .get("fetch_job_descriptions")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let max_description_fetches = source
        .get("max_description_fetches")
        .and_then(|v| v.as_u64())
        .unwrap_or(200)
        .max(1) as usize;
    let description_concurrency = source
        .get("description_fetch_concurrency")
        .and_then(|v| v.as_u64())
        .unwrap_or(4)
        .max(1) as usize;
    let description_fetch_delay_ms = source
        .get("description_fetch_delay_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(120);
    let fallback_posted_date_to_now = source
        .get("fallback_posted_date_to_now")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let extra_headers = vec![
        ("Accept-Language".into(), "en-US".into()),
        ("Origin".into(), origin.clone()),
        ("Referer".into(), referer.clone()),
    ];

    let source_tag = format!("workday:{}/{}", workday_host, cxs_site);
    let mut out: Vec<JobPosting> = Vec::new();
    let mut paths_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut offset: i64 = 0;

    loop {
        let body = json!({
            "appliedFacets": Value::Object(Default::default()),
            "limit": page_limit,
            "offset": offset,
            "searchText": ""
        });

        let text = http_fetch::post_json_text(
            client,
            &api_url,
            &body,
            &extra_headers,
            Some(throttle.as_ref()),
        )
        .await?;
        let v: Value = serde_json::from_str(&text).context("workday: JSON parse")?;
        let jobs_arr = v
            .get("jobPostings")
            .and_then(|x| x.as_array())
            .context("workday: missing jobPostings")?;

        if jobs_arr.is_empty() {
            break;
        }

        let mut page_new = 0usize;
        for item in jobs_arr {
            let title = item.get("title").and_then(|x| x.as_str()).unwrap_or("").trim();
            let ext = item
                .get("externalPath")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim();
            if title.is_empty() || ext.is_empty() {
                continue;
            }
            if paths_seen.contains(ext) {
                return finish_workday(
                    client,
                    out,
                    workday_host,
                    cxs_org,
                    cxs_site,
                    locale,
                    &origin,
                    &referer,
                    fetch_descriptions,
                    max_description_fetches,
                    description_concurrency,
                    description_fetch_delay_ms,
                    throttle,
                )
                .await;
            }
            paths_seen.insert(ext.to_string());

            let listing_url = format!(
                "https://{}/{}/{}{}",
                workday_host, locale, cxs_site, ext
            );
            let loc = item
                .get("locationsText")
                .and_then(|x| x.as_str())
                .map(String::from);

            let posted_list = posted_from_workday_job_blob(item).or_else(|| {
                if fallback_posted_date_to_now {
                    Some(chrono::Utc::now().naive_utc())
                } else {
                    None
                }
            });

            out.push(JobPosting {
                title: title.to_string(),
                company: company.to_string(),
                url: listing_url,
                location: loc,
                description: None,
                posted_date: posted_list,
                source: Some(source_tag.clone()),
                job_id: String::new(),
                raw: item.clone(),
            });
            page_new += 1;

            if out.len() >= max_jobs {
                return finish_workday(
                    client,
                    out,
                    workday_host,
                    cxs_org,
                    cxs_site,
                    locale,
                    &origin,
                    &referer,
                    fetch_descriptions,
                    max_description_fetches,
                    description_concurrency,
                    description_fetch_delay_ms,
                    throttle,
                )
                .await;
            }
        }

        if page_new == 0 {
            break;
        }
        if (jobs_arr.len() as usize) < page_limit {
            break;
        }

        offset += page_limit as i64;
        if page_delay_ms > 0 {
            sleep(Duration::from_millis(page_delay_ms)).await;
        }
    }

    finish_workday(
        client,
        out,
        workday_host,
        cxs_org,
        cxs_site,
        locale,
        &origin,
        &referer,
        fetch_descriptions,
        max_description_fetches,
        description_concurrency,
        description_fetch_delay_ms,
        throttle,
    )
    .await
}

async fn finish_workday(
    client: &reqwest::Client,
    mut out: Vec<JobPosting>,
    workday_host: &str,
    cxs_org: &str,
    cxs_site: &str,
    _locale: &str,
    origin: &str,
    referer: &str,
    fetch_descriptions: bool,
    max_description_fetches: usize,
    description_concurrency: usize,
    description_fetch_delay_ms: u64,
    throttle: &Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let mut descriptions_fetched = 0u32;
    let mut descriptions_failed = 0u32;
    let mut posted_dates_from_detail = 0u32;

    if fetch_descriptions && !out.is_empty() {
        let n = out.len().min(max_description_fetches);

        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            "application/json"
                .parse()
                .map_err(|e| anyhow::anyhow!("header: {e}"))?,
        );
        headers.insert(
            ORIGIN,
            origin
                .parse()
                .map_err(|e| anyhow::anyhow!("header: {e}"))?,
        );
        headers.insert(
            REFERER,
            referer
                .parse()
                .map_err(|e| anyhow::anyhow!("header: {e}"))?,
        );

        let sem = Arc::new(Semaphore::new(description_concurrency));
        let mut set: JoinSet<(usize, Result<String, anyhow::Error>)> = JoinSet::new();

        for i in 0..n {
            let ext = match out[i].raw.get("externalPath").and_then(|x| x.as_str()) {
                Some(e) if !e.is_empty() => e.to_string(),
                _ => continue,
            };
            let detail_url = format!(
                "https://{}/wday/cxs/{}/{}{}",
                workday_host, cxs_org, cxs_site, ext
            );
            let client = client.clone();
            let headers = headers.clone();
            let sem = sem.clone();
            let th = Arc::clone(throttle);
            set.spawn(async move {
                let _p = sem.acquire_owned().await.ok();
                if description_fetch_delay_ms > 0 {
                    sleep(Duration::from_millis(description_fetch_delay_ms)).await;
                }
                let text = http_fetch::get_text_with_headers(
                    &client,
                    &detail_url,
                    &headers,
                    Some(th.as_ref()),
                )
                .await;
                (i, text)
            });
        }

        let mut updates: HashMap<usize, WorkdayDetailPatch> = HashMap::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((i, Ok(body))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&body) {
                        let jpi = v.get("jobPostingInfo");
                        let mut patch = WorkdayDetailPatch::default();
                        if let Some(info) = jpi {
                            if let Some(html) = info
                                .get("jobDescription")
                                .and_then(|x| x.as_str())
                                .filter(|s| !s.trim().is_empty())
                            {
                                patch.description = Some(html.to_string());
                                descriptions_fetched += 1;
                            }
                            if let Some(p) = posted_from_workday_job_blob(info) {
                                patch.posted_date = Some(p);
                            }
                        }
                        if patch.description.is_some() || patch.posted_date.is_some() {
                            updates.insert(i, patch);
                            continue;
                        }
                    }
                    descriptions_failed += 1;
                }
                Ok((_, Err(_))) => descriptions_failed += 1,
                Err(_) => descriptions_failed += 1,
            }
        }

        for (i, patch) in updates {
            if let Some(j) = out.get_mut(i) {
                if let Some(html) = patch.description {
                    j.description = Some(html);
                }
                if j.posted_date.is_none() {
                    if let Some(p) = patch.posted_date {
                        j.posted_date = Some(p);
                        posted_dates_from_detail += 1;
                    }
                }
            }
        }
    }

    let detail = serde_json::json!({
        "fetch_job_descriptions": fetch_descriptions,
        "descriptions_fetched": descriptions_fetched,
        "descriptions_failed": descriptions_failed,
        "posted_dates_filled_from_detail": posted_dates_from_detail,
    });

    Ok(CrawlOutput {
        jobs: out,
        detail: Some(detail),
    })
}
