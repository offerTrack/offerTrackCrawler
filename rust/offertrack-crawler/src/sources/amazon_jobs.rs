//! Amazon Jobs — uses the same `search.json` endpoint as the public careers site (undocumented; respect robots/ToS).
use anyhow::{Context, Result};
use serde_json::Value;
use tokio::time::{sleep, Duration};
use url::Url;

use offertrack_crawler::date_parse::parse_english_month_day_year;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

fn build_search_url(
    locale_prefix: &str,
    offset: u64,
    result_limit: u32,
    base_query: &str,
    loc_query: &str,
    sort: &str,
) -> Result<String> {
    let loc = locale_prefix.trim();
    let loc = if loc.is_empty() { "/en" } else { loc };
    let path = format!(
        "https://www.amazon.jobs{}/search.json",
        loc.trim_end_matches('/')
    );
    let mut u = Url::parse(&path).with_context(|| format!("bad amazon jobs url base {path}"))?;
    {
        let mut q = u.query_pairs_mut();
        q.append_pair("offset", &offset.to_string());
        q.append_pair("result_limit", &result_limit.to_string());
        q.append_pair("sort", sort);
        if !base_query.is_empty() {
            q.append_pair("base_query", base_query);
        }
        if !loc_query.is_empty() {
            q.append_pair("loc_query", loc_query);
        }
    }
    Ok(u.to_string())
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let display_company = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or("Amazon");
    let locale_prefix = source
        .get("locale_prefix")
        .and_then(|v| v.as_str())
        .unwrap_or("/en");
    let base_query = source
        .get("base_query")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let loc_query = source
        .get("loc_query")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let sort = source
        .get("sort")
        .and_then(|v| v.as_str())
        .unwrap_or("recent");
    let result_limit = source
        .get("result_limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(100)
        .clamp(1, 500) as u32;
    let max_jobs = source
        .get("max_jobs")
        .and_then(|v| v.as_u64())
        .unwrap_or(500)
        .max(1) as usize;
    let page_delay_ms = source
        .get("page_delay_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(400);

    let source_tag = format!(
        "amazon_jobs:{}",
        locale_prefix.trim_start_matches('/').replace('/', "_")
    );
    let mut out = Vec::new();
    let mut offset: u64 = 0;

    loop {
        let url = build_search_url(
            locale_prefix,
            offset,
            result_limit,
            base_query,
            loc_query,
            sort,
        )?;
        let text = http_fetch::get_text(client, &url, Some(throttle.as_ref())).await?;
        let v: Value = serde_json::from_str(&text).context("amazon search.json parse")?;
        let jobs_arr = v
            .get("jobs")
            .and_then(|x| x.as_array())
            .context("amazon: missing jobs array")?;

        if jobs_arr.is_empty() {
            break;
        }

        for item in jobs_arr {
            let title = item.get("title").and_then(|x| x.as_str()).unwrap_or("").trim();
            let job_path = item
                .get("job_path")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim();
            if title.is_empty() || job_path.is_empty() {
                continue;
            }
            let listing_url = format!("https://www.amazon.jobs{}", job_path);
            let company = item
                .get("company_name")
                .and_then(|x| x.as_str())
                .unwrap_or(display_company)
                .to_string();
            let location = item
                .get("normalized_location")
                .or_else(|| item.get("location"))
                .and_then(|x| x.as_str())
                .map(String::from);
            let desc = item
                .get("description")
                .and_then(|x| x.as_str())
                .map(String::from);
            let posted = item
                .get("posted_date")
                .and_then(|x| x.as_str())
                .and_then(parse_english_month_day_year);

            out.push(JobPosting {
                title: title.to_string(),
                company,
                url: listing_url,
                location,
                description: desc,
                posted_date: posted,
                source: Some(source_tag.clone()),
                job_id: String::new(),
                raw: item.clone(),
            });

            if out.len() >= max_jobs {
                break;
            }
        }

        if out.len() >= max_jobs {
            break;
        }
        if (jobs_arr.len() as u32) < result_limit {
            break;
        }
        offset += result_limit as u64;
        if page_delay_ms > 0 {
            sleep(Duration::from_millis(page_delay_ms)).await;
        }
    }

    Ok(CrawlOutput::jobs(out))
}
