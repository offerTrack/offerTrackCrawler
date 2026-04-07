use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, AUTHORIZATION};
use serde_json::{json, Value};

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let subdomain = source
        .get("workable_subdomain")
        .or_else(|| source.get("company"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .context("workable: set workable_subdomain")?;
    let token = source
        .get("workable_api_token")
        .or_else(|| source.get("api_token"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            source
                .get("workable_api_token_env")
                .and_then(|v| v.as_str())
                .and_then(|k| std::env::var(k).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .context("workable: set workable_api_token or workable_api_token_env (SPI v3 bearer token)")?;

    let page_limit = source
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(100)
        .clamp(1, 200) as usize;
    let max_pages = source
        .get("max_pages")
        .and_then(|v| v.as_u64())
        .unwrap_or(30)
        .max(1) as usize;

    let company = source
        .get("display_name")
        .or_else(|| source.get("company"))
        .and_then(|v| v.as_str())
        .unwrap_or(subdomain)
        .to_string();
    let source_tag = format!("workable:{subdomain}");

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        format!("Bearer {}", token)
            .parse()
            .map_err(|e| anyhow::anyhow!("workable auth header: {e}"))?,
    );

    let mut out = Vec::new();
    let mut page = 1usize;
    for _ in 0..max_pages {
        let url = format!(
            "https://{}.workable.com/spi/v3/jobs?state=published&limit={}&page={}",
            subdomain, page_limit, page
        );
        let text = http_fetch::get_text_with_headers(client, &url, &headers, Some(throttle.as_ref())).await?;
        let v: Value = serde_json::from_str(&text).context("workable: JSON parse")?;
        let rows: Vec<Value> = if let Some(arr) = v.get("jobs").and_then(|x| x.as_array()) {
            arr.clone()
        } else if let Some(arr) = v.get("results").and_then(|x| x.as_array()) {
            arr.clone()
        } else if let Some(arr) = v.as_array() {
            arr.clone()
        } else {
            Vec::new()
        };
        if rows.is_empty() {
            break;
        }
        for item in rows {
            let title = item
                .get("title")
                .or_else(|| item.get("shortcode"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim();
            let url = item
                .get("url")
                .or_else(|| item.get("application_url"))
                .or_else(|| item.get("apply_url"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim();
            if title.is_empty() || url.is_empty() {
                continue;
            }
            let loc = item
                .get("location")
                .or_else(|| item.get("location_name"))
                .and_then(|x| x.as_str())
                .map(String::from);
            let desc = item
                .get("description")
                .or_else(|| item.get("full_description"))
                .or_else(|| item.get("short_description"))
                .and_then(|x| x.as_str())
                .map(String::from);
            let posted = parse_date(
                item.get("published")
                    .or_else(|| item.get("created_at"))
                    .or_else(|| item.get("updated_at"))
                    .and_then(|x| x.as_str()),
            );
            out.push(JobPosting {
                title: title.to_string(),
                company: company.clone(),
                url: url.to_string(),
                location: loc,
                description: desc,
                posted_date: posted,
                source: Some(source_tag.clone()),
                job_id: String::new(),
                raw: item,
            });
        }
        if out.len() < page * page_limit {
            break;
        }
        page += 1;
    }

    Ok(CrawlOutput {
        jobs: out,
        detail: Some(json!({ "subdomain": subdomain })),
    })
}
