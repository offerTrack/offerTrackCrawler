use anyhow::{Context, Result};
use serde_json::Value;

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let company = source
        .get("company")
        .and_then(|v| v.as_str())
        .context("lever: missing company")?;
    let display = source
        .get("display_name")
        .and_then(|v| v.as_str())
        .unwrap_or(company);
    let default_url = format!("https://api.lever.co/v0/postings/{company}?mode=json");
    let api_url = source
        .get("api_url")
        .and_then(|v| v.as_str())
        .unwrap_or(&default_url);

    let text = http_fetch::get_text(client, api_url, Some(throttle.as_ref())).await?;
    let arr: Vec<Value> = serde_json::from_str(&text)?;
    let source_tag = format!("lever:{company}");
    let mut out = Vec::new();

    for item in arr {
        let title = item.get("text").and_then(|x| x.as_str()).unwrap_or("").trim();
        let url = item
            .get("hostedUrl")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim();
        if title.is_empty() || url.is_empty() {
            continue;
        }

        let loc = item
            .get("categories")
            .and_then(|c| c.get("location"))
            .and_then(|x| x.as_str())
            .map(String::from);

        let desc = item
            .get("descriptionPlain")
            .or_else(|| item.get("description"))
            .and_then(|x| x.as_str())
            .map(String::from);

        let posted = parse_date(item.get("createdAt").and_then(|x| x.as_str()));

        out.push(JobPosting {
            title: title.to_string(),
            company: display.to_string(),
            url: url.to_string(),
            location: loc,
            description: desc,
            posted_date: posted,
            source: Some(source_tag.clone()),
            job_id: String::new(),
            raw: item,
        });
    }

    Ok(CrawlOutput::jobs(out))
}
