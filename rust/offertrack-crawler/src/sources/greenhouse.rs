use anyhow::{Context, Result};
use serde_json::Value;

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

fn hosted_greenhouse_url(board: &str, item: &Value, fallback: &str) -> String {
    if let Some(id) = item.get("id").and_then(|x| x.as_i64()) {
        return format!("https://job-boards.greenhouse.io/{board}/jobs/{id}");
    }
    fallback.to_string()
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let board = source
        .get("board")
        .and_then(|v| v.as_str())
        .context("greenhouse: missing board")?;
    let company = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or(board);
    let default_url = format!("https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true");
    let api_url = source
        .get("api_url")
        .and_then(|v| v.as_str())
        .unwrap_or(&default_url);

    let text = http_fetch::get_text(client, api_url, Some(throttle.as_ref())).await?;
    let v: Value = serde_json::from_str(&text)?;
    let jobs_arr = v
        .get("jobs")
        .and_then(|x| x.as_array())
        .context("greenhouse: jobs array")?;

    let source_tag = format!("greenhouse:{board}");
    let mut out = Vec::new();

    for item in jobs_arr {
        let title = item.get("title").and_then(|x| x.as_str()).unwrap_or("").trim();
        let url = item
            .get("absolute_url")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim();
        if title.is_empty() || url.is_empty() {
            continue;
        }
        let url = hosted_greenhouse_url(board, item, url);

        let loc = item.get("location").and_then(|l| {
            if let Some(name) = l.get("name").and_then(|n| n.as_str()) {
                return Some(name.to_string());
            }
            None
        });

        let desc = item
            .get("content")
            .and_then(|x| x.as_str())
            .map(String::from);

        let posted = parse_date(
            item.get("updated_at")
                .or_else(|| item.get("created_at"))
                .and_then(|x| x.as_str()),
        );

        out.push(JobPosting {
            title: title.to_string(),
            company: company.to_string(),
            url,
            location: loc,
            description: desc,
            posted_date: posted,
            source: Some(source_tag.clone()),
            job_id: String::new(),
            raw: item.clone(),
        });
    }

    Ok(CrawlOutput::jobs(out))
}
