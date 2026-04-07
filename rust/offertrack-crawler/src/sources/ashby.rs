use anyhow::{Context, Result};
use serde_json::Value;

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

/// Public Ashby Job Posting API (no auth for listed boards).
pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let board = source
        .get("board")
        .and_then(|v| v.as_str())
        .context("ashby: missing board (job board slug)")?;
    let company = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or(board);
    let default_url = format!("https://api.ashbyhq.com/posting-api/job-board/{board}");
    let api_url = source
        .get("api_url")
        .and_then(|v| v.as_str())
        .unwrap_or(&default_url);

    let text = http_fetch::get_text(client, api_url, Some(throttle.as_ref())).await?;
    let v: Value = serde_json::from_str(&text)?;
    let jobs_arr = v
        .get("jobs")
        .and_then(|x| x.as_array())
        .context("ashby: jobs array")?;

    let source_tag = format!("ashby:{board}");
    let mut out = Vec::new();

    for item in jobs_arr {
        if let Some(false) = item.get("isListed").and_then(|x| x.as_bool()) {
            continue;
        }
        let title = item.get("title").and_then(|x| x.as_str()).unwrap_or("").trim();
        let url = item
            .get("jobUrl")
            .or_else(|| item.get("applyUrl"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim();
        if title.is_empty() || url.is_empty() {
            continue;
        }

        let location = item
            .get("location")
            .and_then(|x| x.as_str())
            .map(String::from)
            .filter(|s| !s.trim().is_empty());

        let description = item
            .get("descriptionHtml")
            .or_else(|| item.get("descriptionPlain"))
            .and_then(|x| x.as_str())
            .map(String::from);

        let posted = item
            .get("publishedAt")
            .and_then(|x| x.as_str())
            .and_then(|s| parse_date(Some(s)));

        out.push(JobPosting {
            title: title.to_string(),
            company: company.to_string(),
            url: url.to_string(),
            location,
            description,
            posted_date: posted,
            source: Some(source_tag.clone()),
            job_id: String::new(),
            raw: serde_json::json!({ "ashby": item }),
        });
    }

    Ok(CrawlOutput::jobs(out))
}
