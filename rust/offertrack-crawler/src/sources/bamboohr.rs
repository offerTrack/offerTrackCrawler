use anyhow::{Context, Result};
use serde_json::{json, Value};

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

fn jobs_url(source: &Value) -> Result<String> {
    if let Some(base) = source
        .get("bamboohr_careers_base")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
    {
        return Ok(format!("{base}/list?output=json"));
    }
    if let Some(sub) = source
        .get("bamboohr_subdomain")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Ok(format!("https://{}.bamboohr.com/careers/list?output=json", sub));
    }
    anyhow::bail!("bamboohr: set bamboohr_careers_base or bamboohr_subdomain")
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let company = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or("BambooHR")
        .to_string();
    let api_url = jobs_url(source)?;
    let text = http_fetch::get_text(client, &api_url, Some(throttle.as_ref())).await?;
    let v: Value = serde_json::from_str(&text).context("bamboohr: JSON parse")?;
    let jobs = v
        .get("jobs")
        .or_else(|| v.get("openings"))
        .and_then(|x| x.as_array())
        .context("bamboohr: missing jobs/openings array")?;

    let source_tag = format!(
        "bamboohr:{}",
        source
            .get("bamboohr_subdomain")
            .or_else(|| source.get("bamboohr_careers_base"))
            .and_then(|x| x.as_str())
            .unwrap_or("board")
    );
    let mut out = Vec::new();
    for item in jobs {
        let title = item
            .get("jobOpeningName")
            .or_else(|| item.get("title"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim();
        let url = item
            .get("jobOpeningUrl")
            .or_else(|| item.get("url"))
            .or_else(|| item.get("applyUrl"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim();
        if title.is_empty() || url.is_empty() {
            continue;
        }
        let loc = item
            .get("location")
            .or_else(|| item.get("departmentLocation"))
            .and_then(|x| x.as_str())
            .map(String::from);
        let posted = parse_date(
            item.get("postedDate")
                .or_else(|| item.get("createdDate"))
                .and_then(|x| x.as_str()),
        );
        out.push(JobPosting {
            title: title.to_string(),
            company: company.clone(),
            url: url.to_string(),
            location: loc,
            description: None,
            posted_date: posted,
            source: Some(source_tag.clone()),
            job_id: String::new(),
            raw: item.clone(),
        });
    }

    Ok(CrawlOutput {
        jobs: out,
        detail: Some(json!({ "api_url": api_url })),
    })
}
