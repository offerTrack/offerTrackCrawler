//! Recruitee public careers API — `GET {base}/api/offers/` (see Recruitee docs).
use anyhow::{Context, Result};
use serde_json::{json, Value};

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

fn offers_api_url(source: &Value) -> Result<String> {
    if let Some(base) = source
        .get("recruitee_careers_base")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
    {
        return Ok(format!("{base}/api/offers/"));
    }
    if let Some(sub) = source
        .get("recruitee_subdomain")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Ok(format!(
            "https://{}.recruitee.com/api/offers/",
            sub.trim_end_matches(".recruitee.com")
        ));
    }
    anyhow::bail!("recruitee: set recruitee_careers_base (e.g. https://careers.example.com) or recruitee_subdomain")
}

fn location_string(locations: &[Value]) -> Option<String> {
    if locations.is_empty() {
        return None;
    }
    let mut parts: Vec<String> = locations
        .iter()
        .filter_map(|l| {
            l.get("name")
                .or_else(|| l.get("city"))
                .and_then(|x| x.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .collect();
    parts.sort();
    parts.dedup();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" | "))
    }
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let company_default = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or("Recruitee")
        .to_string();
    let api_url = offers_api_url(source)?;
    let text = http_fetch::get_text(client, &api_url, Some(throttle.as_ref())).await?;
    let v: Value = serde_json::from_str(&text).context("recruitee: JSON parse")?;
    let offers = v
        .get("offers")
        .and_then(|x| x.as_array())
        .context("recruitee: missing offers array")?;

    let source_tag = format!(
        "recruitee:{}",
        source
            .get("recruitee_subdomain")
            .or_else(|| source.get("recruitee_careers_base"))
            .and_then(|x| x.as_str())
            .unwrap_or("board")
    );

    let mut out = Vec::new();
    for item in offers {
        let status = item.get("status").and_then(|x| x.as_str()).unwrap_or("");
        if status != "published" && !status.is_empty() {
            continue;
        }
        let title = item.get("title").and_then(|x| x.as_str()).unwrap_or("").trim();
        let url = item
            .get("careers_url")
            .or_else(|| item.get("careers_apply_url"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim();
        if title.is_empty() || url.is_empty() {
            continue;
        }
        let company = item
            .get("company_name")
            .and_then(|x| x.as_str())
            .unwrap_or(&company_default)
            .to_string();
        let loc = item
            .get("locations")
            .and_then(|x| x.as_array())
            .map(|a| a.as_slice())
            .and_then(location_string);
        let desc = [
            item.get("description").and_then(|x| x.as_str()),
            item.get("requirements").and_then(|x| x.as_str()),
            item.get("highlight").and_then(|x| x.as_str()),
        ]
        .into_iter()
        .flatten()
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
        let description = if desc.is_empty() {
            None
        } else {
            Some(desc)
        };
        let posted = parse_date(
            item.get("published_at")
                .or_else(|| item.get("publishedAt"))
                .or_else(|| item.get("created_at"))
                .and_then(|x| x.as_str()),
        );

        out.push(JobPosting {
            title: title.to_string(),
            company,
            url: url.to_string(),
            location: loc,
            description,
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
