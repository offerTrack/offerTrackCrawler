use anyhow::{Context, Result};
use scraper::{Html, Selector};
use serde_json::{json, Value};
use url::Url;

use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

fn absolute(base: &str, href: &str) -> Option<String> {
    let b = Url::parse(base).ok()?;
    b.join(href).ok().map(|u| u.to_string())
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let list_url = source
        .get("icims_jobs_url")
        .or_else(|| source.get("url"))
        .or_else(|| source.get("career_url"))
        .and_then(|v| v.as_str())
        .context("icims: set icims_jobs_url/url/career_url")?;
    let company = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or("iCIMS")
        .to_string();

    let html = http_fetch::get_text(client, list_url, Some(throttle.as_ref())).await?;
    let doc = Html::parse_document(&html);
    let sel_a = Selector::parse("a[href]").map_err(|e| anyhow::anyhow!("icims selector: {e}"))?;
    let source_tag = format!("icims:{company}");

    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for a in doc.select(&sel_a) {
        let Some(href) = a.value().attr("href") else { continue };
        if !href.contains("/jobs/") {
            continue;
        }
        let Some(url) = absolute(list_url, href) else { continue };
        if !seen.insert(url.clone()) {
            continue;
        }
        let title = a.text().collect::<Vec<_>>().join(" ").trim().to_string();
        if title.is_empty() {
            continue;
        }
        out.push(JobPosting {
            title,
            company: company.clone(),
            url,
            location: None,
            description: None,
            posted_date: None,
            source: Some(source_tag.clone()),
            job_id: String::new(),
            raw: json!({ "href": href }),
        });
    }

    Ok(CrawlOutput {
        jobs: out,
        detail: Some(json!({ "list_url": list_url })),
    })
}
