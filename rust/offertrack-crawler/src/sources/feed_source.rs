use anyhow::{Context, Result};
use feed_rs::model::Entry;
use serde_json::{json, Value};
use url::Url;

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;
use crate::html;

fn entry_link(e: &Entry) -> String {
    e.links
        .first()
        .map(|l| l.href.clone())
        .unwrap_or_default()
}

fn entry_title(e: &Entry) -> String {
    e.title
        .as_ref()
        .map(|t| t.content.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Untitled job".into())
}

fn entry_published(e: &Entry) -> Option<String> {
    e.published
        .or(e.updated)
        .map(|dt| dt.to_rfc3339())
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let feed_url = source
        .get("url")
        .and_then(|v| v.as_str())
        .context("rss/atom: missing url")?;
    let typ = source.get("type").and_then(|v| v.as_str()).unwrap_or("rss");

    let default_company = source
        .get("company")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            Url::parse(feed_url)
                .ok()
                .and_then(|u| u.host_str().map(String::from))
        })
        .unwrap_or_else(|| "feed".into());

    let default_location = source
        .get("default_location")
        .and_then(|v| v.as_str())
        .map(String::from);

    let bytes = http_fetch::get_bytes(client, feed_url, Some(throttle.as_ref())).await?;

    let feed = feed_rs::parser::parse(&bytes[..]).map_err(|e| anyhow::anyhow!("feed parse: {e}"))?;

    let source_tag = format!("{typ}:{feed_url}");
    let mut out = Vec::new();

    for e in feed.entries {
        let title = entry_title(&e);
        let link = entry_link(&e);
        let url = if link.is_empty() {
            feed_url.to_string()
        } else {
            link
        };
        if title.is_empty() || url.is_empty() {
            continue;
        }

        let desc = e.summary.as_ref().map(|s| s.content.clone());
        let posted = parse_date(entry_published(&e).as_deref());

        out.push(JobPosting {
            title,
            company: default_company.clone(),
            url,
            location: default_location.clone(),
            description: desc,
            posted_date: posted,
            source: Some(source_tag.clone()),
            job_id: String::new(),
            raw: serde_json::json!({ "feed": typ }),
        });
    }

    let fetch_entry_detail = source
        .get("fetch_entry_detail_page")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let max_entry_detail_fetches = source
        .get("max_entry_detail_fetches")
        .and_then(|v| v.as_u64())
        .unwrap_or(30) as usize;
    let entry_detail_min_description_chars = source
        .get("entry_detail_min_description_chars")
        .and_then(|v| v.as_u64())
        .unwrap_or(120) as usize;

    let mut entry_detail_fetch_errors = 0u32;
    let mut entry_detail_enriched = 0u32;

    if fetch_entry_detail {
        for job in out.iter_mut().take(max_entry_detail_fetches) {
            let cur_len = job.description.as_deref().map(|s| s.chars().count()).unwrap_or(0);
            if cur_len >= entry_detail_min_description_chars {
                continue;
            }
            match http_fetch::get_text(client, &job.url, Some(throttle.as_ref())).await {
                Ok(html) => {
                    let parsed = html::extract_jobs_from_html(&html, &job.url, "feed_detail");
                    let best = parsed.iter().max_by_key(|j| {
                        j.description.as_deref().map(|s| s.chars().count()).unwrap_or(0)
                    });
                    if let Some(j0) = best {
                        let nlen = j0.description.as_deref().map(|s| s.chars().count()).unwrap_or(0);
                        if nlen > cur_len {
                            job.description = j0.description.clone();
                            entry_detail_enriched += 1;
                        }
                    }
                }
                Err(_) => entry_detail_fetch_errors += 1,
            }
        }
    }

    let detail = if fetch_entry_detail {
        Some(json!({
            "fetch_entry_detail_page": true,
            "entry_detail_enriched": entry_detail_enriched,
            "entry_detail_fetch_errors": entry_detail_fetch_errors,
        }))
    } else {
        None
    };

    Ok(CrawlOutput { jobs: out, detail })
}
