//! SmartRecruiters Customer API — public job postings list (no auth for many career sites).
//! `GET https://api.smartrecruiters.com/v1/companies/{companyId}/postings`

use anyhow::{Context, Result};
use serde_json::{json, Value};

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

use super::CrawlOutput;
use crate::http_fetch;

fn location_string(loc: &Value) -> Option<String> {
    if let Some(s) = loc.get("fullLocation").and_then(|x| x.as_str()).map(str::trim) {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    let city = loc.get("city").and_then(|x| x.as_str()).unwrap_or("").trim();
    let region = loc.get("region").and_then(|x| x.as_str()).unwrap_or("").trim();
    let country = loc.get("country").and_then(|x| x.as_str()).unwrap_or("").trim();
    let mut parts: Vec<&str> = Vec::new();
    if !city.is_empty() {
        parts.push(city);
    }
    if !region.is_empty() {
        parts.push(region);
    }
    if !country.is_empty() {
        parts.push(country);
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

fn description_from_posting(p: &Value) -> Option<String> {
    let sections = p.get("jobAd")?.get("sections")?;
    let jd = sections.get("jobDescription")?;
    if let Some(t) = jd.get("text").and_then(|x| x.as_str()) {
        let t = t.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Some(t) = jd.as_str() {
        let t = t.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    None
}

fn posting_url(p: &Value) -> Option<String> {
    p.get("postingUrl")
        .or_else(|| p.get("ref"))
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// Parse one API response page (`content` array already extracted) — unit-testable.
pub fn postings_from_api_values(items: &[Value], company: &str, source_tag: &str) -> Vec<JobPosting> {
    let mut out = Vec::new();
    for p in items {
        let title = p.get("name").and_then(|x| x.as_str()).unwrap_or("").trim();
        let Some(url) = posting_url(p) else { continue };
        if title.is_empty() {
            continue;
        }
        let loc = p
            .get("location")
            .and_then(|l| location_string(l));
        let desc = description_from_posting(p);
        let posted = parse_date(
            p.get("releasedDate")
                .or_else(|| p.get("createdOn"))
                .and_then(|x| x.as_str()),
        );
        out.push(JobPosting {
            title: title.to_string(),
            company: company.to_string(),
            url,
            location: loc,
            description: desc,
            posted_date: posted,
            source: Some(source_tag.to_string()),
            job_id: String::new(),
            raw: p.clone(),
        });
    }
    out
}

pub async fn crawl(
    client: &reqwest::Client,
    source: &Value,
    throttle: &std::sync::Arc<http_fetch::HostThrottle>,
) -> Result<CrawlOutput> {
    let company_id = source
        .get("smartrecruiters_company_id")
        .or_else(|| source.get("board"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .context("smartrecruiters: set smartrecruiters_company_id (API company slug / id)")?;

    let company = source
        .get("company")
        .and_then(|v| v.as_str())
        .unwrap_or(company_id)
        .to_string();

    let page_size = source
        .get("page_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(100)
        .clamp(1, 100) as usize;

    let max_pages = source
        .get("max_pages")
        .and_then(|v| v.as_u64())
        .unwrap_or(50)
        .max(1) as usize;

    let source_tag = format!("smartrecruiters:{company_id}");
    let mut all = Vec::new();
    let mut pages_ok = 0u32;
    let mut last_total: Option<u64> = None;

    for page in 0..max_pages {
        let offset = (page as u64) * (page_size as u64);
        let url = format!(
            "https://api.smartrecruiters.com/v1/companies/{company_id}/postings?offset={offset}&limit={page_size}"
        );
        let text = http_fetch::get_text(client, &url, Some(throttle.as_ref())).await?;
        let v: Value = serde_json::from_str(&text).context("smartrecruiters: JSON parse")?;
        if let Some(t) = v.get("totalFound").and_then(|x| x.as_u64()) {
            last_total = Some(t);
        }
        let items = v
            .get("content")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            break;
        }
        pages_ok += 1;
        all.extend(postings_from_api_values(&items, &company, &source_tag));
        if items.len() < page_size {
            break;
        }
    }

    let detail = json!({
        "api_base": "https://api.smartrecruiters.com/v1",
        "company_id": company_id,
        "pages_fetched": pages_ok,
        "total_found_last_page": last_total,
    });

    Ok(CrawlOutput {
        jobs: all,
        detail: Some(detail),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sample_posting() {
        let p = json!({
            "name": "Engineer",
            "postingUrl": "https://jobs.smartrecruiters.com/Co/123",
            "releasedDate": "2024-06-01",
            "location": { "city": "Austin", "country": "US" },
            "jobAd": { "sections": { "jobDescription": { "text": "<p>Do things</p>" } } }
        });
        let jobs = postings_from_api_values(std::slice::from_ref(&p), "Co", "smartrecruiters:test");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].title, "Engineer");
        assert!(jobs[0].description.as_deref().unwrap().contains("Do things"));
        assert_eq!(jobs[0].location.as_deref(), Some("Austin, US"));
    }
}
