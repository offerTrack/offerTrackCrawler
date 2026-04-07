//! Generic Schema.org `JobPosting` JSON-LD + link discovery (aligned with Python `generic_schemaorg.py`).

use scraper::{Html, Selector};
use serde_json::Value;
use url::Url;

use offertrack_crawler::date_parse::parse_date;
use offertrack_crawler::job::JobPosting;

pub fn clean_text(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn make_absolute_url(base: &str, href: &str) -> Option<String> {
    let b = Url::parse(base).ok()?;
    b.join(href).ok().map(|u| u.into())
}

fn type_is_jobposting(t: Option<&Value>) -> bool {
    match t {
        None => false,
        Some(Value::String(s)) => s.to_lowercase() == "jobposting",
        Some(Value::Array(a)) => a.iter().any(|x| {
            x.as_str()
                .map(|s| s.to_lowercase() == "jobposting")
                .unwrap_or(false)
        }),
        _ => false,
    }
}

fn walk_json_for_jobpostings(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(map) => {
            let t = map.get("@type").or_else(|| map.get("type"));
            if type_is_jobposting(t) {
                out.push(Value::Object(map.clone()));
            }
            if let Some(Value::Array(graph)) = map.get("@graph") {
                for item in graph {
                    walk_json_for_jobpostings(item, out);
                }
            }
            // Do not walk `@graph` again via `map.values()` — that would duplicate nodes.
            for (k, val) in map.iter() {
                if k == "@graph" {
                    continue;
                }
                walk_json_for_jobpostings(val, out);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                walk_json_for_jobpostings(item, out);
            }
        }
        _ => {}
    }
}

fn schema_location(job_location: Option<&Value>) -> Option<String> {
    let jl = job_location?;
    match jl {
        Value::String(s) => Some(s.clone()),
        Value::Array(a) => a.first().and_then(|x| schema_location(Some(x))),
        Value::Object(m) => {
            let addr = m.get("address")?;
            match addr {
                Value::String(s) => Some(s.clone()),
                Value::Object(am) => {
                    let mut parts = vec![
                        am.get("addressLocality").and_then(|x| x.as_str()),
                        am.get("addressRegion").and_then(|x| x.as_str()),
                        am.get("addressCountry").and_then(|x| x.as_str()),
                    ];
                    parts.retain(|p| p.is_some());
                    if parts.is_empty() {
                        None
                    } else {
                        Some(
                            parts
                                .into_iter()
                                .flatten()
                                .map(|s| s.to_string())
                                .collect::<Vec<_>>()
                                .join(", "),
                        )
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn jobposting_from_schema(jp: &Value, page_url: &str, domain: &str) -> Option<JobPosting> {
    let obj = jp.as_object()?;
    let title = obj
        .get("title")
        .or_else(|| obj.get("name"))
        .and_then(|x| x.as_str())?;
    let hiring = obj.get("hiringOrganization");
    let company = hiring
        .and_then(|h| h.as_object())
        .and_then(|o| o.get("name"))
        .and_then(|x| x.as_str())
        .map(String::from)
        .unwrap_or_else(|| domain.to_string());

    let loc = schema_location(obj.get("jobLocation"));
    let desc = obj
        .get("description")
        .and_then(|x| x.as_str())
        .map(String::from);

    let posted = obj
        .get("datePosted")
        .and_then(|x| x.as_str())
        .and_then(|s| parse_date(Some(s)));

    let canonical = obj
        .get("url")
        .or_else(|| obj.get("sameAs"))
        .and_then(|x| x.as_str())
        .unwrap_or(page_url);

    Some(JobPosting {
        title: clean_text(title),
        company: clean_text(&company),
        url: canonical.to_string(),
        location: loc.map(|s| clean_text(&s)),
        description: desc.map(|s| clean_text(&s)),
        posted_date: posted,
        source: Some(domain.to_string()),
        job_id: String::new(),
        raw: serde_json::json!({ "schema_org": jp }),
    })
}

fn first_text(document: &Html, tags: &[&str]) -> Option<String> {
    for tag in tags {
        let sel = Selector::parse(tag).ok()?;
        if let Some(el) = document.select(&sel).next() {
            let t = el.text().collect::<Vec<_>>().join(" ");
            let t = clean_text(&t);
            if !t.is_empty() {
                return Some(t);
            }
        }
    }
    None
}

fn best_effort_company(document: &Html) -> Option<String> {
    let sel = Selector::parse(r#"meta[property="og:site_name"]"#).ok()?;
    document
        .select(&sel)
        .next()
        .and_then(|el| el.value().attr("content"))
        .map(|s| clean_text(s))
}

fn best_effort_location(document: &Html) -> Option<String> {
    for key in ["jobLocation", "location", "address"] {
        let sel = Selector::parse(&format!(r#"meta[name="{}"]"#, key)).ok()?;
        if let Some(el) = document.select(&sel).next() {
            if let Some(c) = el.value().attr("content") {
                let t = clean_text(c);
                if !t.is_empty() {
                    return Some(t);
                }
            }
        }
    }
    None
}

fn best_effort_description(document: &Html) -> Option<String> {
    let sel = Selector::parse(r#"meta[name="description"]"#).ok()?;
    document
        .select(&sel)
        .next()
        .and_then(|el| el.value().attr("content"))
        .map(|s| clean_text(s))
}

fn best_effort_posted(document: &Html) -> Option<chrono::NaiveDateTime> {
    for key in ["datePosted", "article:published_time", "og:updated_time"] {
        for sel_str in [
            format!(r#"meta[property="{}"]"#, key),
            format!(r#"meta[name="{}"]"#, key),
        ] {
            let sel = Selector::parse(&sel_str).ok()?;
            if let Some(el) = document.select(&sel).next() {
                if let Some(c) = el.value().attr("content") {
                    if let Some(d) = parse_date(Some(c)) {
                        return Some(d);
                    }
                }
            }
        }
    }
    None
}

/// Extract jobs from one HTML page (`domain` is config `sites[].domain`, used as `source`).
pub fn extract_jobs_from_html(content: &str, page_url: &str, domain: &str) -> Vec<JobPosting> {
    let document = Html::parse_document(content);
    let sel_ld = match Selector::parse(r#"script[type="application/ld+json"]"#) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    let mut jobs = Vec::new();
    for el in document.select(&sel_ld) {
        let text: String = el.text().collect();
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            continue;
        };
        let mut postings = Vec::new();
        walk_json_for_jobpostings(&v, &mut postings);
        for jp in postings {
            if let Some(j) = jobposting_from_schema(&jp, page_url, domain) {
                jobs.push(j);
            }
        }
    }

    if jobs.is_empty() {
        if let Some(title) = first_text(&document, &["h1", "title"]) {
            let host = Url::parse(page_url)
                .ok()
                .and_then(|u| u.host_str().map(String::from))
                .unwrap_or_else(|| domain.to_string());
            jobs.push(JobPosting {
                title,
                company: best_effort_company(&document).unwrap_or(host),
                url: page_url.to_string(),
                location: best_effort_location(&document),
                description: best_effort_description(&document),
                posted_date: best_effort_posted(&document),
                source: Some(domain.to_string()),
                job_id: String::new(),
                raw: serde_json::json!({ "_fallback": true }),
            });
        }
    }

    jobs
}

const JOB_PATH_KEYS: &[&str] = &[
    "/job", "/jobs", "/career", "/careers", "/position", "/positions",
];

/// Discover job detail links on same host. `href_contains` adds extra case-insensitive substring
/// matches on the full URL (e.g. `"vacature"`, `"opening"`).
pub fn extract_job_urls(content: &str, page_url: &str, href_contains: &[String]) -> Vec<String> {
    let Ok(base_host) = Url::parse(page_url).map(|u| u.host_str().unwrap_or("").to_string()) else {
        return vec![];
    };
    if base_host.is_empty() {
        return vec![];
    }
    let hints: Vec<String> = href_contains.iter().map(|s| s.to_lowercase()).collect();
    let document = Html::parse_document(content);
    let Ok(sel_a) = Selector::parse("a[href]") else {
        return vec![];
    };

    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for el in document.select(&sel_a) {
        let Some(href) = el.value().attr("href") else {
            continue;
        };
        let Some(abs) = make_absolute_url(page_url, href) else {
            continue;
        };
        let Ok(u) = Url::parse(&abs) else {
            continue;
        };
        if u.scheme() != "http" && u.scheme() != "https" {
            continue;
        }
        if u.host_str() != Some(base_host.as_str()) {
            continue;
        }
        let path = u.path().to_lowercase();
        let abs_low = abs.to_lowercase();
        let path_hit = JOB_PATH_KEYS.iter().any(|k| path.contains(k));
        let hint_hit = hints.iter().any(|h| !h.is_empty() && abs_low.contains(h));
        if path_hit || hint_hit {
            if seen.insert(abs.clone()) {
                out.push(abs);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_text_collapses_whitespace() {
        assert_eq!(clean_text("  a \n\t b  "), "a b");
    }

    #[test]
    fn make_absolute_resolves_relative() {
        assert_eq!(
            make_absolute_url("https://example.com/careers/", "/jobs/1"),
            Some("https://example.com/jobs/1".to_string())
        );
    }

    #[test]
    fn extract_job_urls_same_host_and_path_heuristics() {
        let html = r#"<a href="/jobs/123">a</a><a href="https://evil.com/jobs/1">b</a><a href="/about">c</a>"#;
        let urls = extract_job_urls(html, "https://example.com/page", &[]);
        assert_eq!(urls, vec!["https://example.com/jobs/123"]);
    }

    #[test]
    fn extract_job_urls_extra_hint_matches() {
        let html = r#"<a href="/careers/vacature-12">x</a><a href="/about">y</a>"#;
        let hints = vec!["vacature".to_string()];
        let urls = extract_job_urls(html, "https://example.com/", &hints);
        assert_eq!(urls, vec!["https://example.com/careers/vacature-12"]);
    }

    #[test]
    fn extract_jobs_from_json_ld_jobposting() {
        let html = r#"<!DOCTYPE html><html><head>
        <script type="application/ld+json">
        {"@type":"JobPosting","title":"  Staff Eng  ","hiringOrganization":{"name":"Acme Corp"},"url":"https://example.com/jobs/1","description":"Build things"}
        </script></head><body></body></html>"#;
        let jobs = extract_jobs_from_html(html, "https://example.com/list", "example.com");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].title, "Staff Eng");
        assert_eq!(jobs[0].company, "Acme Corp");
        assert_eq!(jobs[0].url, "https://example.com/jobs/1");
        assert_eq!(
            jobs[0].description.as_deref(),
            Some("Build things")
        );
        assert_eq!(jobs[0].source.as_deref(), Some("example.com"));
    }

    #[test]
    fn extract_jobs_from_at_graph_jobposting() {
        let html = r#"<!DOCTYPE html><html><head>
        <script type="application/ld+json">
        {"@context":"https://schema.org","@graph":[{"@type":"Organization","name":"X"},{"@type":"JobPosting","title":"Designer","hiringOrganization":{"name":"Design Co"}}]}
        </script></head><body></body></html>"#;
        let jobs = extract_jobs_from_html(html, "https://hire.example.com/", "hire.example.com");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].title, "Designer");
        assert_eq!(jobs[0].company, "Design Co");
    }

    #[test]
    fn fallback_uses_h1_when_no_json_ld() {
        let html = r#"<!DOCTYPE html><html><head><title>Ignored</title></head><body><h1>  Open Role  </h1></body></html>"#;
        let jobs = extract_jobs_from_html(html, "https://jobs.example.com/p/1", "jobs.example.com");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].title, "Open Role");
        assert!(jobs[0].raw.get("_fallback").and_then(|v| v.as_bool()).unwrap_or(false));
    }
}
