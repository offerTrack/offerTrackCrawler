//! HTML `sites` crawl: Schema.org JSON-LD JobPosting + BFS link discovery (Python `WebCrawler` parity).

mod schema_org;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use robots_txt::{matcher::SimpleMatcher, parts::Robots};
use serde_json::Value;
use tokio::sync::Mutex;
use url::Url;

pub use schema_org::extract_jobs_from_html;

use offertrack_crawler::job::JobPosting;

#[derive(Clone, Debug)]
pub struct HtmlCrawlSettings {
    pub max_concurrent_requests: usize,
    pub delay_between_requests: f64,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub user_agent: String,
    pub respect_robots_txt: bool,
    pub max_pages_per_site: usize,
}

impl Default for HtmlCrawlSettings {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 5,
            delay_between_requests: 1.0,
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            user_agent: "RAG-Platform-Crawler/1.0".to_string(),
            respect_robots_txt: true,
            max_pages_per_site: 100,
        }
    }
}

impl HtmlCrawlSettings {
    pub fn from_config(cfg: &Value) -> Self {
        let mut s = Self::default();
        if let Some(v) = cfg.get("max_concurrent_requests").and_then(|x| x.as_u64()) {
            s.max_concurrent_requests = v as usize;
        }
        if let Some(v) = cfg.get("delay_between_requests").and_then(|x| x.as_f64()) {
            s.delay_between_requests = v;
        }
        if let Some(v) = cfg.get("max_pages_per_site").and_then(|x| x.as_u64()) {
            s.max_pages_per_site = v as usize;
        }
        if let Some(v) = cfg.get("respect_robots_txt").and_then(|x| x.as_bool()) {
            s.respect_robots_txt = v;
        }
        s
    }
}

struct RobotsTxtCache {
    inner: Mutex<HashMap<String, Option<String>>>,
}

impl RobotsTxtCache {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    async fn can_fetch(
        &self,
        client: &reqwest::Client,
        user_agent: &str,
        url: &str,
        respect: bool,
    ) -> bool {
        if !respect {
            return true;
        }
        let Ok(parsed) = Url::parse(url) else {
            return true;
        };
        let Some(domain) = parsed.host_str().map(String::from) else {
            return true;
        };
        let path = parsed.path();

        let body = {
            let mut g = self.inner.lock().await;
            if !g.contains_key(&domain) {
                let robots_url = format!("{}://{}/robots.txt", parsed.scheme(), domain);
                let fetched = match client.get(&robots_url).timeout(Duration::from_secs(10)).send().await {
                    Ok(r) if r.status() == 200 => r.text().await.ok(),
                    _ => None,
                };
                g.insert(domain.clone(), fetched);
            }
            g.get(&domain).cloned().flatten()
        };

        let Some(text) = body else {
            return true;
        };
        let robots = Robots::from_str_lossy(&text);
        let section = robots.choose_section(user_agent);
        let matcher = SimpleMatcher::new(&section.rules);
        matcher.check_path(path)
    }
}

async fn rate_wait(domain: &str, delay_sec: f64, last_by_domain: &Mutex<HashMap<String, Instant>>) {
    if delay_sec <= 0.0 {
        return;
    }
    let mut g = last_by_domain.lock().await;
    let now = Instant::now();
    if let Some(prev) = g.get(domain) {
        let elapsed = now.duration_since(*prev).as_secs_f64();
        if elapsed < delay_sec {
            let wait = delay_sec - elapsed;
            drop(g);
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            g = last_by_domain.lock().await;
        }
    }
    g.insert(domain.to_string(), Instant::now());
}

async fn fetch_page(
    client: &reqwest::Client,
    url: &str,
    settings: &HtmlCrawlSettings,
) -> Option<String> {
    for attempt in 0..settings.max_retries {
        match client
            .get(url)
            .timeout(settings.request_timeout)
            .send()
            .await
        {
            Ok(resp) if resp.status() == 200 => return resp.text().await.ok(),
            _ => {
                if attempt + 1 < settings.max_retries {
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
                }
            }
        }
    }
    None
}

async fn crawl_one_site(
    client: &reqwest::Client,
    settings: &HtmlCrawlSettings,
    robots: &RobotsTxtCache,
    rate_state: &Mutex<HashMap<String, Instant>>,
    domain: &str,
    start_urls: &[String],
    job_link_href_contains: &[String],
) -> Vec<JobPosting> {
    let mut jobs = Vec::new();
    let mut urls: VecDeque<String> = start_urls.iter().cloned().collect();
    let mut crawled: HashSet<String> = HashSet::new();
    let mut crawled_count = 0usize;

    while let Some(url) = urls.pop_front() {
        if crawled_count >= settings.max_pages_per_site {
            break;
        }
        if crawled.contains(&url) {
            continue;
        }
        if !robots
            .can_fetch(client, &settings.user_agent, &url, settings.respect_robots_txt)
            .await
        {
            continue;
        }
        rate_wait(domain, settings.delay_between_requests, rate_state).await;
        let Some(html) = fetch_page(client, &url, settings).await else {
            continue;
        };
        crawled.insert(url.clone());
        crawled_count += 1;

        let page_jobs = extract_jobs_from_html(&html, &url, domain);
        jobs.extend(page_jobs);

        for u in schema_org::extract_job_urls(&html, &url, job_link_href_contains) {
            if !crawled.contains(&u) {
                urls.push_back(u);
            }
        }
    }

    jobs
}

/// Crawl all enabled `sites` entries that use `GenericSchemaOrgExtractor`.
pub async fn crawl_html_sites(
    client: &reqwest::Client,
    settings: &HtmlCrawlSettings,
    sites: &[Value],
) -> Vec<JobPosting> {
    let robots = Arc::new(RobotsTxtCache::new());
    let rate = Arc::new(Mutex::new(HashMap::<String, Instant>::new()));

    let mut handles = Vec::new();
    for site in sites {
        if !site.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false) {
            continue;
        }
        let ext = site
            .get("extractor")
            .and_then(|v| v.as_str())
            .unwrap_or("GenericSchemaOrgExtractor");
        if ext != "GenericSchemaOrgExtractor" {
            eprintln!("[WARN] html site skipped: unknown extractor {ext}");
            continue;
        }
        let Some(domain) = site.get("domain").and_then(|v| v.as_str()) else {
            eprintln!("[WARN] html site missing domain");
            continue;
        };
        let start_urls: Vec<String> = site
            .get("start_urls")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if start_urls.is_empty() {
            continue;
        }

        let job_link_href_contains: Vec<String> = site
            .get("job_link_href_contains")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let client = client.clone();
        let settings = settings.clone();
        let robots = robots.clone();
        let rate = rate.clone();
        let domain = domain.to_string();
        handles.push(tokio::spawn(async move {
            crawl_one_site(
                &client,
                &settings,
                &robots,
                &rate,
                &domain,
                &start_urls,
                &job_link_href_contains,
            )
            .await
        }));
    }

    let mut all = Vec::new();
    for h in handles {
        if let Ok(v) = h.await {
            all.extend(v);
        }
    }
    all
}
