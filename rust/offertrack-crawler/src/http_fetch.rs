//! Shared HTTP GET/POST with per-host pacing and 429 / 5xx retries.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::header::HeaderMap;
use tokio::sync::Mutex;
use url::Url;

const MAX_ATTEMPTS: u32 = 5;

/// Minimum time between requests to the same host (across concurrent tasks).
pub struct HostThrottle {
    inner: Mutex<HashMap<String, Instant>>,
    min_gap: Duration,
}

impl HostThrottle {
    pub fn new(min_gap_ms: u64) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            min_gap: Duration::from_millis(min_gap_ms.max(1)),
        }
    }

    pub async fn pace(&self, host: &str) {
        let mut g = self.inner.lock().await;
        let now = Instant::now();
        if let Some(last) = g.get(host) {
            let elapsed = now.duration_since(*last);
            if elapsed < self.min_gap {
                let wait = self.min_gap - elapsed;
                drop(g);
                tokio::time::sleep(wait).await;
                g = self.inner.lock().await;
            }
        }
        g.insert(host.to_string(), Instant::now());
    }
}

pub(crate) fn host_of(url: &str) -> Option<String> {
    Url::parse(url).ok().and_then(|u| u.host_str().map(String::from))
}

async fn pace(url: &str, throttle: Option<&HostThrottle>) {
    if let Some(t) = throttle {
        if let Some(h) = host_of(url) {
            t.pace(&h).await;
        }
    }
}

async fn backoff_sleep(attempt: u32, resp: Option<&reqwest::Response>) {
    if let Some(r) = resp {
        if r.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            if let Some(sec) = r
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
            {
                tokio::time::sleep(Duration::from_secs(sec.max(1).min(120))).await;
                return;
            }
        }
        if r.status().is_server_error() {
            tokio::time::sleep(Duration::from_millis(400 * (1u64 << attempt.min(5)))).await;
            return;
        }
    }
    tokio::time::sleep(Duration::from_millis(250 * (1u64 << attempt.min(4)))).await;
}

/// Shared retry loop: send the request built by `build`, retry on 429 / 5xx.
async fn execute_with_retry(
    url: &str,
    build: impl Fn() -> reqwest::RequestBuilder,
) -> Result<reqwest::Response> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..MAX_ATTEMPTS {
        let resp = match build().send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(e.into());
                backoff_sleep(attempt, None).await;
                continue;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            backoff_sleep(attempt, Some(&resp)).await;
            last_err = Some(anyhow::anyhow!("HTTP {}", status));
            continue;
        }
        return resp
            .error_for_status()
            .with_context(|| format!("request {}", url));
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("retries exhausted for {}", url)))
}

/// GET URL → UTF-8 body. Retries on 429 and 5xx.
pub async fn get_text(
    client: &reqwest::Client,
    url: &str,
    throttle: Option<&HostThrottle>,
) -> Result<String> {
    pace(url, throttle).await;
    execute_with_retry(url, || client.get(url))
        .await?
        .text()
        .await
        .with_context(|| format!("read body {}", url))
}

/// GET → raw bytes (RSS/Atom; avoids UTF-8 lossy issues).
pub async fn get_bytes(
    client: &reqwest::Client,
    url: &str,
    throttle: Option<&HostThrottle>,
) -> Result<Vec<u8>> {
    pace(url, throttle).await;
    let bytes = execute_with_retry(url, || client.get(url))
        .await?
        .bytes()
        .await
        .with_context(|| format!("read body {}", url))?;
    Ok(bytes.to_vec())
}

/// POST JSON body. Retries on 429 and 5xx.
pub async fn post_json_text(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
    extra_headers: &[(String, String)],
    throttle: Option<&HostThrottle>,
) -> Result<String> {
    pace(url, throttle).await;
    execute_with_retry(url, || {
        let mut req = client
            .post(url)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .json(body);
        for (k, v) in extra_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req
    })
    .await?
    .text()
    .await
    .with_context(|| format!("read body {}", url))
}

/// GET with custom headers (e.g. Workday job detail).
pub async fn get_text_with_headers(
    client: &reqwest::Client,
    url: &str,
    headers: &HeaderMap,
    throttle: Option<&HostThrottle>,
) -> Result<String> {
    pace(url, throttle).await;
    execute_with_retry(url, || client.get(url).headers(headers.clone()))
        .await?
        .text()
        .await
        .with_context(|| format!("read body {}", url))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_of_parses_valid_url() {
        assert_eq!(host_of("https://example.com/path?q=1"), Some("example.com".into()));
        assert_eq!(host_of("http://api.greenhouse.io/v1/boards"), Some("api.greenhouse.io".into()));
    }

    #[test]
    fn host_of_returns_none_for_invalid() {
        assert_eq!(host_of("not-a-url"), None);
        assert_eq!(host_of(""), None);
    }

    #[test]
    fn host_throttle_new_clamps_zero_gap() {
        // min_gap_ms=0 should be clamped to 1ms (not panic).
        let t = HostThrottle::new(0);
        assert_eq!(t.min_gap, Duration::from_millis(1));
    }
}
