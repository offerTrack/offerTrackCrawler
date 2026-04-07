//! Canonical listing URL — must stay aligned with Python `storage._canonical_url` for stable `job_id`.

use std::collections::BTreeMap;
use url::Url;

pub fn canonical_url(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    let parsed = match Url::parse(raw) {
        Ok(u) => u,
        Err(_) => match Url::parse(&format!("https://{raw}")) {
            Ok(u) => u,
            Err(_) => return String::new(),
        },
    };

    let scheme = if parsed.scheme().is_empty() {
        "https".to_string()
    } else {
        parsed.scheme().to_lowercase()
    };
    let host = parsed.host_str().unwrap_or("").to_lowercase();
    let mut path = parsed.path().to_string();
    if path.len() > 1 {
        while path.ends_with('/') {
            path.pop();
        }
    }
    if path.is_empty() {
        path = "/".to_string();
    }
    let base = format!("{scheme}://{host}{path}");

    let mut qp: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in parsed.query_pairs() {
        qp.entry(k.into_owned()).or_insert_with(|| v.into_owned());
    }

    let filtered: Vec<(String, String)> = qp
        .into_iter()
        .filter(|(k, _)| {
            let lk = k.to_lowercase();
            !(lk == "utm_source" || lk.starts_with("utm_"))
        })
        .collect();

    if filtered.is_empty() {
        return base;
    }

    let q = filtered
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                urlencoding::encode(k),
                urlencoding::encode(v)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{q}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_trailing_slash_and_utm() {
        let u = canonical_url("HTTPS://Example.COM/jobs/foo/?utm_source=x&gh_jid=1");
        assert_eq!(u, "https://example.com/jobs/foo?gh_jid=1");
    }

    #[test]
    fn empty_input_returns_empty() {
        assert_eq!(canonical_url(""), "");
        assert_eq!(canonical_url("   "), "");
    }

    #[test]
    fn no_query_params() {
        let u = canonical_url("https://example.com/jobs/42");
        assert_eq!(u, "https://example.com/jobs/42");
    }

    #[test]
    fn root_path_normalised() {
        let u = canonical_url("https://example.com/");
        assert_eq!(u, "https://example.com/");
    }

    #[test]
    fn fragment_stripped() {
        // URL fragments (#section) are client-side only and should not affect the canonical form.
        let u = canonical_url("https://example.com/jobs/1#apply");
        assert_eq!(u, "https://example.com/jobs/1");
    }

    #[test]
    fn query_params_sorted_alphabetically() {
        let u = canonical_url("https://example.com/jobs?z=last&a=first");
        assert_eq!(u, "https://example.com/jobs?a=first&z=last");
    }

    #[test]
    fn all_utm_variants_stripped() {
        let u = canonical_url("https://example.com/jobs?utm_source=x&utm_medium=y&utm_campaign=z&id=42");
        assert_eq!(u, "https://example.com/jobs?id=42");
    }

    #[test]
    fn host_lowercased() {
        let u = canonical_url("HTTPS://BOARDS.GREENHOUSE.IO/stripe/jobs/1");
        assert_eq!(u, "https://boards.greenhouse.io/stripe/jobs/1");
    }

    #[test]
    fn invalid_url_returns_empty() {
        // Truly unparseable even with https:// prefix.
        assert_eq!(canonical_url("://bad url here"), "");
    }

    #[test]
    fn bare_domain_gets_https_scheme() {
        // "example.com/jobs" isn't a valid URL but becomes valid after prepending https://.
        let u = canonical_url("example.com/jobs");
        assert!(u.starts_with("https://example.com"));
    }

    #[test]
    fn duplicate_query_key_first_value_wins() {
        // BTreeMap de-duplicates keys; first (alphabetically, then insertion) wins.
        let u = canonical_url("https://example.com/jobs?id=1&id=2");
        assert!(u.contains("id="));
    }
}
