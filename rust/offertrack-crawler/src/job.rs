use chrono::NaiveDateTime;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::canonical::canonical_url;

/// Same namespace as Python `storage._JOB_LISTING_NAMESPACE`.
pub static JOB_NAMESPACE: Lazy<Uuid> =
    Lazy::new(|| Uuid::parse_str("018d6b2e-8000-7000-8000-000000000001").expect("job namespace uuid"));

#[derive(Debug, Clone, Default)]
pub struct JobPosting {
    pub title: String,
    pub company: String,
    pub url: String,
    pub location: Option<String>,
    pub description: Option<String>,
    pub posted_date: Option<NaiveDateTime>,
    pub source: Option<String>,
    pub job_id: String,
    pub raw: serde_json::Value,
}

/// Storage / dedupe key: canonical listing URL only (same job from RSS + ATS + aggregator → one row).
pub fn listing_signature_canonical_url(url: &str) -> String {
    let key = canonical_url(url);
    let key = if key.is_empty() {
        url.trim().to_lowercase()
    } else {
        key
    };
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

/// Stable `job_id` after cross-source dedupe (URL-canonical; ignores `source` tag).
pub fn stable_job_id_canonical_url(url: &str) -> String {
    let sig = listing_signature_canonical_url(url);
    Uuid::new_v5(&*JOB_NAMESPACE, sig.as_bytes()).to_string()
}

pub fn assign_canonical_job_ids(jobs: &mut [JobPosting]) {
    for j in jobs.iter_mut() {
        j.job_id = stable_job_id_canonical_url(&j.url);
    }
}

fn merge_job_group(group: Vec<JobPosting>) -> JobPosting {
    if group.len() == 1 {
        return group.into_iter().next().unwrap();
    }

    let mut sources: Vec<String> = group
        .iter()
        .filter_map(|j| j.source.as_ref().cloned())
        .collect();
    sources.sort();
    sources.dedup();

    let best_idx = group
        .iter()
        .enumerate()
        .max_by_key(|(_, j)| {
            let desc_len = j.description.as_ref().map(|s| s.len()).unwrap_or(0);
            (desc_len, j.posted_date)
        })
        .map(|(i, _)| i)
        .unwrap_or(0);

    let mut out = group[best_idx].clone();
    out.source = Some(sources.join(" | "));

    // Keep the newest posted_date from any entry in the group.
    for j in &group {
        if let Some(p) = j.posted_date {
            out.posted_date = match out.posted_date {
                Some(o) => Some(o.max(p)),
                None => Some(p),
            };
        }
    }

    let canon = canonical_url(&out.url);
    if !canon.is_empty() {
        out.url = canon;
    }

    out.job_id.clear();
    out
}

/// Union of all crawled rows, merged by canonical apply URL; combined `source` labels.
pub fn dedupe_merge_by_canonical_url(mut jobs: Vec<JobPosting>) -> (Vec<JobPosting>, usize) {
    use std::collections::HashMap;

    let before = jobs.len();
    let mut map: HashMap<String, Vec<JobPosting>> = HashMap::new();
    for j in jobs.drain(..) {
        let k = canonical_url(&j.url);
        let k = if k.is_empty() {
            j.url.trim().to_lowercase()
        } else {
            k
        };
        map.entry(k).or_default().push(j);
    }

    let mut out: Vec<JobPosting> = map.into_values().map(merge_job_group).collect();
    out.sort_by(|a, b| canonical_url(&a.url).cmp(&canonical_url(&b.url)));
    let removed = before.saturating_sub(out.len());
    (out, removed)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExportRow {
    #[serde(default)]
    pub job_id: String,
    pub title: String,
    pub company: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posted_date: Option<String>,
    /// UTC instant (RFC3339) when this `jobs.json` export was written; same value for all rows in one crawl run.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub crawl_exported_at_utc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default)]
    pub jd: String,
}

impl ExportRow {
    pub fn into_job_posting(self) -> JobPosting {
        let posted = crate::date_parse::parse_date(self.posted_date.as_deref());
        JobPosting {
            title: self.title,
            company: self.company,
            url: self.url,
            location: self.location,
            description: if self.jd.is_empty() {
                None
            } else {
                Some(self.jd)
            },
            posted_date: posted,
            source: self.source,
            job_id: String::new(),
            raw: serde_json::json!({ "from": "export_row" }),
        }
    }
}

impl From<&JobPosting> for ExportRow {
    fn from(j: &JobPosting) -> Self {
        ExportRow {
            job_id: j.job_id.clone(),
            title: j.title.clone(),
            company: j.company.clone(),
            location: j.location.clone(),
            url: j.url.clone(),
            posted_date: j.posted_date.map(|d| d.format("%Y-%m-%dT%H:%M:%S").to_string()),
            crawl_exported_at_utc: String::new(),
            source: j.source.clone(),
            jd: j.description.clone().unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_job(url: &str, source: &str, desc: &str, date_ymd: (i32, u32, u32)) -> JobPosting {
        JobPosting {
            title: "Engineer".into(),
            company: "Acme".into(),
            url: url.into(),
            source: Some(source.into()),
            description: Some(desc.into()),
            posted_date: Some(
                NaiveDate::from_ymd_opt(date_ymd.0, date_ymd.1, date_ymd.2)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            job_id: String::new(),
            raw: serde_json::json!({}),
            ..Default::default()
        }
    }

    #[test]
    fn dedupe_merges_sources_same_canonical_url() {
        let u = "https://stripe.com/jobs/search?gh_jid=123&utm_source=x";
        let j1 = JobPosting {
            title: "A".into(),
            company: "Stripe".into(),
            url: u.into(),
            location: None,
            description: Some("longer desc".into()),
            posted_date: Some(
                NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
            ),
            source: Some("greenhouse:stripe".into()),
            job_id: String::new(),
            raw: serde_json::json!({}),
        };
        let j2 = JobPosting {
            title: "A".into(),
            company: "Stripe".into(),
            url: "https://stripe.com/jobs/search?gh_jid=123".into(),
            location: None,
            description: Some("short".into()),
            posted_date: Some(
                NaiveDate::from_ymd_opt(2026, 2, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
            ),
            source: Some("jobright.ai".into()),
            job_id: String::new(),
            raw: serde_json::json!({}),
        };
        let (merged, removed) = dedupe_merge_by_canonical_url(vec![j1, j2]);
        assert_eq!(merged.len(), 1);
        assert_eq!(removed, 1);
        assert!(merged[0].source.as_deref().unwrap().contains("greenhouse"));
        assert!(merged[0].source.as_deref().unwrap().contains("jobright"));
        assert!(merged[0].description.as_deref().unwrap().contains("longer"));
        assert_eq!(merged[0].posted_date.unwrap().date(), NaiveDate::from_ymd_opt(2026, 2, 1).unwrap());
    }

    #[test]
    fn dedupe_distinct_urls_not_merged() {
        let j1 = make_job("https://example.com/jobs/1", "s1", "desc", (2026, 1, 1));
        let j2 = make_job("https://example.com/jobs/2", "s2", "desc", (2026, 1, 2));
        let (merged, removed) = dedupe_merge_by_canonical_url(vec![j1, j2]);
        assert_eq!(merged.len(), 2);
        assert_eq!(removed, 0);
    }

    #[test]
    fn dedupe_single_item_passthrough() {
        let j = make_job("https://example.com/jobs/1", "src", "d", (2026, 3, 1));
        let (merged, removed) = dedupe_merge_by_canonical_url(vec![j]);
        assert_eq!(merged.len(), 1);
        assert_eq!(removed, 0);
    }

    #[test]
    fn merge_picks_newest_posted_date() {
        let old = make_job("https://example.com/j/1", "s1", "short", (2026, 1, 1));
        let new = make_job("https://example.com/j/1", "s2", "short also", (2026, 6, 1));
        let (merged, _) = dedupe_merge_by_canonical_url(vec![old, new]);
        assert_eq!(merged[0].posted_date.unwrap().date(), NaiveDate::from_ymd_opt(2026, 6, 1).unwrap());
    }

    #[test]
    fn merge_picks_longest_description() {
        let short = make_job("https://example.com/j/2", "s1", "hi", (2026, 1, 1));
        let long = make_job("https://example.com/j/2", "s2", "a much longer description here", (2026, 1, 1));
        let (merged, _) = dedupe_merge_by_canonical_url(vec![short, long]);
        assert!(merged[0].description.as_deref().unwrap().len() > 5);
    }

    #[test]
    fn listing_signature_canonical_url_stable() {
        let s1 = listing_signature_canonical_url("https://example.com/jobs/1?utm_source=x");
        let s2 = listing_signature_canonical_url("https://example.com/jobs/1");
        assert_eq!(s1, s2, "UTM params must not change the signature");
    }

    #[test]
    fn listing_signature_canonical_url_nonempty_for_empty_input() {
        // Empty URL falls back to literal empty string hash — deterministic but non-empty hex.
        let s = listing_signature_canonical_url("");
        assert!(!s.is_empty());
    }

    #[test]
    fn stable_job_id_canonical_url_is_deterministic() {
        let id1 = stable_job_id_canonical_url("https://example.com/jobs/42");
        let id2 = stable_job_id_canonical_url("https://example.com/jobs/42");
        assert_eq!(id1, id2);
    }

    #[test]
    fn stable_job_id_ignores_utm() {
        let id1 = stable_job_id_canonical_url("https://example.com/jobs/42?utm_campaign=a");
        let id2 = stable_job_id_canonical_url("https://example.com/jobs/42");
        assert_eq!(id1, id2);
    }

    #[test]
    fn assign_canonical_job_ids_fills_job_id() {
        let mut jobs = vec![make_job("https://example.com/jobs/99", "src", "d", (2026, 1, 1))];
        assign_canonical_job_ids(&mut jobs);
        assert!(!jobs[0].job_id.is_empty());
        // UUID v5 format: 8-4-4-4-12
        assert_eq!(jobs[0].job_id.len(), 36);
    }

    #[test]
    fn export_row_round_trip() {
        let job = JobPosting {
            title: "SWE".into(),
            company: "Acme".into(),
            url: "https://example.com/job/1".into(),
            location: Some("Remote".into()),
            description: Some("Build stuff".into()),
            posted_date: Some(
                NaiveDate::from_ymd_opt(2026, 3, 15).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            ),
            source: Some("greenhouse:acme".into()),
            job_id: "abc-123".into(),
            raw: serde_json::json!({}),
        };
        let row = ExportRow::from(&job);
        assert_eq!(row.title, "SWE");
        assert_eq!(row.company, "Acme");
        assert_eq!(row.url, "https://example.com/job/1");
        assert_eq!(row.location, Some("Remote".into()));
        assert_eq!(row.jd, "Build stuff");
        assert!(row.posted_date.as_deref().unwrap().starts_with("2026-03-15"));

        let restored = row.into_job_posting();
        assert_eq!(restored.title, "SWE");
        assert_eq!(restored.description, Some("Build stuff".into()));
        assert!(restored.posted_date.is_some());
    }

    #[test]
    fn export_row_empty_jd_becomes_none() {
        let row = ExportRow {
            job_id: "x".into(),
            title: "T".into(),
            company: "C".into(),
            location: None,
            url: "https://example.com".into(),
            posted_date: None,
            crawl_exported_at_utc: String::new(),
            source: None,
            jd: String::new(),
        };
        let job = row.into_job_posting();
        assert!(job.description.is_none());
    }
}

#[derive(Debug, Serialize)]
pub struct MinimalRow {
    pub job_id: String,
    pub jd: String,
    pub first_seen_at: String,
    /// Same as full export: UTC time when this jobs.json was written.
    pub crawl_exported_at_utc: String,
}
