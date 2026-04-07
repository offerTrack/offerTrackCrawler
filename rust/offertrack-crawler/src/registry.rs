//! Expand `config/registry/employers.json` into `api_sources` + `sites` rows (Phase 1 scale-out).

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

/// Load registry file and expand to `(api_sources, sites)`.
pub fn load_and_expand(path: &Path) -> Result<(Vec<Value>, Vec<Value>)> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read registry {}", path.display()))?;
    let v: Value = serde_json::from_str(&text).context("registry JSON")?;
    let employers = if let Some(arr) = v.as_array() {
        arr.clone()
    } else {
        v.get("employers")
            .and_then(|x| x.as_array())
            .cloned()
            .context("registry root must be { \"employers\": [...] } or a JSON array")?
    };

    let mut api = Vec::new();
    let mut sites = Vec::new();
    for (i, row) in employers.iter().enumerate() {
        match expand_one(row) {
            Ok(Some(Either::Api(a))) => api.push(a),
            Ok(Some(Either::Site(s))) => sites.push(s),
            Ok(None) => {}
            Err(e) => eprintln!("[WARN] registry row {i}: {e}"),
        }
    }
    Ok((api, sites))
}

enum Either {
    Api(Value),
    Site(Value),
}

fn expand_one(row: &Value) -> Result<Option<Either>> {
    let enabled = row.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);
    if !enabled {
        return Ok(None);
    }

    let company = row
        .get("company")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing company"))?
        .trim();
    if company.is_empty() {
        return Err(anyhow!("empty company"));
    }

    let ats = row
        .get("ats")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing ats"))?
        .trim()
        .to_lowercase();

    let career_url = row.get("career_url").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());

    match ats.as_str() {
        "greenhouse" | "gh" => {
            let board = row
                .get("board")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("greenhouse: missing board"))?;
            let mut o = json!({
                "enabled": true,
                "type": "greenhouse",
                "board": board,
                "company": company,
            });
            merge_tier_metadata(&mut o, row);
            if let Some(u) = career_url {
                o.as_object_mut().unwrap().insert("_career_url".to_string(), json!(u));
            }
            Ok(Some(Either::Api(o)))
        }
        "lever" => {
            let slug = row
                .get("lever_company")
                .or_else(|| row.get("board"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("lever: missing lever_company or board (API slug)"))?;
            let mut o = json!({
                "enabled": true,
                "type": "lever",
                "company": slug,
                "display_name": company,
            });
            merge_tier_metadata(&mut o, row);
            if let Some(u) = career_url {
                o.as_object_mut().unwrap().insert("_career_url".to_string(), json!(u));
            }
            Ok(Some(Either::Api(o)))
        }
        "ashby" => {
            let board = row
                .get("board")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("ashby: missing board (job board slug)"))?;
            let mut o = json!({
                "enabled": true,
                "type": "ashby",
                "board": board,
                "company": company,
            });
            merge_tier_metadata(&mut o, row);
            if let Some(u) = career_url {
                o.as_object_mut().unwrap().insert("_career_url".to_string(), json!(u));
            }
            Ok(Some(Either::Api(o)))
        }
        "rss" | "atom" => {
            let url = row
                .get("feed_url")
                .or_else(|| row.get("url"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("rss: missing feed_url or url"))?;
            let mut o = json!({
                "enabled": true,
                "type": "rss",
                "url": url,
                "company": company,
            });
            for (k, rk) in [
                ("fetch_entry_detail_page", "fetch_entry_detail_page"),
                ("max_entry_detail_fetches", "max_entry_detail_fetches"),
                (
                    "entry_detail_min_description_chars",
                    "entry_detail_min_description_chars",
                ),
            ] {
                if let Some(x) = row.get(rk).cloned() {
                    o.as_object_mut().unwrap().insert(k.to_string(), x);
                }
            }
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Api(o)))
        }
        "jobright" => {
            let urls: Vec<String> = row
                .get("job_list_urls")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                .unwrap_or_default();
            if urls.is_empty() {
                return Err(anyhow!("jobright: missing or empty job_list_urls"));
            }
            let mut o = json!({
                "enabled": true,
                "type": "jobright",
                "job_list_urls": urls,
                "company": company,
            });
            if let Some(b) = row.get("fetch_company_detail").and_then(|v| v.as_bool()) {
                o.as_object_mut().unwrap().insert("fetch_company_detail".to_string(), json!(b));
            }
            if let Some(n) = row.get("max_detail_requests").and_then(|v| v.as_u64()) {
                o.as_object_mut()
                    .unwrap()
                    .insert("max_detail_requests".to_string(), json!(n));
            }
            if let Some(n) = row.get("detail_concurrency").and_then(|v| v.as_u64()) {
                o.as_object_mut()
                    .unwrap()
                    .insert("detail_concurrency".to_string(), json!(n));
            }
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Api(o)))
        }
        "amazon_jobs" | "amazon" => {
            let locale_prefix = row
                .get("locale_prefix")
                .and_then(|v| v.as_str())
                .unwrap_or("/en");
            let base_query = row.get("base_query").and_then(|v| v.as_str()).unwrap_or("");
            let loc_query = row.get("loc_query").and_then(|v| v.as_str()).unwrap_or("");
            let result_limit = row.get("result_limit").and_then(|v| v.as_u64()).unwrap_or(100);
            let max_jobs = row.get("max_jobs").and_then(|v| v.as_u64()).unwrap_or(500);
            let sort = row.get("sort").and_then(|v| v.as_str()).unwrap_or("recent");
            let page_delay_ms = row.get("page_delay_ms").and_then(|v| v.as_u64()).unwrap_or(400);
            let mut o = json!({
                "enabled": true,
                "type": "amazon_jobs",
                "locale_prefix": locale_prefix,
                "base_query": base_query,
                "loc_query": loc_query,
                "result_limit": result_limit,
                "max_jobs": max_jobs,
                "sort": sort,
                "page_delay_ms": page_delay_ms,
                "company": company,
            });
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Api(o)))
        }
        "workday" => {
            let workday_host = row
                .get("workday_host")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("workday: missing workday_host"))?;
            let cxs_org = row
                .get("cxs_org")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("workday: missing cxs_org"))?;
            let cxs_site = row
                .get("cxs_site")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("workday: missing cxs_site"))?;
            let locale = row.get("locale").and_then(|v| v.as_str()).unwrap_or("en-US");
            let page_limit = row.get("page_limit").and_then(|v| v.as_u64()).unwrap_or(20);
            let max_jobs = row.get("max_jobs").and_then(|v| v.as_u64()).unwrap_or(500);
            let page_delay_ms = row.get("page_delay_ms").and_then(|v| v.as_u64()).unwrap_or(400);
            let mut o = json!({
                "enabled": true,
                "type": "workday",
                "company": company,
                "workday_host": workday_host,
                "cxs_org": cxs_org,
                "cxs_site": cxs_site,
                "locale": locale,
                "page_limit": page_limit,
                "max_jobs": max_jobs,
                "page_delay_ms": page_delay_ms,
            });
            if let Some(u) = row.get("cxs_jobs_url").and_then(|v| v.as_str()) {
                o.as_object_mut()
                    .unwrap()
                    .insert("cxs_jobs_url".to_string(), json!(u));
            }
            if let Some(u) = career_url {
                o.as_object_mut()
                    .unwrap()
                    .insert("career_url".to_string(), json!(u));
            }
            for (k, rk) in [
                ("fetch_job_descriptions", "fetch_job_descriptions"),
                ("max_description_fetches", "max_description_fetches"),
                ("description_fetch_concurrency", "description_fetch_concurrency"),
                ("description_fetch_delay_ms", "description_fetch_delay_ms"),
                ("fallback_posted_date_to_now", "fallback_posted_date_to_now"),
            ] {
                if let Some(x) = row.get(rk).cloned() {
                    o.as_object_mut().unwrap().insert(k.to_string(), x);
                }
            }
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Api(o)))
        }
        "icims" => {
            let list_url = row
                .get("icims_jobs_url")
                .or_else(|| row.get("url"))
                .or_else(|| row.get("career_url"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("icims: missing icims_jobs_url/url/career_url"))?;
            let mut o = json!({
                "enabled": true,
                "type": "icims",
                "company": company,
                "icims_jobs_url": list_url,
            });
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Api(o)))
        }
        "bamboohr" => {
            let mut o = json!({
                "enabled": true,
                "type": "bamboohr",
                "company": company,
            });
            if let Some(sub) = row.get("bamboohr_subdomain").and_then(|v| v.as_str()) {
                o.as_object_mut()
                    .unwrap()
                    .insert("bamboohr_subdomain".to_string(), json!(sub.trim()));
            }
            if let Some(base) = row.get("bamboohr_careers_base").and_then(|v| v.as_str()) {
                o.as_object_mut().unwrap().insert(
                    "bamboohr_careers_base".to_string(),
                    json!(base.trim().trim_end_matches('/')),
                );
            }
            if o.get("bamboohr_subdomain").is_none() && o.get("bamboohr_careers_base").is_none() {
                if let Some(u) = career_url {
                    o.as_object_mut()
                        .unwrap()
                        .insert("bamboohr_careers_base".to_string(), json!(u.trim_end_matches('/')));
                } else {
                    return Err(anyhow!("bamboohr: missing bamboohr_subdomain or bamboohr_careers_base"));
                }
            }
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Api(o)))
        }
        "workable" => {
            let subdomain = row
                .get("workable_subdomain")
                .or_else(|| row.get("board"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("workable: missing workable_subdomain or board"))?;
            let token = row
                .get("workable_api_token")
                .or_else(|| row.get("api_token"))
                .and_then(|v| v.as_str());
            let token_env = row.get("workable_api_token_env").and_then(|v| v.as_str());
            if token.is_none() && token_env.is_none() {
                return Err(anyhow!("workable: missing workable_api_token/api_token or workable_api_token_env"));
            }
            let mut o = json!({
                "enabled": true,
                "type": "workable",
                "company": company,
                "workable_subdomain": subdomain.trim(),
                "limit": row.get("limit").and_then(|v| v.as_u64()).unwrap_or(100),
                "max_pages": row.get("max_pages").and_then(|v| v.as_u64()).unwrap_or(30),
            });
            if let Some(tk) = token {
                o.as_object_mut()
                    .unwrap()
                    .insert("workable_api_token".into(), json!(tk));
            }
            if let Some(ek) = token_env {
                o.as_object_mut()
                    .unwrap()
                    .insert("workable_api_token_env".into(), json!(ek));
            }
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Api(o)))
        }
        "smartrecruiters" | "smartrecruiters_api" | "sr" => {
            let company_id = row
                .get("smartrecruiters_company_id")
                .or_else(|| row.get("board"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("smartrecruiters: missing smartrecruiters_company_id or board (API company id)"))?;
            let page_size = row.get("page_size").and_then(|v| v.as_u64()).unwrap_or(100);
            let max_pages = row.get("max_pages").and_then(|v| v.as_u64()).unwrap_or(50);
            let mut o = json!({
                "enabled": true,
                "type": "smartrecruiters",
                "smartrecruiters_company_id": company_id.trim(),
                "company": company,
                "page_size": page_size,
                "max_pages": max_pages,
            });
            merge_tier_metadata(&mut o, row);
            if let Some(u) = career_url {
                o.as_object_mut().unwrap().insert("_career_url".to_string(), json!(u));
            }
            Ok(Some(Either::Api(o)))
        }
        "recruitee" => {
            let mut o = json!({
                "enabled": true,
                "type": "recruitee",
                "company": company,
            });
            if let Some(sub) = row.get("recruitee_subdomain").and_then(|v| v.as_str()) {
                o.as_object_mut()
                    .unwrap()
                    .insert("recruitee_subdomain".to_string(), json!(sub.trim()));
            }
            if let Some(base) = row.get("recruitee_careers_base").and_then(|v| v.as_str()) {
                o.as_object_mut().unwrap().insert(
                    "recruitee_careers_base".to_string(),
                    json!(base.trim().trim_end_matches('/')),
                );
            }
            if o.get("recruitee_subdomain").is_none() && o.get("recruitee_careers_base").is_none() {
                if let Some(u) = career_url {
                    let u = u.trim_end_matches('/');
                    if u.contains("recruitee.com") {
                        o.as_object_mut()
                            .unwrap()
                            .insert("recruitee_careers_base".to_string(), json!(u));
                    } else {
                        return Err(anyhow!(
                            "recruitee: set recruitee_subdomain or recruitee_careers_base (or use career_url on *.recruitee.com)"
                        ));
                    }
                } else {
                    return Err(anyhow!(
                        "recruitee: missing recruitee_subdomain or recruitee_careers_base"
                    ));
                }
            }
            merge_tier_metadata(&mut o, row);
            if let Some(u) = career_url {
                o.as_object_mut()
                    .unwrap()
                    .insert("_career_url".to_string(), json!(u));
            }
            Ok(Some(Either::Api(o)))
        }
        "site" | "html" | "schema" => {
            let domain = row
                .get("domain")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("site: missing domain"))?;
            let start_urls: Vec<String> = if let Some(arr) = row.get("start_urls").and_then(|v| v.as_array()) {
                arr.iter().filter_map(|x| x.as_str().map(String::from)).collect()
            } else if let Some(u) = career_url {
                vec![u.to_string()]
            } else {
                return Err(anyhow!("site: need start_urls or career_url"));
            };
            if start_urls.is_empty() {
                return Err(anyhow!("site: empty start_urls"));
            }
            let extractor = row
                .get("extractor")
                .and_then(|v| v.as_str())
                .unwrap_or("GenericSchemaOrgExtractor");
            let mut o = json!({
                "enabled": true,
                "domain": domain,
                "start_urls": start_urls,
                "extractor": extractor,
            });
            if let Some(arr) = row.get("job_link_href_contains").and_then(|v| v.as_array()) {
                if !arr.is_empty() {
                    o.as_object_mut()
                        .unwrap()
                        .insert("job_link_href_contains".into(), Value::Array(arr.clone()));
                }
            }
            merge_tier_metadata(&mut o, row);
            Ok(Some(Either::Site(o)))
        }
        other => Err(anyhow!("unknown ats: {other}")),
    }
}

fn merge_tier_metadata(target: &mut Value, row: &Value) {
    let Some(obj) = target.as_object_mut() else {
        return;
    };
    if let Some(t) = row.get("tier").and_then(|v| v.as_str()) {
        obj.insert("_tier".to_string(), json!(t));
    }
    if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
        obj.insert("_registry_id".to_string(), json!(id));
    }
    // Optional tagging for ops / filtering (healthcare, education, trades, etc.)
    for key in ["vertical", "industry"] {
        if let Some(s) = row.get(key).and_then(|v| v.as_str()) {
            obj.insert(format!("_{key}"), json!(s));
        }
    }
    for key in ["min_expected_jobs"] {
        if let Some(v) = row.get(key) {
            obj.insert(key.to_string(), v.clone());
        }
    }
}

/// Stable key for deduplicating merged `api_sources`.
pub fn api_source_key(v: &Value) -> Option<String> {
    let t = v.get("type").and_then(|x| x.as_str())?;
    match t {
        "greenhouse" => {
            let b = v.get("board").and_then(|x| x.as_str())?;
            Some(format!("greenhouse:{b}"))
        }
        "lever" => {
            let c = v.get("company").and_then(|x| x.as_str())?;
            Some(format!("lever:{c}"))
        }
        "ashby" => {
            let b = v.get("board").and_then(|x| x.as_str())?;
            Some(format!("ashby:{b}"))
        }
        "rss" | "atom" => {
            let u = v.get("url").and_then(|x| x.as_str())?;
            Some(format!("rss:{u}"))
        }
        "jobright" => {
            let urls = v.get("job_list_urls").and_then(|x| x.as_array())?;
            let joined: Vec<&str> = urls.iter().filter_map(|x| x.as_str()).collect();
            Some(format!("jobright:{}", joined.join("|")))
        }
        "amazon_jobs" => {
            let loc = v.get("locale_prefix").and_then(|x| x.as_str()).unwrap_or("/en");
            let bq = v.get("base_query").and_then(|x| x.as_str()).unwrap_or("");
            let lq = v.get("loc_query").and_then(|x| x.as_str()).unwrap_or("");
            let mx = v.get("max_jobs").and_then(|x| x.as_u64()).unwrap_or(0);
            Some(format!("amazon_jobs:{loc}|{bq}|{lq}|{mx}"))
        }
        "workday" => {
            let h = v.get("workday_host").and_then(|x| x.as_str())?;
            let o = v.get("cxs_org").and_then(|x| x.as_str())?;
            let s = v.get("cxs_site").and_then(|x| x.as_str())?;
            Some(format!("workday:{h}:{o}:{s}"))
        }
        "recruitee" => {
            if let Some(b) = v.get("recruitee_careers_base").and_then(|x| x.as_str()) {
                Some(format!("recruitee:base:{b}"))
            } else if let Some(s) = v.get("recruitee_subdomain").and_then(|x| x.as_str()) {
                Some(format!("recruitee:sub:{s}"))
            } else {
                None
            }
        }
        "smartrecruiters" => {
            let id = v
                .get("smartrecruiters_company_id")
                .or_else(|| v.get("board"))
                .and_then(|x| x.as_str())?;
            Some(format!("smartrecruiters:{id}"))
        }
        "icims" => {
            let u = v.get("icims_jobs_url").and_then(|x| x.as_str())?;
            Some(format!("icims:{u}"))
        }
        "bamboohr" => {
            if let Some(sub) = v.get("bamboohr_subdomain").and_then(|x| x.as_str()) {
                Some(format!("bamboohr:sub:{sub}"))
            } else {
                Some(format!(
                    "bamboohr:base:{}",
                    v.get("bamboohr_careers_base").and_then(|x| x.as_str())?
                ))
            }
        }
        "workable" => {
            let sub = v.get("workable_subdomain").and_then(|x| x.as_str())?;
            Some(format!("workable:{sub}"))
        }
        _ => None,
    }
}

/// Dedupe api_sources: first occurrence wins.
pub fn dedupe_api_sources(sources: &mut Vec<Value>) {
    let mut seen = std::collections::HashSet::new();
    sources.retain(|v| {
        let Some(k) = api_source_key(v) else {
            return true;
        };
        seen.insert(k)
    });
}

/// Dedupe sites by (domain, first start_url).
pub fn dedupe_sites(sites: &mut Vec<Value>) {
    let mut seen = std::collections::HashSet::new();
    sites.retain(|v| {
        let domain = v.get("domain").and_then(|x| x.as_str()).unwrap_or("");
        let u0 = v
            .get("start_urls")
            .and_then(|x| x.as_array())
            .and_then(|a| a.first())
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let k = format!("{domain}|{u0}");
        seen.insert(k)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api_row(company: &str, ats: &str, extra: serde_json::Value) -> Value {
        let mut m = serde_json::json!({ "company": company, "ats": ats });
        if let (Some(obj), Some(ext)) = (m.as_object_mut(), extra.as_object()) {
            obj.extend(ext.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        m
    }

    #[test]
    fn expand_greenhouse_and_dedupe_key() {
        let row = json!({
            "company": "TestCo",
            "ats": "greenhouse",
            "board": "testco",
            "tier": "series_a"
        });
        let r = expand_one(&row).unwrap().unwrap();
        match r {
            Either::Api(v) => {
                assert_eq!(v["type"], "greenhouse");
                assert_eq!(v["board"], "testco");
                assert_eq!(api_source_key(&v).unwrap(), "greenhouse:testco");
            }
            Either::Site(_) => panic!("expected api"),
        }
    }

    #[test]
    fn expand_lever() {
        let row = api_row("LeverCo", "lever", json!({ "board": "levercoslug" }));
        let r = expand_one(&row).unwrap().unwrap();
        match r {
            Either::Api(v) => {
                assert_eq!(v["type"], "lever");
                assert_eq!(api_source_key(&v).unwrap(), "lever:levercoslug");
            }
            Either::Site(_) => panic!("expected api"),
        }
    }

    #[test]
    fn expand_ashby() {
        let row = api_row("AshbyCo", "ashby", json!({ "board": "ashbyboard" }));
        let r = expand_one(&row).unwrap().unwrap();
        match r {
            Either::Api(v) => {
                assert_eq!(v["type"], "ashby");
                assert_eq!(api_source_key(&v).unwrap(), "ashby:ashbyboard");
            }
            Either::Site(_) => panic!("expected api"),
        }
    }

    #[test]
    fn expand_workday() {
        let row = api_row("BigCorp", "workday", json!({
            "workday_host": "bigcorp.wd1.myworkdayjobs.com",
            "cxs_org": "bigcorp",
            "cxs_site": "bigcorp-external",
        }));
        let r = expand_one(&row).unwrap().unwrap();
        match r {
            Either::Api(v) => {
                assert_eq!(v["type"], "workday");
                assert!(api_source_key(&v).unwrap().starts_with("workday:"));
            }
            Either::Site(_) => panic!("expected api"),
        }
    }

    #[test]
    fn expand_bamboohr_subdomain() {
        let row = api_row("BambooCo", "bamboohr", json!({ "bamboohr_subdomain": "bambooco" }));
        let r = expand_one(&row).unwrap().unwrap();
        match r {
            Either::Api(v) => {
                assert_eq!(v["type"], "bamboohr");
                assert_eq!(api_source_key(&v).unwrap(), "bamboohr:sub:bambooco");
            }
            Either::Site(_) => panic!("expected api"),
        }
    }

    #[test]
    fn expand_rss() {
        let row = api_row("FeedCo", "rss", json!({ "feed_url": "https://feedco.com/rss.xml" }));
        let r = expand_one(&row).unwrap().unwrap();
        match r {
            Either::Api(v) => {
                assert_eq!(v["type"], "rss");
                assert!(api_source_key(&v).unwrap().starts_with("rss:https://feedco.com/rss.xml"));
            }
            Either::Site(_) => panic!("expected api"),
        }
    }

    #[test]
    fn expand_site_html() {
        let row = api_row("SiteCo", "site", json!({
            "domain": "siteco.com",
            "career_url": "https://siteco.com/careers",
        }));
        let r = expand_one(&row).unwrap().unwrap();
        match r {
            Either::Site(v) => {
                assert_eq!(v["domain"], "siteco.com");
            }
            Either::Api(_) => panic!("expected site"),
        }
    }

    #[test]
    fn disabled_row_returns_none() {
        let row = json!({ "company": "SkipCo", "ats": "greenhouse", "board": "skip", "enabled": false });
        assert!(expand_one(&row).unwrap().is_none());
    }

    #[test]
    fn missing_company_returns_error() {
        let row = json!({ "ats": "greenhouse", "board": "x" });
        assert!(expand_one(&row).is_err());
    }

    #[test]
    fn unknown_ats_returns_error() {
        let row = json!({ "company": "X", "ats": "unknown_ats_xyz" });
        assert!(expand_one(&row).is_err());
    }

    #[test]
    fn greenhouse_missing_board_returns_error() {
        let row = json!({ "company": "X", "ats": "greenhouse" });
        assert!(expand_one(&row).is_err());
    }

    #[test]
    fn dedupe_api_sources_removes_duplicates() {
        let row = json!({ "company": "X", "ats": "greenhouse", "board": "x" });
        let (mut sources, _) = {
            let a = expand_one(&row).unwrap().unwrap();
            let b = expand_one(&row).unwrap().unwrap();
            match (a, b) {
                (Either::Api(a), Either::Api(b)) => (vec![a, b], ()),
                _ => panic!(),
            }
        };
        assert_eq!(sources.len(), 2);
        dedupe_api_sources(&mut sources);
        assert_eq!(sources.len(), 1);
    }

    #[test]
    fn dedupe_sites_removes_duplicates() {
        let site = json!({
            "enabled": true,
            "domain": "example.com",
            "start_urls": ["https://example.com/careers"],
            "extractor": "GenericSchemaOrgExtractor",
        });
        let mut sites = vec![site.clone(), site];
        assert_eq!(sites.len(), 2);
        dedupe_sites(&mut sites);
        assert_eq!(sites.len(), 1);
    }

    #[test]
    fn api_source_key_workable() {
        let v = json!({ "type": "workable", "workable_subdomain": "mycompany" });
        assert_eq!(api_source_key(&v).unwrap(), "workable:mycompany");
    }

    #[test]
    fn api_source_key_smartrecruiters() {
        let v = json!({ "type": "smartrecruiters", "smartrecruiters_company_id": "BigCo" });
        assert_eq!(api_source_key(&v).unwrap(), "smartrecruiters:BigCo");
    }

    #[test]
    fn api_source_key_icims() {
        let v = json!({ "type": "icims", "icims_jobs_url": "https://careers.icims.com/jobs" });
        assert_eq!(api_source_key(&v).unwrap(), "icims:https://careers.icims.com/jobs");
    }

    #[test]
    fn merge_tier_metadata_copies_fields() {
        let row = json!({ "tier": "series_b", "id": "emp-001", "vertical": "fintech" });
        let mut target = json!({ "type": "greenhouse" });
        merge_tier_metadata(&mut target, &row);
        assert_eq!(target["_tier"], "series_b");
        assert_eq!(target["_registry_id"], "emp-001");
        assert_eq!(target["_vertical"], "fintech");
    }
}
