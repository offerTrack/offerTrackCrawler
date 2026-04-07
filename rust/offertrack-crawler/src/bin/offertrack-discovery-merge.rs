//! Append rows from `offertrack-career-discover` CSV into `employers.json` (skip duplicates by id / ATS key).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::{json, Value};
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "offertrack-discovery-merge")]
#[command(about = "Merge discovery CSV → employers.json (append new registry rows).")]
struct Args {
    #[arg(long)]
    discovered: PathBuf,
    #[arg(long)]
    employers: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    dry_run: bool,
}

fn header_map(h: &csv::StringRecord) -> Result<HashMap<String, usize>> {
    let mut m = HashMap::new();
    for (i, name) in h.iter().enumerate() {
        m.insert(name.trim().to_lowercase(), i);
    }
    Ok(m)
}

fn get<'a>(rec: &'a csv::StringRecord, m: &HashMap<String, usize>, key: &str) -> &'a str {
    m.get(key)
        .and_then(|&i| rec.get(i))
        .unwrap_or("")
        .trim()
}

fn norm_key(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_lowercase()
}

fn id_for_company(company: &str, suffix: &str) -> String {
    let base: String = company
        .split_whitespace()
        .map(|w| norm_key(w))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    format!("{base}_{suffix}")
}

fn parse_workday_page(url_str: &str) -> Option<(String, String, String, String)> {
    let u = Url::parse(url_str.trim()).ok()?;
    let host = u.host_str()?.to_string();
    let path: Vec<&str> = u.path_segments()?.collect();
    let path: Vec<&str> = path.into_iter().filter(|s| !s.is_empty()).collect();
    if path.len() < 2 {
        return None;
    }
    let locale = path[0].to_string();
    let cxs_site = path[1].to_string();
    let cxs_org = host.split('.').next()?.to_string();
    Some((host, cxs_org, cxs_site, locale))
}

fn parse_pipe_workday(slug: &str) -> Option<(String, String, String, String)> {
    let p: Vec<&str> = slug.split('|').collect();
    if p.len() != 3 {
        return None;
    }
    let host = p[0].to_string();
    let locale = p[1].to_string();
    let site = p[2].to_string();
    let org = host.split('.').next()?.to_string();
    Some((host, org, site, locale))
}

fn row_to_employer(rec: &csv::StringRecord, m: &HashMap<String, usize>) -> Option<Value> {
    let ats = get(rec, m, "detected_ats");
    let company = get(rec, m, "company");
    let career_url = get(rec, m, "career_url");
    let slug = get(rec, m, "slug_or_board");
    let wd_page = get(rec, m, "workday_page_url");

    if company.is_empty() {
        return None;
    }

    let push_career = |o: &mut serde_json::Map<String, Value>| {
        if !career_url.is_empty() {
            o.insert("career_url".to_string(), json!(career_url));
        }
    };

    match ats {
        "greenhouse" if !slug.is_empty() => {
            let mut o = serde_json::Map::new();
            o.insert("id".to_string(), json!(id_for_company(company, "gh")));
            o.insert("company".to_string(), json!(company));
            o.insert("ats".to_string(), json!("greenhouse"));
            o.insert("board".to_string(), json!(slug));
            o.insert("enabled".to_string(), json!(true));
            push_career(&mut o);
            Some(Value::Object(o))
        }
        "lever" if !slug.is_empty() => {
            let mut o = serde_json::Map::new();
            o.insert("id".to_string(), json!(id_for_company(company, "lever")));
            o.insert("company".to_string(), json!(company));
            o.insert("ats".to_string(), json!("lever"));
            o.insert("lever_company".to_string(), json!(slug));
            o.insert("enabled".to_string(), json!(true));
            push_career(&mut o);
            Some(Value::Object(o))
        }
        "ashby" if !slug.is_empty() => {
            let mut o = serde_json::Map::new();
            o.insert("id".to_string(), json!(id_for_company(company, "ashby")));
            o.insert("company".to_string(), json!(company));
            o.insert("ats".to_string(), json!("ashby"));
            o.insert("board".to_string(), json!(slug));
            o.insert("enabled".to_string(), json!(true));
            push_career(&mut o);
            Some(Value::Object(o))
        }
        "workday" => {
            let parsed = if !wd_page.is_empty() {
                parse_workday_page(wd_page)
            } else {
                parse_pipe_workday(slug)
            };
            let (host, org, site, locale) = parsed?;
            let mut o = serde_json::Map::new();
            o.insert(
                "id".to_string(),
                json!(id_for_company(company, "workday")),
            );
            o.insert("company".to_string(), json!(company));
            o.insert("ats".to_string(), json!("workday"));
            o.insert("workday_host".to_string(), json!(host));
            o.insert("cxs_org".to_string(), json!(org));
            o.insert("cxs_site".to_string(), json!(site));
            o.insert("locale".to_string(), json!(locale));
            o.insert("max_jobs".to_string(), json!(600));
            o.insert("page_limit".to_string(), json!(20));
            o.insert("page_delay_ms".to_string(), json!(400));
            o.insert("enabled".to_string(), json!(true));
            push_career(&mut o);
            Some(Value::Object(o))
        }
        "amazon_jobs" => {
            let mut o = serde_json::Map::new();
            o.insert("id".to_string(), json!(id_for_company(company, "amz")));
            o.insert(
                "company".to_string(),
                json!(format!("{company} (amazon.jobs)")),
            );
            o.insert("ats".to_string(), json!("amazon_jobs"));
            o.insert("locale_prefix".to_string(), json!("/en"));
            o.insert("loc_query".to_string(), json!("United States"));
            o.insert("max_jobs".to_string(), json!(800));
            o.insert("result_limit".to_string(), json!(100));
            o.insert("enabled".to_string(), json!(true));
            o.insert("career_url".to_string(), json!("https://www.amazon.jobs"));
            Some(Value::Object(o))
        }
        _ => None,
    }
}

fn employer_dedupe_key(v: &Value) -> Option<String> {
    let ats = v.get("ats")?.as_str()?;
    match ats {
        "greenhouse" => Some(format!("gh:{}", v.get("board")?.as_str()?)),
        "lever" => Some(format!(
            "lever:{}",
            v.get("lever_company")
                .or_else(|| v.get("board"))?
                .as_str()?
        )),
        "ashby" => Some(format!("ashby:{}", v.get("board")?.as_str()?)),
        "workday" => Some(format!(
            "wd:{}:{}:{}",
            v.get("workday_host")?.as_str()?,
            v.get("cxs_org")?.as_str()?,
            v.get("cxs_site")?.as_str()?
        )),
        "amazon_jobs" => {
            let lq = v.get("loc_query").and_then(|x| x.as_str()).unwrap_or("");
            Some(format!("amz:{lq}"))
        }
        _ => None,
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let raw_emp = std::fs::read_to_string(&args.employers)
        .with_context(|| format!("read {}", args.employers.display()))?;
    let array_format = raw_emp.trim_start().starts_with('[');
    let root: Value = serde_json::from_str(&raw_emp).context("parse employers JSON")?;
    let mut employers: Vec<Value> = if let Some(a) = root.as_array() {
        a.clone()
    } else {
        root.get("employers")
            .and_then(|x| x.as_array())
            .cloned()
            .context("employers.json: expected [] or { \"employers\": [] }")?
    };

    let disc_text = std::fs::read_to_string(&args.discovered)
        .with_context(|| format!("read {}", args.discovered.display()))?;
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(disc_text.as_bytes());
    let headers = rdr.headers()?.clone();
    let hm = header_map(&headers)?;

    let mut seen: HashSet<String> = HashSet::new();
    for e in &employers {
        if let Some(id) = e.get("id").and_then(|x| x.as_str()) {
            seen.insert(format!("id:{id}"));
        }
        if let Some(k) = employer_dedupe_key(e) {
            seen.insert(format!("key:{k}"));
        }
    }

    let mut added = 0u32;
    for rec in rdr.records() {
        let rec = rec?;
        let Some(new_e) = row_to_employer(&rec, &hm) else {
            continue;
        };
        let id_key = new_e
            .get("id")
            .and_then(|x| x.as_str())
            .map(|id| format!("id:{id}"));
        let api_key = employer_dedupe_key(&new_e).map(|k| format!("key:{k}"));
        let mut skip = false;
        if let Some(ref k) = id_key {
            if seen.contains(k) {
                skip = true;
            }
        }
        if let Some(ref k) = api_key {
            if seen.contains(k) {
                skip = true;
            }
        }
        if skip {
            continue;
        }
        if let Some(k) = id_key {
            seen.insert(k);
        }
        if let Some(k) = api_key {
            seen.insert(k);
        }
        eprintln!("+ {}", serde_json::to_string(&new_e)?);
        employers.push(new_e);
        added += 1;
    }

    let out_val = if array_format {
        Value::Array(employers)
    } else {
        json!({ "employers": employers })
    };

    if !args.dry_run {
        if let Some(dir) = args.output.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        std::fs::write(
            &args.output,
            serde_json::to_string_pretty(&out_val).context("serialize")?,
        )
        .with_context(|| format!("write {}", args.output.display()))?;
    }
    eprintln!("Added {added} employer row(s) → {}", args.output.display());
    Ok(())
}
