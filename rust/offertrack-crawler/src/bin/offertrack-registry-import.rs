//! Convert CSV rows into `employers.json` for the crawler registry.
//!
//! Header row (columns can be in any order):
//!   company,tier,ats,slug,career_url,enabled
//!
//! - **ats** = `greenhouse` | `lever` | `ashby` | `rss` | `site`
//! - **slug**: Greenhouse board, Lever company slug, Ashby board, RSS feed URL, or site domain
//! - **career_url**: optional; for `site`, used as `start_urls` if only one URL needed
//! - **enabled**: `true` / `false` / empty (default true)

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde_json::{json, Value};

#[derive(Parser, Debug)]
#[command(name = "offertrack-registry-import")]
#[command(about = "CSV → employers.json for offertrack-crawl registry merge.")]
struct Args {
    /// Input CSV path
    csv: PathBuf,
    /// Write JSON here (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

fn col_map(headers: &[String]) -> Result<HashMap<String, usize>> {
    let mut m = HashMap::new();
    for (i, h) in headers.iter().enumerate() {
        m.insert(h.clone(), i);
    }
    let need = ["company", "ats", "slug"];
    for k in need {
        if !m.contains_key(k) {
            return Err(anyhow!("CSV must include column: {k}"));
        }
    }
    Ok(m)
}

fn get(rec: &csv::StringRecord, m: &HashMap<String, usize>, key: &str) -> Option<String> {
    let i = *m.get(key)?;
    rec.get(i).map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn row_to_employer(rec: &csv::StringRecord, m: &HashMap<String, usize>) -> Result<Value> {
    let company = get(rec, m, "company").ok_or_else(|| anyhow!("empty company"))?;
    let ats = get(rec, m, "ats")
        .ok_or_else(|| anyhow!("empty ats"))?
        .to_lowercase();
    let slug = get(rec, m, "slug").ok_or_else(|| anyhow!("empty slug"))?;
    let tier = get(rec, m, "tier");
    let career_url = get(rec, m, "career_url");
    let enabled = match get(rec, m, "enabled").as_deref() {
        Some("false") | Some("0") | Some("no") => false,
        _ => true,
    };

    let o = match ats.as_str() {
        "greenhouse" | "gh" => {
            let mut v = json!({
                "company": company,
                "ats": "greenhouse",
                "board": slug,
                "enabled": enabled,
            });
            if let Some(t) = tier {
                v.as_object_mut().unwrap().insert("tier".to_string(), json!(t));
            }
            if let Some(u) = career_url {
                v.as_object_mut().unwrap().insert("career_url".to_string(), json!(u));
            }
            v
        }
        "lever" => {
            let mut v = json!({
                "company": company,
                "ats": "lever",
                "lever_company": slug,
                "enabled": enabled,
            });
            if let Some(t) = tier {
                v.as_object_mut().unwrap().insert("tier".to_string(), json!(t));
            }
            if let Some(u) = career_url {
                v.as_object_mut().unwrap().insert("career_url".to_string(), json!(u));
            }
            v
        }
        "ashby" => {
            let mut v = json!({
                "company": company,
                "ats": "ashby",
                "board": slug,
                "enabled": enabled,
            });
            if let Some(t) = tier {
                v.as_object_mut().unwrap().insert("tier".to_string(), json!(t));
            }
            if let Some(u) = career_url {
                v.as_object_mut().unwrap().insert("career_url".to_string(), json!(u));
            }
            v
        }
        "rss" | "atom" => {
            let mut v = json!({
                "company": company,
                "ats": "rss",
                "feed_url": slug,
                "enabled": enabled,
            });
            if let Some(t) = tier {
                v.as_object_mut().unwrap().insert("tier".to_string(), json!(t));
            }
            if let Some(u) = career_url {
                v.as_object_mut().unwrap().insert("career_url".to_string(), json!(u));
            }
            v
        }
        "site" | "html" => {
            let url = career_url.ok_or_else(|| anyhow!("site rows need career_url as job listing page"))?;
            let mut v = json!({
                "company": company,
                "ats": "site",
                "domain": slug,
                "start_urls": [url],
                "extractor": "GenericSchemaOrgExtractor",
                "enabled": enabled,
            });
            if let Some(t) = tier {
                v.as_object_mut().unwrap().insert("tier".to_string(), json!(t));
            }
            v
        }
        other => return Err(anyhow!("unknown ats: {other}")),
    };

    Ok(o)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut rdr = csv::Reader::from_path(&args.csv)
        .with_context(|| format!("open {}", args.csv.display()))?;
    let headers: Vec<String> = rdr
        .headers()?
        .iter()
        .map(|h| h.trim().to_lowercase())
        .collect();
    let cmap = col_map(&headers)?;

    let mut employers = Vec::new();
    for (i, result) in rdr.records().enumerate() {
        let rec = result.with_context(|| format!("CSV row {}", i + 2))?;
        if rec.iter().all(|f| f.trim().is_empty()) {
            continue;
        }
        match row_to_employer(&rec, &cmap) {
            Ok(v) => employers.push(v),
            Err(e) => eprintln!("[WARN] row {}: {e}", i + 2),
        }
    }

    let doc = json!({ "employers": employers });
    let text = serde_json::to_string_pretty(&doc)?;
    match args.output {
        Some(path) => {
            std::fs::write(&path, text).with_context(|| format!("write {}", path.display()))?;
        }
        None => println!("{text}"),
    }
    Ok(())
}
