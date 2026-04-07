//! Probe domains for likely career pages and detect common ATS embeds (Greenhouse, Lever, Ashby, Workday).
//! Output CSV rows you can turn into `employers.json` (or merge by hand). Does not crawl full job counts.
//!
//! ```text
//! cargo run -p offertrack-crawler --bin offertrack-career-discover -- \
//!   config/discovery/example-seeds.csv -o out/discovered-careers.csv
//! ```

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(name = "offertrack-career-discover")]
#[command(about = "Discover career URLs + ATS hints from company domains (seed list → CSV).")]
struct Args {
    /// One domain per line (`acme.com`), or CSV with header `company,domain`
    input: PathBuf,
    #[arg(short, long, default_value = "out/discovered-careers.csv")]
    output: PathBuf,
    #[arg(long, default_value_t = 800)]
    delay_ms: u64,
    #[arg(long, default_value_t = 20)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 350_000usize)]
    max_body_bytes: usize,
}

#[derive(Debug, Clone)]
struct Row {
    company: String,
    domain: String,
    career_url: String,
    status: String,
    detected_ats: String,
    slug_or_board: String,
    /// Full listing base URL when regex finds `https://tenant.wdN.myworkdayjobs.com/en-US/Site`
    workday_page_url: String,
    next_step: String,
}

static RE_GH_BOARD: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"boards\.greenhouse\.io/([a-zA-Z0-9_-]+)").expect("regex")
});
static RE_GH_EMBED: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)greenhouse\.io/embed/job_board/js\?for=([a-zA-Z0-9_-]+)"#).expect("regex")
});
static RE_LEVER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"jobs\.lever\.co/([a-z0-9-]+)"#).expect("regex"));
static RE_ASHBY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)"#).expect("regex"));
static RE_WD_LISTING: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"https://([a-z0-9.-]+\.myworkdayjobs\.com)/(en-[A-Za-z]{2})/([^"'\\s<>?#]+)"#).expect("regex")
});
static RE_WORKDAY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"([a-z0-9.-]+\.myworkdayjobs\.com[^"'\\s<>]*)"#).expect("regex")
});
static RE_ICIMS: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"([a-z0-9-]+\.icims\.com[^"'\\s<>]{0,120})"#).expect("regex")
});

fn normalize_domain(raw: &str) -> String {
    let s = raw.trim().to_lowercase();
    let s = s.strip_prefix("https://").unwrap_or(&s);
    let s = s.strip_prefix("http://").unwrap_or(s);
    let s = s.split('/').next().unwrap_or(s);
    s.trim_start_matches("www.").to_string()
}

fn company_from_domain(domain: &str) -> String {
    let base = domain.split('.').next().unwrap_or(domain);
    let mut c = base.chars();
    match c.next() {
        None => domain.to_string(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

fn candidate_career_urls(host: &str) -> Vec<String> {
    let h = host.trim_start_matches("www.");
    let www = format!("www.{h}");
    [
        format!("https://careers.{h}/"),
        format!("https://careers.{h}/jobs"),
        format!("https://jobs.{h}/"),
        format!("https://jobs.{h}/careers"),
        format!("https://{www}/careers"),
        format!("https://{h}/careers"),
        format!("https://{www}/jobs"),
        format!("https://{h}/jobs"),
        format!("https://{www}/careers/overview"),
        format!("https://{h}/careers/overview"),
        format!("https://{www}/about/careers"),
        format!("https://{h}/about/careers"),
        format!("https://{www}/about/careers/"),
        format!("https://{h}/about/careers/"),
        format!("https://{www}/about/careers/applications"),
        format!("https://{h}/about/careers/applications"),
        format!("https://{www}/about/careers/applications/"),
        format!("https://{h}/about/careers/applications/"),
        format!("https://{www}/company/careers"),
        format!("https://{h}/company/careers"),
    ]
    .into_iter()
    .collect()
}

fn detect_stack(html: &str) -> (String, String, String, String) {
    let compact: String = html.chars().filter(|c| !c.is_control() || *c == '\n').take(500_000).collect();

    if let Some(c) = RE_GH_BOARD.captures(&compact) {
        let slug = c.get(1).map(|m| m.as_str()).unwrap_or("").to_string();
        if !slug.is_empty() {
            return (
                "greenhouse".into(),
                slug.clone(),
                format!(r#"{{ "ats":"greenhouse","board":"{slug}","career_url":"<PUT_FINAL_URL>" }}"#),
                String::new(),
            );
        }
    }
    if let Some(c) = RE_GH_EMBED.captures(&compact) {
        let slug = c.get(1).map(|m| m.as_str()).unwrap_or("").to_string();
        if !slug.is_empty() {
            return (
                "greenhouse".into(),
                slug.clone(),
                format!(r#"{{ "ats":"greenhouse","board":"{slug}","career_url":"<PUT_FINAL_URL>" }}"#),
                String::new(),
            );
        }
    }
    if let Some(c) = RE_LEVER.captures(&compact) {
        let slug = c.get(1).map(|m| m.as_str()).unwrap_or("").to_string();
        if !slug.is_empty() {
            return (
                "lever".into(),
                slug.clone(),
                format!(r#"{{ "ats":"lever","lever_company":"{slug}","career_url":"<PUT_FINAL_URL>" }}"#),
                String::new(),
            );
        }
    }
    if let Some(c) = RE_ASHBY.captures(&compact) {
        let slug = c.get(1).map(|m| m.as_str()).unwrap_or("").to_string();
        if !slug.is_empty() {
            return (
                "ashby".into(),
                slug.clone(),
                format!(r#"{{ "ats":"ashby","board":"{slug}","career_url":"<PUT_FINAL_URL>" }}"#),
                String::new(),
            );
        }
    }
    if let Some(c) = RE_WD_LISTING.captures(&compact) {
        let host = c.get(1).map(|m| m.as_str()).unwrap_or("").to_string();
        let loc = c.get(2).map(|m| m.as_str()).unwrap_or("").to_string();
        let site = c.get(3).map(|m| m.as_str()).unwrap_or("").to_string();
        if !host.is_empty() && !loc.is_empty() && !site.is_empty() {
            let page = format!("https://{}/{}/{}", host, loc, site);
            let slug = format!("{}|{}|{}", host, loc, site);
            return (
                "workday".into(),
                slug,
                "Run offertrack-discovery-merge to append employers.json, or copy workday_host/cxs_* manually."
                    .into(),
                page,
            );
        }
    }
    if let Some(c) = RE_WORKDAY.captures(&compact) {
        let frag = c.get(1).map(|m| m.as_str()).unwrap_or("").to_string();
        if !frag.is_empty() {
            return (
                "workday".into(),
                frag.chars().take(120).collect(),
                "Found myworkdayjobs host fragment only; open site in browser, copy full /en-US/Site URL into workday_page_url column by hand, then merge."
                    .into(),
                String::new(),
            );
        }
    }
    if let Some(c) = RE_ICIMS.captures(&compact) {
        let frag = c.get(1).map(|m| m.as_str()).unwrap_or("").to_string();
        if !frag.is_empty() {
            return (
                "icims".into(),
                frag.chars().take(80).collect(),
                "iCIMS not in offertrack-crawl yet; track URL for future connector.".into(),
                String::new(),
            );
        }
    }

    if compact.to_lowercase().contains("amazon.jobs")
        || compact.to_lowercase().contains("www.amazon.jobs")
    {
        return (
            "amazon_jobs".into(),
            "/en".into(),
            "Add registry row ats=amazon_jobs (see employers.json amazon_jobs_us).".into(),
            String::new(),
        );
    }

    (
        "unknown_html".into(),
        String::new(),
        "No ATS regex match: SPA (e.g. Google) or custom stack — use headless/API research or site+Schema crawl.".into(),
        String::new(),
    )
}

async fn fetch_html(
    client: &Client,
    url: &str,
    max_bytes: usize,
) -> Result<(u16, String)> {
    let resp = client.get(url).send().await?;
    let status = resp.status().as_u16();
    let bytes = resp.bytes().await?;
    let slice = if bytes.len() > max_bytes {
        &bytes[..max_bytes]
    } else {
        &bytes[..]
    };
    let text = String::from_utf8_lossy(slice).into_owned();
    Ok((status, text))
}

async fn discover_one(
    client: &Client,
    company: &str,
    domain: &str,
    max_body: usize,
) -> Row {
    let host = normalize_domain(domain);
    if host.is_empty() {
        return Row {
            company: company.to_string(),
            domain: domain.to_string(),
            career_url: String::new(),
            status: "bad_domain".into(),
            detected_ats: String::new(),
            slug_or_board: String::new(),
            workday_page_url: String::new(),
            next_step: "Fix domain in seed file".into(),
        };
    }

    if host == "amazon.jobs" || host.ends_with(".amazon.jobs") {
        return Row {
            company: company.to_string(),
            domain: host.clone(),
            career_url: "https://www.amazon.jobs".into(),
            status: "skip_probe".into(),
            detected_ats: "amazon_jobs".into(),
            slug_or_board: "/en".into(),
            workday_page_url: String::new(),
            next_step: r#"Use registry ats amazon_jobs (already supported)."#.into(),
        };
    }

    let urls = candidate_career_urls(&host);
    for url in urls {
        match fetch_html(client, &url, max_body).await {
            Ok((status, body)) if (200..400).contains(&status) => {
                let ct_html = body.to_lowercase().contains("<html")
                    || body.to_lowercase().contains("<!doctype")
                    || body.contains("</head>");
                if !ct_html && body.len() < 200 {
                    continue;
                }
                let (ats, slug, hint, wd_page) = detect_stack(&body);
                return Row {
                    company: company.to_string(),
                    domain: host.clone(),
                    career_url: url,
                    status: format!("http_{status}"),
                    detected_ats: ats,
                    slug_or_board: slug,
                    workday_page_url: wd_page,
                    next_step: hint,
                };
            }
            Ok((status, _)) => {
                if status == 403 || status == 429 {
                    return Row {
                        company: company.to_string(),
                        domain: host.clone(),
                        career_url: url,
                        status: format!("http_{status}"),
                        detected_ats: String::new(),
                        slug_or_board: String::new(),
                        workday_page_url: String::new(),
                        next_step: "Blocked (403/429); retry later, different network, or manual URL.".into(),
                    };
                }
            }
            Err(_) => continue,
        }
    }

    Row {
        company: company.to_string(),
        domain: host,
        career_url: String::new(),
        status: "not_found".into(),
        detected_ats: String::new(),
        slug_or_board: String::new(),
        workday_page_url: String::new(),
        next_step: "No candidate URL returned usable HTML; add career_url manually or use search.".into(),
    }
}

fn parse_input(path: &PathBuf) -> Result<Vec<(String, String)>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let text = text.trim_start_matches('\u{feff}');
    let first = text.lines().find(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'));
    let is_csv = first
        .map(|l| {
            let t = l.to_lowercase();
            t.contains("domain") && t.contains("company")
        })
        .unwrap_or(false);

    if is_csv {
        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(text.as_bytes());
        let headers = rdr.headers().cloned().context("CSV headers")?;
        let ci = |name: &str| headers.iter().position(|h| h.eq_ignore_ascii_case(name));
        let i_company = ci("company").context("CSV needs company column")?;
        let i_domain = ci("domain").context("CSV needs domain column")?;
        let mut out = Vec::new();
        for rec in rdr.records() {
            let rec = rec?;
            let company = rec.get(i_company).unwrap_or("").trim().to_string();
            let domain = rec.get(i_domain).unwrap_or("").trim().to_string();
            if domain.is_empty() {
                continue;
            }
            let company = if company.is_empty() {
                company_from_domain(&domain)
            } else {
                company
            };
            out.push((company, domain));
        }
        return Ok(out);
    }

    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let domain = normalize_domain(line);
        if domain.is_empty() {
            continue;
        }
        out.push((company_from_domain(&domain), domain));
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let rows_in = parse_input(&args.input)?;

    let client = Client::builder()
        .user_agent("offerTrack-career-discover/1.0 (+https://github.com/)")
        .timeout(Duration::from_secs(args.timeout_secs))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;

    let mut rows_out = Vec::new();
    for (i, (company, domain)) in rows_in.iter().enumerate() {
        if i > 0 && args.delay_ms > 0 {
            sleep(Duration::from_millis(args.delay_ms)).await;
        }
        eprintln!("[{}/{}] {} ({})", i + 1, rows_in.len(), company, domain);
        let row = discover_one(&client, company, domain, args.max_body_bytes).await;
        rows_out.push(row);
    }

    if let Some(dir) = args.output.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let mut wtr = csv::Writer::from_path(&args.output)
        .with_context(|| format!("write {}", args.output.display()))?;
    wtr.write_record([
        "company",
        "domain",
        "career_url",
        "status",
        "detected_ats",
        "slug_or_board",
        "workday_page_url",
        "next_step",
    ])?;
    for r in &rows_out {
        wtr.write_record([
            &r.company,
            &r.domain,
            &r.career_url,
            &r.status,
            &r.detected_ats,
            &r.slug_or_board,
            &r.workday_page_url,
            &r.next_step,
        ])?;
    }
    wtr.flush()?;
    eprintln!("Wrote {} rows → {}", rows_out.len(), args.output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greenhouse_in_html() {
        let html = r#"<script src="https://boards.greenhouse.io/acme/jobs"></script>"#;
        let (ats, slug, _, _) = detect_stack(html);
        assert_eq!(ats, "greenhouse");
        assert_eq!(slug, "acme");
    }

    #[test]
    fn lever_in_html() {
        let html = r#"href="https://jobs.lever.co/widget-corp/apply""#;
        let (ats, slug, _, _) = detect_stack(html);
        assert_eq!(ats, "lever");
        assert_eq!(slug, "widget-corp");
    }
}
