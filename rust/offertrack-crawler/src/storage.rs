use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use rusqlite::{params, Connection};

use offertrack_crawler::job::{listing_signature_canonical_url, stable_job_id_canonical_url, JobPosting};

pub struct JobStorage {
    conn: Connection,
}

impl JobStorage {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(path).with_context(|| format!("open sqlite {}", path.display()))?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signature TEXT NOT NULL UNIQUE,
                job_id TEXT,
                canonical_job_id TEXT,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT,
                description TEXT,
                url TEXT NOT NULL,
                posted_date TEXT,
                source TEXT,
                raw_json TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_posted_date ON jobs(posted_date);
            CREATE INDEX IF NOT EXISTS idx_jobs_last_seen_at ON jobs(last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON jobs(job_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_canonical ON jobs(canonical_job_id);
            "#,
        )?;

        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(jobs)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .filter_map(|x| x.ok())
            .collect();
        if !cols.iter().any(|c| c == "job_id") {
            conn.execute("ALTER TABLE jobs ADD COLUMN job_id TEXT", [])?;
        }
        if !cols.iter().any(|c| c == "canonical_job_id") {
            conn.execute("ALTER TABLE jobs ADD COLUMN canonical_job_id TEXT", [])?;
        }

        Ok(Self { conn })
    }

    pub fn upsert_jobs(&mut self, jobs: &[JobPosting]) -> Result<Stats> {
        let now = Utc::now().naive_utc().format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
        let mut inserted = 0u32;
        let mut updated = 0u32;

        let tx = self.conn.transaction()?;
        for job in jobs {
            let sig = listing_signature_canonical_url(&job.url);
            let jid = stable_job_id_canonical_url(&job.url);
            let canonical_jid = jid.clone();
            let raw_json = serde_json::to_string(&job.raw).unwrap_or_else(|_| "{}".to_string());
            let posted = job
                .posted_date
                .map(|d| d.format("%Y-%m-%dT%H:%M:%S").to_string());

            let count: i64 = tx.query_row(
                "SELECT COUNT(1) FROM jobs WHERE signature = ?1",
                params![sig],
                |r| r.get(0),
            )?;

            if count > 0 {
                tx.execute(
                    r#"UPDATE jobs SET job_id=?1, canonical_job_id=?2, title=?3, company=?4, location=?5, description=?6,
                    url=?7, posted_date=?8, source=?9, raw_json=?10, last_seen_at=?11 WHERE signature=?12"#,
                    params![
                        jid,
                        canonical_jid,
                        job.title,
                        job.company,
                        job.location,
                        job.description,
                        job.url,
                        posted,
                        job.source,
                        raw_json,
                        &now,
                        sig,
                    ],
                )?;
                updated += 1;
            } else {
                tx.execute(
                    r#"INSERT INTO jobs (signature, job_id, canonical_job_id, title, company, location, description, url,
                    posted_date, source, raw_json, first_seen_at, last_seen_at)
                    VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)"#,
                    params![
                        sig,
                        jid,
                        canonical_jid,
                        job.title,
                        job.company,
                        job.location,
                        job.description,
                        job.url,
                        posted,
                        job.source,
                        raw_json,
                        &now,
                        &now,
                    ],
                )?;
                inserted += 1;
            }
        }
        tx.commit()?;

        Ok(Stats { inserted, updated })
    }

    /// `first_seen_at` for an existing listing (by canonical URL signature), if present.
    pub fn first_seen_for_signature(&self, signature: &str) -> Result<Option<NaiveDateTime>> {
        let mut stmt = self
            .conn
            .prepare("SELECT first_seen_at FROM jobs WHERE signature = ?1 LIMIT 1")?;
        let mut rows = stmt.query_map(params![signature], |r| {
            let s: String = r.get(0)?;
            Ok(s)
        })?;
        if let Some(r) = rows.next() {
            let s = r?;
            if let Ok(dt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f") {
                return Ok(Some(dt));
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
                return Ok(Some(dt));
            }
        }
        Ok(None)
    }

    pub fn first_seen_and_description(&self, job_id: &str) -> Result<Option<(String, Option<String>)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT first_seen_at, description FROM jobs WHERE job_id = ?1")?;
        let mut rows = stmt.query_map(params![job_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        if let Some(r) = rows.next() {
            return Ok(Some(r?));
        }
        Ok(None)
    }
}

#[derive(Debug, Default)]
pub struct Stats {
    pub inserted: u32,
    pub updated: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_job(url: &str) -> JobPosting {
        JobPosting {
            title: "Engineer".into(),
            company: "TestCo".into(),
            url: url.into(),
            job_id: String::new(),
            raw: serde_json::json!({}),
            ..Default::default()
        }
    }

    fn in_memory() -> JobStorage {
        JobStorage::open(std::path::Path::new(":memory:")).unwrap()
    }

    #[test]
    fn open_creates_table() {
        // If open() succeeds without panic, the schema was created correctly.
        let _storage = in_memory();
    }

    #[test]
    fn upsert_inserts_new_job() {
        let mut storage = in_memory();
        let job = make_job("https://example.com/jobs/1");
        let stats = storage.upsert_jobs(&[job]).unwrap();
        assert_eq!(stats.inserted, 1);
        assert_eq!(stats.updated, 0);
    }

    #[test]
    fn upsert_updates_existing_job() {
        let mut storage = in_memory();
        let job = make_job("https://example.com/jobs/2");
        storage.upsert_jobs(&[job.clone()]).unwrap();

        // Second upsert with same URL → update.
        let stats = storage.upsert_jobs(&[job]).unwrap();
        assert_eq!(stats.inserted, 0);
        assert_eq!(stats.updated, 1);
    }

    #[test]
    fn first_seen_for_signature_present_after_insert() {
        let mut storage = in_memory();
        let job = make_job("https://example.com/jobs/3");
        storage.upsert_jobs(&[job]).unwrap();

        let sig = listing_signature_canonical_url("https://example.com/jobs/3");
        let first_seen = storage.first_seen_for_signature(&sig).unwrap();
        assert!(first_seen.is_some());
    }

    #[test]
    fn first_seen_for_signature_absent_for_unknown() {
        let storage = in_memory();
        let result = storage.first_seen_for_signature("nonexistent_signature").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn first_seen_and_description_returns_data() {
        let mut storage = in_memory();
        let mut job = make_job("https://example.com/jobs/4");
        job.description = Some("Great role".into());
        storage.upsert_jobs(&[job]).unwrap();

        let job_id = stable_job_id_canonical_url("https://example.com/jobs/4");
        let result = storage.first_seen_and_description(&job_id).unwrap();
        assert!(result.is_some());
        let (_, desc) = result.unwrap();
        assert_eq!(desc, Some("Great role".into()));
    }

    #[test]
    fn first_seen_and_description_absent_for_unknown_id() {
        let storage = in_memory();
        let result = storage.first_seen_and_description("unknown-id").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn upsert_multiple_jobs_in_one_call() {
        let mut storage = in_memory();
        let jobs: Vec<JobPosting> = (1..=5)
            .map(|i| make_job(&format!("https://example.com/jobs/{i}")))
            .collect();
        let stats = storage.upsert_jobs(&jobs).unwrap();
        assert_eq!(stats.inserted, 5);
        assert_eq!(stats.updated, 0);
    }
}
