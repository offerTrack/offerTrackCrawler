use chrono::{NaiveDateTime, Utc};

pub fn parse_date(value: Option<&str>) -> Option<NaiveDateTime> {
    let s = value?.trim();
    if s.is_empty() {
        return None;
    }

    // Date-only: NaiveDateTime cannot parse these directly; convert via NaiveDate.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d.and_hms_opt(0, 0, 0);
    }

    let formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%a, %d %b %Y %H:%M:%S %z",
    ];

    for fmt in formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(dt);
        }
    }

    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.naive_utc());
    }

    if let Ok(dt) = s.parse::<chrono::DateTime<Utc>>() {
        return Some(dt.naive_utc());
    }

    NaiveDateTime::parse_from_str(&s.replace('Z', ""), "%Y-%m-%dT%H:%M:%S").ok()
}

/// Amazon.jobs `posted_date` strings like `April 7, 2026` (no time).
pub fn parse_english_month_day_year(s: &str) -> Option<NaiveDateTime> {
    use chrono::NaiveDate;
    let s = s.trim();
    let (md, yrest) = s.rsplit_once(',')?;
    let year: i32 = yrest.trim().parse().ok()?;
    let mut parts = md.trim().split_whitespace();
    let month_name = parts.next()?.to_lowercase();
    let day: u32 = parts.next()?.parse().ok()?;
    let month = match month_name.as_str() {
        "january" => 1,
        "february" => 2,
        "march" => 3,
        "april" => 4,
        "may" => 5,
        "june" => 6,
        "july" => 7,
        "august" => 8,
        "september" => 9,
        "october" => 10,
        "november" => 11,
        "december" => 12,
        _ => return None,
    };
    NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(0, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn english_month_day_year() {
        let dt = parse_english_month_day_year("April 7, 2026").unwrap();
        assert_eq!(dt.date().month(), 4);
        assert_eq!(dt.date().day(), 7);
        assert_eq!(dt.date().year(), 2026);
    }

    #[test]
    fn english_all_months() {
        let months = [
            ("January", 1), ("February", 2), ("March", 3), ("April", 4),
            ("May", 5), ("June", 6), ("July", 7), ("August", 8),
            ("September", 9), ("October", 10), ("November", 11), ("December", 12),
        ];
        for (name, num) in months {
            let s = format!("{name} 1, 2026");
            let dt = parse_english_month_day_year(&s).unwrap();
            assert_eq!(dt.date().month(), num, "failed for {name}");
        }
    }

    #[test]
    fn english_invalid_month_returns_none() {
        assert!(parse_english_month_day_year("Octember 1, 2026").is_none());
    }

    #[test]
    fn english_missing_comma_returns_none() {
        assert!(parse_english_month_day_year("April 7 2026").is_none());
    }

    #[test]
    fn parse_date_none_input() {
        assert!(parse_date(None).is_none());
    }

    #[test]
    fn parse_date_empty_string() {
        assert!(parse_date(Some("")).is_none());
        assert!(parse_date(Some("   ")).is_none());
    }

    #[test]
    fn parse_date_iso_date_only() {
        let dt = parse_date(Some("2026-03-15")).unwrap();
        assert_eq!(dt.date().year(), 2026);
        assert_eq!(dt.date().month(), 3);
        assert_eq!(dt.date().day(), 15);
    }

    #[test]
    fn parse_date_iso_datetime() {
        let dt = parse_date(Some("2026-03-15T12:30:00")).unwrap();
        assert_eq!(dt.date().year(), 2026);
    }

    #[test]
    fn parse_date_iso_datetime_z() {
        let dt = parse_date(Some("2026-03-15T12:30:00Z")).unwrap();
        assert_eq!(dt.date().year(), 2026);
    }

    #[test]
    fn parse_date_rfc3339_with_offset() {
        let dt = parse_date(Some("2026-03-15T12:30:00+05:30")).unwrap();
        assert_eq!(dt.date().year(), 2026);
    }

    #[test]
    fn parse_date_space_separated() {
        let dt = parse_date(Some("2026-03-15 12:30:00")).unwrap();
        assert_eq!(dt.date().year(), 2026);
    }

    #[test]
    fn parse_date_unrecognised_returns_none() {
        assert!(parse_date(Some("not-a-date")).is_none());
    }
}
