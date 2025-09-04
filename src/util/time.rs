use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDate, Utc};

// Parse a window string like "2d", "YYYY-MM-DD", or RFC3339 into a UTC timestamp.
// Returns Some(ts) on success; None if unparseable.
pub fn parse_window_str(s: &str) -> Option<DateTime<Utc>> {
    // "2d" -> now - 2 days
    if let Some(stripped) = s.strip_suffix('d') {
        if let Ok(days) = stripped.parse::<i64>() {
            if days > 0 {
                return Some(Utc::now() - Duration::days(days));
            }
        }
    }
    // "YYYY-MM-DD"
    if let Ok(nd) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(dt) = nd.and_hms_opt(0, 0, 0) {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
        }
    }
    // RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    None
}

// Helper for Option<String> inputs used by CLI flags like --since
pub fn parse_since_opt(since: &Option<String>) -> Result<Option<DateTime<Utc>>> {
    let Some(s) = since.as_ref() else { return Ok(None) };
    Ok(parse_window_str(s))
}

// Specific name used by gc for older_than/cutoff parsing
pub fn parse_cutoff_str(s: &str) -> Option<DateTime<Utc>> {
    parse_window_str(s)
}

