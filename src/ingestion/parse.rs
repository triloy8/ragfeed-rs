use anyhow::Result;
use chrono::{DateTime, Utc};
use rss::{Channel, Item};
use bytes::Bytes;

pub fn parse_channel(xml: &Bytes) -> Result<Channel> {
    let ch = Channel::read_from(&xml[..])?;
    Ok(ch)
}

pub fn extract_published_at(item: &Item) -> Option<DateTime<Utc>> {
    if let Some(pub_date) = item.pub_date() {
        if let Ok(dt) = DateTime::parse_from_rfc2822(pub_date) { return Some(dt.with_timezone(&Utc)); }
    }
    // Attempt Dublin Core date if available (RFC3339)
    if let Some(dc) = item.dublin_core_ext() {
        if let Some(first) = dc.dates().get(0) {
            if let Ok(dt) = DateTime::parse_from_rfc3339(first) { return Some(dt.with_timezone(&Utc)); }
        }
    }
    None
}
