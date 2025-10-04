use scraper::{Html, Selector};

pub fn extract(html: &str) -> Option<String> {
    let doc = Html::parse_document(html);

    // 1) Preferred: meta[name="citation_abstract"][content]
    if let Some(s) = extract_meta(&doc, "meta[name=citation_abstract]") {
        let out = normalize_abstract(&s);
        if !out.is_empty() { return Some(out); }
    }

    // 2) Fallback: meta[property="og:description"][content]
    if let Some(s) = extract_meta(&doc, "meta[property=og:description]") {
        let out = normalize_abstract(&s);
        if !out.is_empty() { return Some(out); }
    }

    // 3) Fallback: visible DOM blockquote.abstract
    if let Some(s) = extract_blockquote(&doc) {
        let out = normalize_abstract(&s);
        if !out.is_empty() { return Some(out); }
    }

    // 4) Fallback: .abstract-full or div.abstract (strip label)
    if let Some(s) = extract_abstract_div(&doc) {
        let out = normalize_abstract(&s);
        if !out.is_empty() { return Some(out); }
    }

    None
}

fn extract_meta(doc: &Html, sel_str: &str) -> Option<String> {
    let sel = Selector::parse(sel_str).ok()?;
    let node = doc.select(&sel).next()?;
    let content = node.value().attr("content")?.trim();
    if content.is_empty() { None } else { Some(content.to_string()) }
}

fn extract_blockquote(doc: &Html) -> Option<String> {
    let sel = Selector::parse("blockquote.abstract").ok()?;
    let node = doc.select(&sel).next()?;
    let text = node.text().collect::<String>();
    let text = text.trim();
    if text.is_empty() { return None; }
    Some(text.to_string())
}

fn extract_abstract_div(doc: &Html) -> Option<String> {
    let sel = Selector::parse(".abstract-full, div.abstract").ok()?;
    let node = doc.select(&sel).next()?;
    let text = node.text().collect::<String>();
    let text = text.trim();
    if text.is_empty() { return None; }
    Some(text.to_string())
}

fn normalize_abstract(s: &str) -> String {
    // Trim and strip leading descriptor if present
    let mut out = s.trim().to_string();

    // Exact, case-sensitive prefix removal for "Abstract:" or "Abstract."
    if out.starts_with("Abstract:") {
        out = out["Abstract:".len()..].trim_start().to_string();
    } else if out.starts_with("Abstract.") {
        out = out["Abstract.".len()..].trim_start().to_string();
    }

    collapse_whitespace(&out)
}

fn collapse_whitespace(s: &str) -> String {
    let mut buf = String::with_capacity(s.len());
    let mut in_ws = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !in_ws {
                if !buf.is_empty() { buf.push(' '); }
                in_ws = true;
            }
        } else {
            buf.push(ch);
            in_ws = false;
        }
    }
    buf.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_citation_abstract() {
        let html = r#"
        <html><head>
        <meta name=\"citation_abstract\" content=\"This is the full abstract with details.\" />
        </head><body></body></html>
        "#;
        let got = extract(html).unwrap();
        assert_eq!(got, "This is the full abstract with details.");
    }

    #[test]
    fn meta_og_description() {
        let html = r#"
        <html><head>
        <meta property=\"og:description\" content=\"OG description abstract.\" />
        </head><body></body></html>
        "#;
        let got = extract(html).unwrap();
        assert_eq!(got, "OG description abstract.");
    }

    #[test]
    fn blockquote_abstract_strips_label() {
        let html = r#"
        <html><body>
          <blockquote class=\"abstract\">
            <span class=\"descriptor\">Abstract:</span>
            This is the abstract text across
            multiple   spaces and\nlines.
          </blockquote>
        </body></html>
        "#;
        let got = extract(html).unwrap();
        assert_eq!(got, "This is the abstract text across multiple spaces and lines.");
    }

    #[test]
    fn abstract_full_variant() {
        let html = r#"
        <html><body>
          <div class=\"abstract-full\">
            <span class=\"descriptor\">Abstract.</span>  Full variant here.
          </div>
        </body></html>
        "#;
        let got = extract(html).unwrap();
        assert_eq!(got, "Full variant here.");
    }

    #[test]
    fn none_when_missing() {
        let html = r#"<html><head><title>No abstract</title></head><body><p>Nothing</p></body></html>"#;
        let got = extract(html);
        assert!(got.is_none());
    }
}

