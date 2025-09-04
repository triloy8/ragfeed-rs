use scraper::{Html, Selector};

pub fn scrape_generic(html: &str) -> Option<String> {
    let doc = Html::parse_document(html);

    // try a set of likely article containers first
    let candidates = [
        "article",
        "main",
        "[role=main]",
        "#content",
        "[itemprop=articleBody]",
        ".post-content",
    ];
    for sel in candidates.iter() {
        if let Some(text) = scrape_with_selector(&doc, sel) {
            if text.len() >= 200 { return Some(text); }
        }
    }

    // fallback: collect all paragraphs
    let p_sel = Selector::parse("p").ok()?;
    let mut out: Vec<String> = Vec::new();
    for p in doc.select(&p_sel) {
        let t = p.text().collect::<String>();
        let s = normalize(&t);
        if !s.is_empty() { out.push(s); }
    }
    let joined = out.join("\n");
    if joined.trim().is_empty() { None } else { Some(joined) }
}

fn scrape_with_selector(doc: &Html, selector: &str) -> Option<String> {
    let sel = Selector::parse(selector).ok()?;
    let node = doc.select(&sel).next()?;
    let text = node.text().collect::<String>();
    let s = normalize(&text);
    if s.trim().is_empty() { None } else { Some(s) }
}

fn normalize(s: &str) -> String {
    // collapse whitespace and trim lines
    let mut out = String::new();
    for line in s.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        if !out.is_empty() { out.push('\n'); }
        out.push_str(trimmed);
    }
    out
}

