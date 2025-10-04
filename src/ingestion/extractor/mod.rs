mod generic;
mod arxiv;

pub fn extract(host: &str, html: &str) -> Option<String> {
    match host {
        // arXiv-specific: only handle host arxiv.org (feeds guarantee /abs/<id>)
        "arxiv.org" => arxiv::extract(html),
        // site-specific modules could go here, e.g., "example.com" => sites::example::extract(html)
        _ => generic::scrape_generic(html),
    }
}
