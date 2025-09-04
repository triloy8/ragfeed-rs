mod generic;

pub fn extract(host: &str, html: &str) -> Option<String> {
    match host {
        // site-specific modules could go here, e.g., "example.com" => sites::example::extract(html)
        _ => generic::scrape_generic(html),
    }
}

