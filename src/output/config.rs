use std::env;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
    Mcp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputConfig {
    pub format: OutputFormat,
    pub pretty: bool,
}

impl OutputConfig {
    pub fn from_env() -> Self {
        let format = match env::var("RAG_OUTPUT_FORMAT").ok().as_deref() {
            Some("json") => OutputFormat::Json,
            Some("mcp") => OutputFormat::Mcp,
            _ => OutputFormat::Text,
        };
        let pretty = match env::var("RAG_OUTPUT_PRETTY").ok().as_deref() {
            Some(v) if v.eq_ignore_ascii_case("1") || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes") => true,
            _ => false,
        };
        OutputConfig { format, pretty }
    }
}

