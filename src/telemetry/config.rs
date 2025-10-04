pub fn logs_are_json() -> bool {
    matches!(std::env::var("RAG_LOG_FORMAT").as_deref(), Ok("json"))
}

/// Initialize tracing/logging according to RUST_LOG and RAG_LOG_FORMAT.
/// - Defaults to `info` if `RUST_LOG` is unset
/// - Supports `RAG_LOG_FORMAT=json` for JSON logs (stderr)
pub fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    use tracing_subscriber::prelude::*; // for .with()

    // Default filter if RUST_LOG unset
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let builder = tracing_subscriber::registry().with(filter);

    match std::env::var("RAG_LOG_FORMAT").as_deref() {
        Ok("json") => {
            let json_layer = fmt::layer()
                .with_target(false)
                .with_writer(std::io::stderr)
                .json()
                .flatten_event(true);
            let _ = builder.with(json_layer).try_init();
        }
        _ => {
            // human-friendly compact text
            let text_layer = fmt::layer()
                .with_target(false)
                .with_writer(std::io::stderr)
                .compact();
            let _ = builder.with(text_layer).try_init();
        }
    }
}
