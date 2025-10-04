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

    let fmt_layer = fmt::layer().with_target(false);
    let builder = tracing_subscriber::registry().with(filter);

    match std::env::var("RAG_LOG_FORMAT").as_deref() {
        Ok("json") => {
            let _ = builder.with(fmt_layer.json().flatten_event(true)).try_init();
        }
        _ => {
            // human-friendly compact text
            let _ = builder.with(fmt_layer.compact()).try_init();
        }
    }
}
