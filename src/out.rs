use serde::Serialize;
use serde_json::json;
use std::io::{self, Write};
use std::sync::OnceLock;

static JSON_MODE: OnceLock<bool> = OnceLock::new();

pub fn set_json_mode(v: bool) {
    let _ = JSON_MODE.set(v);
}

pub fn json_mode() -> bool {
    *JSON_MODE.get().unwrap_or(&false)
}

pub fn logs_are_json() -> bool {
    matches!(std::env::var("RAG_LOG_FORMAT").as_deref(), Ok("json"))
}

#[derive(Serialize)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

pub fn print_plan<T: Serialize>(op: &str, plan: &T, meta: Option<Meta>) -> anyhow::Result<()> {
    let env = json!({
        "op": op,
        "apply": false,
        "plan": plan,
        "meta": meta
    });
    // Write to stdout as a single line
    let mut out = io::stdout();
    serde_json::to_writer(&mut out, &env)?;
    writeln!(&mut out)?;
    Ok(())
}

pub fn print_result<T: Serialize>(op: &str, result: &T, meta: Option<Meta>) -> anyhow::Result<()> {
    let env = json!({
        "op": op,
        "apply": true,
        "result": result,
        "meta": meta
    });
    let mut out = io::stdout();
    serde_json::to_writer(&mut out, &env)?;
    writeln!(&mut out)?;
    Ok(())
}

pub fn log_info(msg: &str) {
    let _ = writeln!(io::stderr(), "{msg}");
}

pub fn log_warn(msg: &str) {
    let _ = writeln!(io::stderr(), "{msg}");
}

pub fn log_error(msg: &str) {
    let _ = writeln!(io::stderr(), "{msg}");
}
