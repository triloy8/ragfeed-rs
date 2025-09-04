use anyhow::Result;
use serde::Serialize;
use serde_json::json;
use std::io::{self, Write};

#[derive(Serialize)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

pub fn print_plan<T: Serialize>(op: &str, plan: &T, meta: Option<Meta>) -> Result<()> {
    let env = json!({ "op": op, "apply": false, "plan": plan, "meta": meta });
    let mut out = io::stdout();
    serde_json::to_writer(&mut out, &env)?;
    writeln!(&mut out)?;
    Ok(())
}

pub fn print_result<T: Serialize>(op: &str, result: &T, meta: Option<Meta>) -> Result<()> {
    let env = json!({ "op": op, "apply": true, "result": result, "meta": meta });
    let mut out = io::stdout();
    serde_json::to_writer(&mut out, &env)?;
    writeln!(&mut out)?;
    Ok(())
}

