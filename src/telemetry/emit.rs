use anyhow::Result;
use serde::Serialize;

use crate::output::config::{OutputConfig, OutputFormat};
use crate::output::types::Envelope;
use crate::output::Emitter;

pub type Meta = crate::output::types::Meta;

pub fn print_plan<T: Serialize>(op: &str, plan: &T, meta: Option<Meta>) -> Result<()> {
    let env = Envelope::plan(op, plan, meta)?;
    let mut cfg = OutputConfig::from_env();
    if matches!(cfg.format, OutputFormat::Text) && super::config::json_mode() { cfg.format = OutputFormat::Json; }
    let emitter = Emitter::from_env(cfg);
    emitter.emit(&env)?;
    Ok(())
}

pub fn print_result<T: Serialize>(op: &str, result: &T, meta: Option<Meta>) -> Result<()> {
    let env = Envelope::result(op, result, meta)?;
    let mut cfg = OutputConfig::from_env();
    if matches!(cfg.format, OutputFormat::Text) && super::config::json_mode() { cfg.format = OutputFormat::Json; }
    let emitter = Emitter::from_env(cfg);
    emitter.emit(&env)?;
    Ok(())
}
