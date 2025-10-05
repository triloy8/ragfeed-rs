use anyhow::Result;
use serde::Serialize;

use crate::output::types::Envelope;
use super::sink;

pub type Meta = crate::output::types::Meta;

pub fn print_plan<T: Serialize>(op: &str, plan: &T, meta: Option<Meta>) -> Result<()> {
    let env = Envelope::plan(op, plan, meta)?;
    sink::current_sink().on_plan(&env)?;
    Ok(())
}

pub fn print_result<T: Serialize>(op: &str, result: &T, meta: Option<Meta>) -> Result<()> {
    let env = Envelope::result(op, result, meta)?;
    sink::current_sink().on_result(&env)?;
    Ok(())
}
