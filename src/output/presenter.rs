use std::io::{self, Write};
use serde_json::json;

use super::config::{OutputConfig, OutputFormat};
use super::types::Envelope;

pub trait Presenter: Send + Sync {
    fn emit(&self, env: &Envelope, w: &mut dyn Write) -> io::Result<()>;
}

pub struct JsonPresenter { pub pretty: bool }
impl Presenter for JsonPresenter {
    fn emit(&self, env: &Envelope, w: &mut dyn Write) -> io::Result<()> {
        if self.pretty { serde_json::to_writer_pretty(w, env).map_err(to_io)? } else { serde_json::to_writer(w, env).map_err(to_io)? }
        writeln!(w)
    }
}

pub struct TextPresenter { pub pretty: bool }
impl Presenter for TextPresenter {
    fn emit(&self, env: &Envelope, w: &mut dyn Write) -> io::Result<()> {
        if env.apply {
            writeln!(w, "Result: {}", env.op)?;
            if self.pretty {
                if let Some(res) = &env.result { serde_json::to_writer_pretty(w, res).map_err(to_io)?; writeln!(w)?; }
            }
        } else {
            writeln!(w, "Plan: {}", env.op)?;
            if self.pretty {
                if let Some(plan) = &env.plan { serde_json::to_writer_pretty(w, plan).map_err(to_io)?; writeln!(w)?; }
            }
        }
        Ok(())
    }
}

pub struct McpPresenter { pub pretty: bool }
impl Presenter for McpPresenter {
    fn emit(&self, env: &Envelope, w: &mut dyn Write) -> io::Result<()> {
        if env.apply {
            let payload = json!({
                "jsonrpc": "2.0",
                "method": "notifications/result",
                "params": {
                    "schema_version": env.schema_version,
                    "request_id": env.request_id,
                    "op": env.op,
                    "result": env.result
                }
            });
            if self.pretty { serde_json::to_writer_pretty(w, &payload).map_err(to_io)?; } else { serde_json::to_writer(w, &payload).map_err(to_io)?; }
            writeln!(w)
        } else {
            let payload = json!({
                "jsonrpc": "2.0",
                "method": "notifications/plan",
                "params": {
                    "schema_version": env.schema_version,
                    "request_id": env.request_id,
                    "op": env.op,
                    "plan": env.plan
                }
            });
            if self.pretty { serde_json::to_writer_pretty(w, &payload).map_err(to_io)?; } else { serde_json::to_writer(w, &payload).map_err(to_io)?; }
            writeln!(w)
        }
    }
}

pub struct Emitter {
    presenter: Box<dyn Presenter>,
}

impl Emitter {
    pub fn from_env(cfg: OutputConfig) -> Self {
        let presenter: Box<dyn Presenter> = match cfg.format {
            OutputFormat::Json => Box::new(JsonPresenter { pretty: cfg.pretty }),
            OutputFormat::Mcp => Box::new(McpPresenter { pretty: cfg.pretty }),
            OutputFormat::Text => Box::new(TextPresenter { pretty: cfg.pretty }),
        };
        Emitter { presenter }
    }

    pub fn emit(&self, env: &Envelope) -> io::Result<()> {
        let mut out = io::stdout();
        self.presenter.emit(env, &mut out)?;
        out.flush()
    }
}

fn to_io(e: serde_json::Error) -> io::Error { io::Error::new(io::ErrorKind::Other, e) }

