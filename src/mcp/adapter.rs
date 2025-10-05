#![cfg(feature = "mcp-server")]

use std::sync::Mutex;

use anyhow::Result;
use serde_json::{json, Value};

use rmcp::model::{LoggingLevel, LoggingMessageNotificationParam};

use crate::output::types::{Envelope, Meta};
use crate::telemetry::{EventPayload, OutputSink};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpContentKind {
    Json,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpContentBlock {
    pub kind: McpContentKind,
    pub value: Value,
}

impl McpContentBlock {
    pub fn json(value: Value) -> Self {
        Self { kind: McpContentKind::Json, value }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpMessageKind {
    Plan,
    Result,
    Event(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CapturedMessage {
    pub kind: McpMessageKind,
    pub op: String,
    pub run_id: Option<String>,
    pub schema_version: &'static str,
    pub block: McpContentBlock,
}

#[derive(Default, Debug)]
pub struct McpSink {
    messages: Mutex<Vec<CapturedMessage>>,
}

impl McpSink {
    pub fn new() -> Self { Self::default() }

    pub fn drain(&self) -> Vec<CapturedMessage> {
        let mut guard = self.messages.lock().expect("captured messages mutex poisoned");
        std::mem::take(&mut *guard)
    }

    fn capture(&self, message: CapturedMessage) {
        let mut guard = self.messages.lock().expect("captured messages mutex poisoned");
        guard.push(message);
    }

    fn run_id(meta: &Option<Meta>) -> Option<String> {
        meta.as_ref().and_then(|m| m.run_id.clone())
    }

    fn plan_payload(env: &Envelope) -> Value {
        let plan = env.plan.clone().unwrap_or(Value::Null);
        json!({
            "schema_version": env.schema_version,
            "op": env.op,
            "plan": plan,
            "run_id": env.meta.as_ref().and_then(|m| m.run_id.clone()),
        })
    }

    fn result_payload(env: &Envelope) -> Value {
        let result = env.result.clone().unwrap_or(Value::Null);
        json!({
            "schema_version": env.schema_version,
            "op": env.op,
            "result": result,
            "run_id": env.meta.as_ref().and_then(|m| m.run_id.clone()),
        })
    }
}

impl CapturedMessage {
    pub fn into_logging_notification(self) -> LoggingMessageNotificationParam {
        let CapturedMessage { kind, op, run_id, block, .. } = self;

        let (level, kind_label, event_logger) = match &kind {
            McpMessageKind::Plan => (LoggingLevel::Info, "plan", None),
            McpMessageKind::Result => (LoggingLevel::Notice, "result", None),
            McpMessageKind::Event(label) => (LoggingLevel::Debug, label.as_str(), Some(format!("rag::event::{label}"))),
        };

        let logger = if !op.is_empty() {
            Some(format!("rag::{}", op))
        } else {
            event_logger.or_else(|| Some("rag".to_string()))
        };

        let mut data = block.value;
        match &mut data {
            Value::Object(map) => {
                map.insert(
                    "message_kind".to_string(),
                    Value::String(kind_label.to_string()),
                );
                if let Some(run_id) = run_id.clone() {
                    map.entry("run_id".to_string()).or_insert(Value::String(run_id));
                }
            }
            _ => {
                data = json!({
                    "message_kind": kind_label,
                    "payload": data,
                    "run_id": run_id,
                });
            }
        }

        LoggingMessageNotificationParam {
            level,
            logger,
            data,
        }
    }
}

impl OutputSink for McpSink {
    fn on_plan(&self, env: &Envelope) -> Result<()> {
        let payload = McpContentBlock::json(Self::plan_payload(env));
        let message = CapturedMessage {
            kind: McpMessageKind::Plan,
            op: env.op.clone(),
            run_id: Self::run_id(&env.meta),
            schema_version: env.schema_version,
            block: payload,
        };
        self.capture(message);
        Ok(())
    }

    fn on_result(&self, env: &Envelope) -> Result<()> {
        let payload = McpContentBlock::json(Self::result_payload(env));
        let message = CapturedMessage {
            kind: McpMessageKind::Result,
            op: env.op.clone(),
            run_id: Self::run_id(&env.meta),
            schema_version: env.schema_version,
            block: payload,
        };
        self.capture(message);
        Ok(())
    }

    fn on_event(&self, event: &EventPayload<'_>) -> Result<()> {
        let block = McpContentBlock::json(json!({ "kind": event.kind }));
        let message = CapturedMessage {
            kind: McpMessageKind::Event(event.kind.to_string()),
            op: String::new(),
            run_id: None,
            schema_version: crate::output::types::SCHEMA_VERSION,
            block,
        };
        self.capture(message);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::types::{Envelope, Meta};
    use serde_json::json;

    #[test]
    fn captures_plan_envelope_as_json_block() {
        let sink = McpSink::new();
        let meta = Meta { run_id: Some("run-123".to_string()), ..Default::default() };
        let env = Envelope::plan("feed.add", &json!({"url": "https://example.com"}), Some(meta)).unwrap();
        sink.on_plan(&env).unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1);
        let message = &captured[0];
        assert!(matches!(message.kind, McpMessageKind::Plan));
        assert_eq!(message.op, "feed.add");
        assert_eq!(message.run_id.as_deref(), Some("run-123"));
        assert_eq!(message.block.kind, McpContentKind::Json);
        assert_eq!(message.block.value["op"], "feed.add");
        assert_eq!(message.block.value["plan"]["url"], "https://example.com");
    }

    #[test]
    fn captures_result_envelope_as_json_block() {
        let sink = McpSink::new();
        let meta = Meta { run_id: None, duration_ms: Some(42) };
        let env = Envelope::result("feed.add", &json!({"inserted": true}), Some(meta)).unwrap();
        sink.on_result(&env).unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1);
        let message = &captured[0];
        assert!(matches!(message.kind, McpMessageKind::Result));
        assert_eq!(message.op, "feed.add");
        assert_eq!(message.block.value["result"]["inserted"], true);
    }
}
