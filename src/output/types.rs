use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

pub const SCHEMA_VERSION: &str = "rag.v1";

#[derive(Debug, Clone, Serialize, Default)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Envelope {
    pub schema_version: &'static str,
    pub time: DateTime<Utc>,
    pub request_id: Uuid,
    pub op: &'static str,
    pub apply: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

impl Envelope {
    pub fn plan<T: Serialize>(op: &'static str, plan: &T, meta: Option<Meta>) -> Result<Self, serde_json::Error> {
        let plan_val = serde_json::to_value(plan)?;
        Ok(Envelope {
            schema_version: SCHEMA_VERSION,
            time: Utc::now(),
            request_id: Uuid::new_v4(),
            op,
            apply: false,
            plan: Some(plan_val),
            result: None,
            meta,
        })
    }

    pub fn result<T: Serialize>(op: &'static str, result: &T, meta: Option<Meta>) -> Result<Self, serde_json::Error> {
        let res_val = serde_json::to_value(result)?;
        Ok(Envelope {
            schema_version: SCHEMA_VERSION,
            time: Utc::now(),
            request_id: Uuid::new_v4(),
            op,
            apply: true,
            plan: None,
            result: Some(res_val),
            meta,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialize_plan_envelope() {
        let plan = json!({"docs": 5});
        let env = Envelope::plan("Query", &plan, None).expect("to serialize plan");
        let s = serde_json::to_string(&env).unwrap();
        assert!(s.contains("\"schema_version\""));
        assert!(s.contains("\"plan\""));
        assert!(s.contains("\"Query\""));
        assert!(s.contains("\"apply\":false"));
    }

    #[test]
    fn serialize_result_envelope() {
        let result = json!({"total": 3});
        let env = Envelope::result("Query", &result, None).expect("to serialize result");
        let s = serde_json::to_string(&env).unwrap();
        assert!(s.contains("\"result\""));
        assert!(s.contains("\"apply\":true"));
    }
}

