use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client as HttpClient, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_MODEL: &str = "gpt-4o-mini";
const DEFAULT_TEMPERATURE: f32 = 0.2;
const DEFAULT_TOP_P: f32 = 1.0;
const DEFAULT_TIMEOUT_SECS: u64 = 60;

#[derive(Clone, Debug)]
pub struct OpenAiClientConfig {
    pub api_key: Option<String>,
    pub base_url: String,
    pub default_model: String,
    pub default_temperature: f32,
    pub default_top_p: f32,
    pub timeout: Duration,
}

impl Default for OpenAiClientConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("OPENAI_API_KEY").ok(),
            base_url: DEFAULT_BASE_URL.to_string(),
            default_model: std::env::var("OPENAI_MODEL")
                .unwrap_or_else(|_| DEFAULT_MODEL.to_string()),
            default_temperature: DEFAULT_TEMPERATURE,
            default_top_p: DEFAULT_TOP_P,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        }
    }
}

impl OpenAiClientConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(base) = std::env::var("OPENAI_BASE_URL") {
            cfg.base_url = base;
        }
        if let Ok(temp) = std::env::var("OPENAI_TEMPERATURE") {
            if let Ok(parsed) = temp.parse::<f32>() {
                cfg.default_temperature = parsed;
            }
        }
        if let Ok(top_p) = std::env::var("OPENAI_TOP_P") {
            if let Ok(parsed) = top_p.parse::<f32>() {
                cfg.default_top_p = parsed;
            }
        }
        if let Ok(timeout) = std::env::var("OPENAI_TIMEOUT_SECS") {
            if let Ok(parsed) = timeout.parse::<u64>() {
                cfg.timeout = Duration::from_secs(parsed);
            }
        }
        cfg
    }
}

#[derive(Clone)]
pub struct OpenAiClient {
    http: HttpClient,
    cfg: OpenAiClientConfig,
}

impl OpenAiClient {
    pub fn new(cfg: OpenAiClientConfig) -> Result<Self, OpenAiError> {
        let http = HttpClient::builder()
            .timeout(cfg.timeout)
            .build()
            .map_err(OpenAiError::http)?;
        Ok(Self { http, cfg })
    }

    fn resolve_api_key(&self) -> Result<String, OpenAiError> {
        if let Some(key) = &self.cfg.api_key {
            return Ok(key.clone());
        }
        std::env::var("OPENAI_API_KEY").map_err(|_| OpenAiError::MissingApiKey)
    }

    fn endpoint(&self) -> String {
        format!(
            "{}/chat/completions",
            self.cfg.base_url.trim_end_matches('/')
        )
    }

    fn build_api_request(&self, req: &ChatCompletionRequest) -> ApiChatCompletionRequest {
        ApiChatCompletionRequest {
            model: req
                .model
                .clone()
                .unwrap_or_else(|| self.cfg.default_model.clone()),
            temperature: req
                .temperature
                .unwrap_or(self.cfg.default_temperature),
            top_p: req.top_p.unwrap_or(self.cfg.default_top_p),
            max_tokens: req.max_tokens,
            messages: req
                .messages
                .iter()
                .map(|m| ApiChatMessage {
                    role: m.role.as_api_str().to_string(),
                    content: Some(m.content.clone()),
                })
                .collect(),
        }
    }
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, OpenAiError>;
}

#[async_trait]
impl LlmClient for OpenAiClient {
    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, OpenAiError> {
        if request.messages.is_empty() {
            return Err(OpenAiError::EmptyMessages);
        }

        let api_key = self.resolve_api_key()?;
        let api_request = self.build_api_request(&request);
        let endpoint = self.endpoint();

        let response = self
            .http
            .post(endpoint)
            .bearer_auth(api_key)
            .json(&api_request)
            .send()
            .await
            .map_err(OpenAiError::from_reqwest)?;

        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(OpenAiError::from_reqwest)?;

        if !status.is_success() {
            let api_err = serde_json::from_slice::<ApiErrorEnvelope>(&bytes)
                .ok()
                .map(|env| env.error);
            return Err(OpenAiError::Api {
                status,
                error: api_err.unwrap_or_default(),
            });
        }

        let parsed: ApiChatCompletionResponse =
            serde_json::from_slice(&bytes).map_err(OpenAiError::Decode)?;
        let raw: Value =
            serde_json::from_slice(&bytes).map_err(OpenAiError::Decode)?;

        let content = parsed
            .choices
            .iter()
            .find_map(|choice| choice.message.content.clone())
            .unwrap_or_default();

        Ok(ChatCompletionResponse {
            content,
            raw,
            usage: parsed.usage.map(|usage| UsageMetrics {
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens,
            }),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatCompletionRequest {
    pub model: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ChatRole {
    System,
    User,
    Assistant,
}

impl ChatRole {
    fn as_api_str(&self) -> &'static str {
        match self {
            ChatRole::System => "system",
            ChatRole::User => "user",
            ChatRole::Assistant => "assistant",
        }
    }
}

impl ChatMessage {
    pub fn new(role: ChatRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatCompletionResponse {
    pub content: String,
    pub raw: Value,
    pub usage: Option<UsageMetrics>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UsageMetrics {
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub total_tokens: Option<u32>,
}

#[derive(Debug)]
pub enum OpenAiError {
    MissingApiKey,
    EmptyMessages,
    Http(reqwest::Error),
    Timeout,
    Api {
        status: StatusCode,
        error: ApiErrorBody,
    },
    MockQueueEmpty,
    Decode(serde_json::Error),
}

impl OpenAiError {
    fn http(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            OpenAiError::Timeout
        } else {
            OpenAiError::Http(err)
        }
    }

    fn from_reqwest(err: reqwest::Error) -> Self {
        Self::http(err)
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            OpenAiError::Timeout => true,
            OpenAiError::Http(_) => true,
            OpenAiError::Api { status, .. } => status.is_server_error(),
            OpenAiError::MissingApiKey
            | OpenAiError::EmptyMessages
            | OpenAiError::MockQueueEmpty
            | OpenAiError::Decode(_) => false,
        }
    }
}

impl std::fmt::Display for OpenAiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenAiError::MissingApiKey => write!(f, "OPENAI_API_KEY is not set"),
            OpenAiError::EmptyMessages => {
                write!(f, "chat completion requires at least one message")
            }
            OpenAiError::Http(err) => write!(f, "http error: {err}"),
            OpenAiError::Timeout => write!(f, "request timed out"),
            OpenAiError::Api { status, error } => {
                write!(f, "api error {status}: {}", error.message)
            }
            OpenAiError::MockQueueEmpty => {
                write!(f, "mock client response queue is empty")
            }
            OpenAiError::Decode(err) => write!(f, "decode error: {err}"),
        }
    }
}

impl std::error::Error for OpenAiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OpenAiError::Http(err) => Some(err),
            OpenAiError::Decode(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub message: String,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub param: Option<String>,
    #[serde(default)]
    pub code: Option<String>,
}

impl Default for ApiErrorBody {
    fn default() -> Self {
        Self {
            message: "unknown error".to_string(),
            r#type: None,
            param: None,
            code: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiErrorEnvelope {
    error: ApiErrorBody,
}

#[derive(Debug, Default)]
pub struct MockClient {
    responses: Mutex<VecDeque<Result<ChatCompletionResponse, OpenAiError>>>,
    calls: Mutex<Vec<ChatCompletionRequest>>,
}

impl MockClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_response(
        &self,
        resp: Result<ChatCompletionResponse, OpenAiError>,
    ) {
        self.responses.lock().unwrap().push_back(resp);
    }

    pub fn calls(&self) -> Vec<ChatCompletionRequest> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl LlmClient for MockClient {
    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, OpenAiError> {
        self.calls.lock().unwrap().push(request.clone());
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(OpenAiError::MockQueueEmpty))
    }
}

#[derive(Debug, Clone, Serialize)]
struct ApiChatCompletionRequest {
    model: String,
    temperature: f32,
    top_p: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    messages: Vec<ApiChatMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiChatMessage {
    role: String,
    content: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiChatCompletionResponse {
    choices: Vec<ApiChatChoice>,
    usage: Option<ApiUsage>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiChatChoice {
    message: ApiChatMessage,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiUsage {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    total_tokens: Option<u32>,
}

#[cfg(test)]
impl OpenAiClient {
    pub(crate) fn build_request_for_tests(
        &self,
        req: &ChatCompletionRequest,
    ) -> ApiChatCompletionRequest {
        self.build_api_request(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: None,
            messages: vec![
                ChatMessage::new(ChatRole::System, "You are helpful."),
                ChatMessage::new(ChatRole::User, "Hello"),
            ],
            max_tokens: Some(64),
            temperature: Some(0.3),
            top_p: Some(0.9),
        }
    }

    #[test]
    fn build_request_serializes_messages() {
        let client = OpenAiClient::new(OpenAiClientConfig {
            api_key: Some("test".into()),
            base_url: DEFAULT_BASE_URL.to_string(),
            default_model: "gpt-4o-mini".into(),
            default_temperature: 0.2,
            default_top_p: 1.0,
            timeout: Duration::from_secs(30),
        })
        .unwrap();

        let request = sample_request();
        let api_request = client.build_request_for_tests(&request);
        let value = serde_json::to_value(&api_request).unwrap();

        assert_eq!(value["model"], "gpt-4o-mini");
        assert_eq!(value["messages"][0]["role"], "system");
        assert_eq!(value["messages"][1]["content"], "Hello");
        assert_eq!(value["temperature"], 0.3);
        assert_eq!(value["top_p"], 0.9);
        assert_eq!(value["max_tokens"], 64);
    }

    #[tokio::test]
    async fn mock_client_returns_enqueued_response() {
        let mock = MockClient::new();
        let response = ChatCompletionResponse {
            content: "hi".into(),
            raw: Value::String("raw".into()),
            usage: None,
        };
        mock.push_response(Ok(response.clone()));

        let req = sample_request();
        let out = mock.chat_completion(req.clone()).await.unwrap();

        assert_eq!(out.content, "hi");
        assert_eq!(mock.calls().len(), 1);
        assert_eq!(mock.calls()[0], req);
    }

    #[test]
    fn api_error_display_includes_status() {
        let err = OpenAiError::Api {
            status: StatusCode::BAD_REQUEST,
            error: ApiErrorBody {
                message: "bad request".into(),
                r#type: Some("invalid_request_error".into()),
                param: None,
                code: None,
            },
        };

        assert_eq!(
            format!("{err}"),
            "api error 400 Bad Request: bad request"
        );
        assert!(!err.is_retryable());
    }
}
