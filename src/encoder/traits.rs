use anyhow::Result;

pub trait Embedder {
    fn embed_queries(&mut self, queries: &[String]) -> Result<Vec<Vec<f32>>>;
    fn embed_passages(&mut self, passages: &[String]) -> Result<Vec<Vec<f32>>>;
    fn embed_query(&mut self, query: &str) -> Result<Vec<f32>>;
}

