use anyhow::Result;
use reqwest::Client;
use bytes::Bytes;

pub async fn fetch_rss(client: &Client, url: &str) -> Result<Bytes> {
    let bytes = client.get(url).send().await?.bytes().await?;
    Ok(bytes)
}

pub async fn fetch_article(client: &Client, url: &str) -> Result<String> {
    let text = client.get(url).send().await?.text().await?;
    Ok(text)
}
