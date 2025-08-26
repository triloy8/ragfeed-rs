use anyhow::Result;
use clap::Args;
use sqlx::PgPool;

#[derive(Args, Debug)]
pub struct InspectCmd {
    #[arg(value_enum)]
    pub entity: InspectEntity, // what to inspect: "doc" or "chunk"
    pub id: i64, // ID of the record
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum InspectEntity {
    #[value(name = "doc")]
    Doc,
    #[value(name = "chunk")]
    Chunk,
}

/// entry point for inspect
pub async fn run(pool: &PgPool, args: InspectCmd) -> Result<()> {
    match args.entity {
        InspectEntity::Doc => inspect_doc(pool, args.id).await?,
        InspectEntity::Chunk => inspect_chunk(pool, args.id).await?,
    }
    Ok(())
}

async fn inspect_doc(pool: &PgPool, id: i64) -> Result<()> {
    let row = sqlx::query!(
        r#"
        SELECT doc_id, feed_id, source_url, source_title, published_at,
               fetched_at, status, error_msg,
               substring(text_clean, 1, 400) AS preview
        FROM rag.document
        WHERE doc_id = $1
        "#,
        id
    )
    .fetch_one(pool)
    .await?;

    println!("📄 Document {}:", row.doc_id);
    println!("  Feed ID: {:?}", row.feed_id);
    println!("  URL: {}", row.source_url);
    println!("  Title: {:?}", row.source_title);
    println!("  Published: {:?}", row.published_at);
    println!("  Fetched: {:?}", row.fetched_at);
    println!("  Status: {:?}", row.status);
    println!("  Error: {:?}", row.error_msg);
    println!("  Preview: {:?}", row.preview);

    Ok(())
}

async fn inspect_chunk(pool: &PgPool, id: i64) -> Result<()> {
    let row = sqlx::query!(
        r#"
        SELECT chunk_id, doc_id, chunk_index, token_count,
               substring(text, 1, 400) AS preview
        FROM rag.chunk
        WHERE chunk_id = $1
        "#,
        id
    )
    .fetch_one(pool)
    .await?;

    println!("🧩 Chunk {} (Doc {:?}):", row.chunk_id, row.doc_id);
    println!("  Index: {:?}", row.chunk_index);
    println!("  Tokens: {:?}", row.token_count);
    println!("  Preview: {:?}", row.preview);

    Ok(())
}
