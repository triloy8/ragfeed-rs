use anyhow::Result;
use sqlx::PgPool;

#[derive(clap::Args)]
pub struct ChunkCmd {
    #[arg(long)] since: Option<String>,
    #[arg(long)] doc_id: Option<i64>,
    #[arg(long, default_value_t = 350)] tokens_target: usize,
    #[arg(long, default_value_t = 80)] overlap: usize,
    #[arg(long, default_value_t = 24)] max_chunks_per_doc: usize,
}

pub async fn run(pool: &PgPool, args: ChunkCmd) -> Result<()> {
    // 1. Select docs needing chunking
    // 2. Split into chunks using your tokenizer
    // 3. Insert into rag.chunk
    // 4. Update rag.document.status = 'chunked'

    Ok(())
}
