use anyhow::Result;
use sqlx::PgPool;

pub async fn create_new_index(pool: &PgPool, lists: i32) -> Result<()> {
    let sql = format!(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS rag.embedding_vec_ivf_idx_new \
         ON rag.embedding USING ivfflat (vec vector_cosine_ops) WITH (lists = {})",
        lists
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

