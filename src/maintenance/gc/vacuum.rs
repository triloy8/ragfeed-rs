use anyhow::Result;
use sqlx::PgPool;

use crate::out;

pub async fn drop_temp_indexes(pool: &PgPool) -> Result<()> {
    sqlx::query("DROP INDEX CONCURRENTLY IF EXISTS rag.embedding_vec_ivf_idx_new")
        .execute(pool)
        .await?;
    let log = out::gc();
    log.info("🧼 Dropped rag.embedding_vec_ivf_idx_new (if existed)");
    Ok(())
}

pub async fn analyze_tables(pool: &PgPool) -> Result<()> {
    sqlx::query("ANALYZE rag.document")
        .execute(pool)
        .await?;
    sqlx::query("ANALYZE rag.chunk")
        .execute(pool)
        .await?;
    sqlx::query("ANALYZE rag.embedding")
        .execute(pool)
        .await?;
    let log = out::gc();
    log.info("📊 Analyzed rag.document, rag.chunk, rag.embedding");
    Ok(())
}

pub async fn vacuum_full(pool: &PgPool) -> Result<()> {
    // warning: FULL takes exclusive locks; use only when asked
    sqlx::query("VACUUM (ANALYZE, FULL) rag.document")
        .execute(pool)
        .await?;
    sqlx::query("VACUUM (ANALYZE, FULL) rag.chunk")
        .execute(pool)
        .await?;
    sqlx::query("VACUUM (ANALYZE, FULL) rag.embedding")
        .execute(pool)
        .await?;
    let log = out::gc();
    log.info("🧽 Vacuumed (FULL) rag.document, rag.chunk, rag.embedding");
    Ok(())
}

