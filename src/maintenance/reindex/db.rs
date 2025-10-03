use anyhow::Result;
use sqlx::{Executor, PgPool, Postgres};

pub async fn embedding_count(pool: &PgPool) -> Result<i64> {
    let n = sqlx::query!("SELECT COUNT(*)::bigint AS n FROM rag.embedding")
        .fetch_one(pool)
        .await?
        .n
        .unwrap_or(0);
    Ok(n)
}

pub async fn index_lists(pool: &PgPool, name: &str) -> Result<Option<i32>> {
    let row = sqlx::query!(
        r#"
        SELECT substring(pg_get_indexdef(i.indexrelid) from 'lists = ([0-9]+)') AS lists
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
        WHERE nsp.nspname = 'rag' AND c.relname = $1
        "#,
        name
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|r| r.lists).and_then(|s| s.parse::<i32>().ok()))
}

pub async fn index_exists(pool: &PgPool, name: &str) -> Result<bool> {
    let row = sqlx::query!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'i' AND n.nspname = 'rag' AND c.relname = $1
        ) AS "exists!: bool"
        "#,
        name
    )
    .fetch_one(pool)
    .await?;
    Ok(row.exists)
}

// Preferred: run on a single acquired connection with search_path set, using
// unqualified identifiers to avoid parser issues on some setups.
pub async fn set_search_path<'e, E>(ex: E) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query("SET search_path = rag, public, pg_catalog").execute(ex).await?;
    Ok(())
}

pub async fn create_new_index_ex<'e, E>(ex: E, lists: i32) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS embedding_vec_ivf_idx_new ON embedding USING ivfflat (vec vector_cosine_ops) WITH (lists = {})",
        lists
    );
    sqlx::query(&sql).execute(ex).await?;
    Ok(())
}

pub async fn drop_index_ex<'e, E>(ex: E, name: &str) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!("DROP INDEX CONCURRENTLY IF EXISTS {}", name);
    sqlx::query(&sql).execute(ex).await?;
    Ok(())
}

pub async fn rename_index_ex<'e, E>(ex: E, old: &str, new: &str) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!("ALTER INDEX {} RENAME TO {}", old, new);
    sqlx::query(&sql).execute(ex).await?;
    Ok(())
}

pub async fn reindex_index_ex<'e, E>(ex: E, name: &str) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!("REINDEX INDEX CONCURRENTLY {}", name);
    sqlx::query(&sql).execute(ex).await?;
    Ok(())
}

pub async fn analyze_embedding_ex<'e, E>(ex: E) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query("ANALYZE embedding").execute(ex).await?;
    Ok(())
}
