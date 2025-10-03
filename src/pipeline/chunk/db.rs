use anyhow::Result;
use sqlx::PgPool;

pub async fn mark_chunked(pool: &PgPool, doc_id: i64) -> Result<()> {
    sqlx::query!("UPDATE rag.document SET status='chunked' WHERE doc_id=$1", doc_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_chunks(pool: &PgPool, doc_id: i64) -> Result<u64> {
    let res = sqlx::query!("DELETE FROM rag.chunk WHERE doc_id = $1", doc_id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

pub async fn insert_chunk(
    pool: &PgPool,
    doc_id: i64,
    chunk_index: i32,
    text: &str,
    token_count: i32,
) -> Result<i64> {
    let row = sqlx::query!(
        r#"
        INSERT INTO rag.chunk (doc_id, chunk_index, text, token_count, md5)
        VALUES ($1, $2, $3, $4, md5($3))
        ON CONFLICT (doc_id, chunk_index) DO UPDATE
          SET text = EXCLUDED.text,
              token_count = EXCLUDED.token_count,
              md5 = EXCLUDED.md5
        RETURNING chunk_id
        "#,
        doc_id,
        chunk_index,
        text,
        token_count
    )
    .fetch_one(pool)
    .await?;
    Ok(row.chunk_id)
}

