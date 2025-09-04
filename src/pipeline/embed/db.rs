use anyhow::Result;
use pgvector::Vector as PgVector;
use sqlx::PgPool;

pub async fn fetch_chunks(pool: &PgPool, model_tag: &str, force: bool, limit: i64) -> Result<Vec<(i64, String)>> {
    if force {
        let rows = sqlx::query!(
            r#"
            SELECT c.chunk_id, c.text
            FROM rag.chunk c
            ORDER BY c.chunk_id
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(pool)
        .await?;
        return Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect());
    }

    let rows = sqlx::query!(
        r#"
        SELECT c.chunk_id, c.text
        FROM rag.chunk c
        LEFT JOIN rag.embedding e
          ON e.chunk_id = c.chunk_id AND e.model = $1
        WHERE e.chunk_id IS NULL
        ORDER BY c.chunk_id
        LIMIT $2
        "#,
        model_tag,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect())
}

pub async fn fetch_all_chunks(pool: &PgPool, limit: Option<i64>) -> Result<Vec<(i64, String)>> {
    if let Some(limit) = limit {
        let rows = sqlx::query!(
            r#"
            SELECT c.chunk_id, c.text
            FROM rag.chunk c
            ORDER BY c.chunk_id
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(pool)
        .await?;
        return Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect());
    }

    let rows = sqlx::query!(
        r#"
        SELECT c.chunk_id, c.text
        FROM rag.chunk c
        ORDER BY c.chunk_id
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect())
}

pub async fn count_candidates(pool: &PgPool, model_tag: &str, force: bool) -> Result<i64> {
    let n = if force {
        sqlx::query_scalar!(r#"SELECT COUNT(*)::bigint FROM rag.chunk"#)
            .fetch_one(pool)
            .await?
    } else {
        sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint
            FROM rag.chunk c
            LEFT JOIN rag.embedding e
              ON e.chunk_id = c.chunk_id AND e.model = $1
            WHERE e.chunk_id IS NULL
            "#,
            model_tag
        )
        .fetch_one(pool)
        .await?
    };
    Ok(n.unwrap_or(0))
}

pub async fn list_candidate_chunk_ids(pool: &PgPool, model_tag: &str, force: bool, limit: i64) -> Result<Vec<i64>> {
    if limit <= 0 { return Ok(vec![]); }
    if force {
        let rows = sqlx::query!(
            r#"
            SELECT c.chunk_id
            FROM rag.chunk c
            ORDER BY c.chunk_id
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(pool)
        .await?;
        return Ok(rows.into_iter().map(|r| r.chunk_id).collect());
    }

    let rows = sqlx::query!(
        r#"
        SELECT c.chunk_id
        FROM rag.chunk c
        LEFT JOIN rag.embedding e
          ON e.chunk_id = c.chunk_id AND e.model = $1
        WHERE e.chunk_id IS NULL
        ORDER BY c.chunk_id
        LIMIT $2
        "#,
        model_tag,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.chunk_id).collect())
}

pub async fn insert_embedding(pool: &PgPool, chunk_id: i64, model_tag: &str, dim: i32, vec: Vec<f32>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO rag.embedding (chunk_id, model, dim, vec)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (chunk_id) DO UPDATE
          SET model = EXCLUDED.model,
              dim   = EXCLUDED.dim,
              vec   = EXCLUDED.vec
        "#
    )
    .bind(chunk_id)
    .bind(model_tag)
    .bind(dim)
    .bind(PgVector::from(vec))
    .execute(pool)
    .await?;
    Ok(())
}

