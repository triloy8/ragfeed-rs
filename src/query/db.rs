use anyhow::Result;
use chrono::{DateTime, Utc};
use pgvector::Vector as PgVector;
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct CandRow {
    pub chunk_id: i64,
    pub doc_id: i64,
    pub title: Option<String>,
    pub preview: Option<String>,
    pub text: Option<String>,
    pub distance: f32,
}

pub struct FetchOpts {
    pub feed: Option<i32>,
    pub since: Option<DateTime<Utc>>,
    pub include_preview: bool,
    pub include_text: bool,
}

pub async fn recommend_probes(pool: &PgPool) -> Result<Option<i32>> {
    let row = sqlx::query!(
        r#"
        SELECT substring(pg_get_indexdef(i.indexrelid) from 'lists = ([0-9]+)') AS lists
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
        WHERE nsp.nspname = 'rag' AND c.relname = 'embedding_vec_ivf_idx'
        "#
    )
    .fetch_optional(pool)
    .await?;
    let lists = row.and_then(|r| r.lists).and_then(|s| s.parse::<i32>().ok());
    Ok(lists.map(|k| (k / 10).max(1)))
}

pub async fn fetch_ann_candidates(
    pool: &PgPool,
    qvec: &[f32],
    top_n: i64,
    opts: &FetchOpts,
) -> Result<Vec<CandRow>> {
    if opts.feed.is_none() && opts.since.is_none() {
        let rows = sqlx::query(
            r#"
            SELECT c.chunk_id, c.doc_id, d.source_title AS title,
                   (e.vec <-> $1) AS distance,
                   CASE WHEN $3 THEN substring(c.text, 1, 300) ELSE NULL END AS preview,
                   CASE WHEN $4 THEN c.text ELSE NULL END AS text
            FROM rag.embedding e
            JOIN rag.chunk c ON c.chunk_id = e.chunk_id
            JOIN rag.document d ON d.doc_id = c.doc_id
            ORDER BY distance ASC
            LIMIT $2
            "#
        )
        .bind(PgVector::from(qvec.to_vec()))
        .bind(top_n)
        .bind(opts.include_preview)
        .bind(opts.include_text)
        .fetch_all(pool)
        .await?;
        let out = rows
            .into_iter()
            .map(|row| CandRow {
                chunk_id: row.get::<i64, _>("chunk_id"),
                doc_id: row.get::<i64, _>("doc_id"),
                title: row.get::<Option<String>, _>("title"),
                preview: row.get::<Option<String>, _>("preview"),
                text: row.get::<Option<String>, _>("text"),
                distance: row.get::<f64, _>("distance") as f32,
            })
            .collect();
        return Ok(out);
    }

    // with filters
    let rows = sqlx::query(
        r#"
        SELECT c.chunk_id, c.doc_id, d.source_title AS title,
               (e.vec <-> $1) AS distance,
               CASE WHEN $5 THEN substring(c.text, 1, 300) ELSE NULL END AS preview,
               CASE WHEN $6 THEN c.text ELSE NULL END AS text
        FROM rag.embedding e
        JOIN rag.chunk c ON c.chunk_id = e.chunk_id
        JOIN rag.document d ON d.doc_id = c.doc_id
        WHERE ($2::int4 IS NULL OR d.feed_id = $2)
          AND ($3::timestamptz IS NULL OR d.fetched_at >= $3)
        ORDER BY distance ASC
        LIMIT $4
        "#
    )
    .bind(PgVector::from(qvec.to_vec()))
    .bind(opts.feed)
    .bind(opts.since)
    .bind(top_n)
    .bind(opts.include_preview)
    .bind(opts.include_text)
    .fetch_all(pool)
    .await?;
    let out = rows
        .into_iter()
        .map(|row| CandRow {
            chunk_id: row.get::<i64, _>("chunk_id"),
            doc_id: row.get::<i64, _>("doc_id"),
            title: row.get::<Option<String>, _>("title"),
            preview: row.get::<Option<String>, _>("preview"),
            text: row.get::<Option<String>, _>("text"),
            distance: row.get::<f64, _>("distance") as f32,
        })
        .collect();
    Ok(out)
}
