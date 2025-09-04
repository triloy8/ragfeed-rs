use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::stats::Phase as StatsPhase;
use crate::stats::types::*;

pub async fn feed_stats(pool: &PgPool, feed_id: i32, doc_limit: i64) -> Result<()> {
    let log = telemetry::stats();
    let _s = log.span(&StatsPhase::FeedStats).entered();

    // feed header
    let f = sqlx::query!(
        r#"
        SELECT feed_id, name, url, is_active, added_at
        FROM rag.feed
        WHERE feed_id = $1
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await?;
    log.info(format!("📡 Feed #{}:", f.feed_id));
    log.info(format!("  Name: {}", f.name.clone().unwrap_or_default()));
    log.info(format!("  URL: {}", f.url));
    log.info(format!("  Active: {}", f.is_active.unwrap_or(true)));
    log.info(format!("  Added: {:?}", f.added_at));

    // documents by status within this feed
    log.info("📄 Documents by status:");
    let docs = sqlx::query!(
        r#"
        SELECT COALESCE(status,'') AS status, COUNT(*)::bigint AS cnt
        FROM rag.document
        WHERE feed_id = $1
        GROUP BY status
        ORDER BY status
        "#,
        feed_id
    )
    .fetch_all(pool)
    .await?;
    for r in &docs {
        log.info(format!("  {:10} {}", r.status.clone().unwrap_or_default(), r.cnt.unwrap_or(0)));
    }
    if let Ok(row) = sqlx::query!(
        r#"SELECT MAX(fetched_at) AS last_fetched FROM rag.document WHERE feed_id = $1"#,
        feed_id
    )
    .fetch_one(pool)
    .await
    {
        log.info(format!("  Last fetched: {:?}", row.last_fetched));
    }

    // chunks for this feed
    if let Ok(row) = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS total_chunks,
               AVG(c.token_count)::float8 AS avg_tokens
        FROM rag.chunk c
        JOIN rag.document d ON d.doc_id = c.doc_id
        WHERE d.feed_id = $1
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await
    {
        log.info(format!("🧩 Chunks: total={} avg_tokens={:.1}", row.total_chunks.unwrap_or(0), row.avg_tokens.unwrap_or(0.0)));
    }

    // embedding coverage for this feed
    let cov = sqlx::query!(
        r#"
        SELECT
          (SELECT COUNT(*)::bigint
           FROM rag.chunk c JOIN rag.document d ON d.doc_id = c.doc_id
           WHERE d.feed_id = $1) AS chunks,
          (SELECT COUNT(*)::bigint
           FROM rag.embedding e
           JOIN rag.chunk c ON c.chunk_id = e.chunk_id
           JOIN rag.document d ON d.doc_id = c.doc_id
           WHERE d.feed_id = $1) AS embedded,
          (SELECT MAX(e.created_at)
           FROM rag.embedding e
           JOIN rag.chunk c ON c.chunk_id = e.chunk_id
           JOIN rag.document d ON d.doc_id = c.doc_id
           WHERE d.feed_id = $1) AS last
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await?;

    let chunks = cov.chunks.unwrap_or(0) as f64;
    let embedded = cov.embedded.unwrap_or(0) as f64;
    let pct = if chunks > 0.0 { (embedded / chunks) * 100.0 } else { 0.0 };
    log.info(format!("📈 Coverage: {}/{} ({:.1}%)  last_embedded={:?}", embedded as i64, chunks as i64, pct, cov.last));

    // missing per-feed
    let missing = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS missing
        FROM rag.chunk c
        JOIN rag.document d ON d.doc_id = c.doc_id
        LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
        WHERE d.feed_id = $1 AND e.chunk_id IS NULL
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await?
    .missing
    .unwrap_or(0);
    log.info(format!("   Missing embeddings: {}", missing));

    // model(s) present for this feed
    let feed_models = sqlx::query!(
        r#"
        SELECT e.model, COUNT(*)::bigint AS cnt, MAX(e.created_at) AS last
        FROM rag.embedding e
        JOIN rag.chunk c ON c.chunk_id = e.chunk_id
        JOIN rag.document d ON d.doc_id = c.doc_id
        WHERE d.feed_id = $1
        GROUP BY e.model
        ORDER BY cnt DESC
        "#,
        feed_id
    )
    .fetch_all(pool)
    .await?;
    match feed_models.len() {
        0 => log.info("   Model: (none)"),
        1 => {
            let m = &feed_models[0];
            log.info(format!("   Model: {} ({} vectors, last={:?})", m.model, m.cnt.unwrap_or(0), m.last));
        }
        _ => {
            let mut labels: Vec<String> = Vec::new();
            for m in feed_models.iter().take(3) {
                labels.push(format!("{} ({} )", m.model, m.cnt.unwrap_or(0)));
            }
            if feed_models.len() > 3 { labels.push("...".to_string()); }
            log.info(format!("   Models: {}", labels.join(", ")));
        }
    }

    // top documents in this feed with pending embeddings
    if missing > 0 {
        log.info("   Top docs with pending embeddings:");
        let rows = sqlx::query!(
            r#"
            SELECT d.doc_id, d.source_title, COUNT(*)::bigint AS pending
            FROM rag.chunk c
            JOIN rag.document d ON d.doc_id = c.doc_id
            LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
            WHERE d.feed_id = $1 AND e.chunk_id IS NULL
            GROUP BY d.doc_id, d.source_title
            ORDER BY pending DESC
            LIMIT 10
            "#,
            feed_id
        )
        .fetch_all(pool)
        .await?;
        for r in rows {
            log.info(format!("     {:>6}  doc={}  {}", r.pending.unwrap_or(0), r.doc_id, r.source_title.unwrap_or_default()));
        }
    }

    // latest docs (IDs visible)
    let rows = sqlx::query!(
        r#"
        SELECT doc_id, status, fetched_at, source_title
        FROM rag.document
        WHERE feed_id = $1
        ORDER BY fetched_at DESC NULLS LAST, doc_id DESC
        LIMIT $2
        "#,
        feed_id,
        doc_limit
    )
    .fetch_all(pool)
    .await?;
    if !rows.is_empty() {
        log.info(format!("📜 Docs (latest {}):", rows.len()));
        for r in rows {
            log.info(format!(
                "  doc_id={}  status={}  fetched={:?}  {}",
                r.doc_id,
                r.status.unwrap_or_default(),
                r.fetched_at,
                r.source_title.unwrap_or_default()
            ));
        }
    }

    // JSON envelope
    if telemetry::config::json_mode() {
        let last_fetched = sqlx::query!(
            r#"SELECT MAX(fetched_at) AS last_fetched FROM rag.document WHERE feed_id = $1"#,
            feed_id
        )
        .fetch_one(pool)
        .await?
        .last_fetched;
        let chunks_row = sqlx::query!(
            r#"
            SELECT COUNT(*)::bigint AS total_chunks,
                   AVG(c.token_count)::float8 AS avg_tokens
            FROM rag.chunk c
            JOIN rag.document d ON d.doc_id = c.doc_id
            WHERE d.feed_id = $1
            "#,
            feed_id
        )
        .fetch_one(pool)
        .await?;
        let feed_models_rows = sqlx::query!(
            r#"
            SELECT e.model, COUNT(*)::bigint AS cnt, MAX(e.created_at) AS last
            FROM rag.embedding e
            JOIN rag.chunk c ON c.chunk_id = e.chunk_id
            JOIN rag.document d ON d.doc_id = c.doc_id
            WHERE d.feed_id = $1
            GROUP BY e.model
            ORDER BY cnt DESC
            "#,
            feed_id
        )
        .fetch_all(pool)
        .await?;
        let pending_rows = sqlx::query!(
            r#"
            SELECT d.doc_id, d.source_title, COUNT(*)::bigint AS pending
            FROM rag.chunk c
            JOIN rag.document d ON d.doc_id = c.doc_id
            LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
            WHERE d.feed_id = $1 AND e.chunk_id IS NULL
            GROUP BY d.doc_id, d.source_title
            ORDER BY pending DESC
            LIMIT 10
            "#,
            feed_id
        )
        .fetch_all(pool)
        .await?;
        let latest_docs_rows = sqlx::query!(
            r#"
            SELECT doc_id, status, fetched_at, source_title
            FROM rag.document
            WHERE feed_id = $1
            ORDER BY fetched_at DESC NULLS LAST, doc_id DESC
            LIMIT $2
            "#,
            feed_id,
            doc_limit
        )
        .fetch_all(pool)
        .await?;

        let result = StatsFeedStats {
            feed: StatsFeedMeta { feed_id: f.feed_id, name: f.name, url: f.url, is_active: f.is_active, added_at: f.added_at },
            documents_by_status: docs.into_iter().map(|r| StatsDocStatus { status: r.status.unwrap_or_default(), cnt: r.cnt.unwrap_or(0) }).collect(),
            last_fetched,
            chunks: StatsChunksSummary { total: chunks_row.total_chunks.unwrap_or(0), avg_tokens: chunks_row.avg_tokens.unwrap_or(0.0) },
            coverage: StatsFeedCoverage { chunks: cov.chunks.unwrap_or(0), embedded: cov.embedded.unwrap_or(0), pct, last: cov.last },
            missing,
            models: feed_models_rows.into_iter().map(|m| StatsModelInfo { model: m.model, cnt: m.cnt.unwrap_or(0), last: m.last }).collect(),
            pending_top_docs: pending_rows.into_iter().map(|r| StatsPendingTopDoc { doc_id: r.doc_id, source_title: r.source_title, pending: r.pending.unwrap_or(0) }).collect(),
            latest_docs: latest_docs_rows.into_iter().map(|r| StatsLatestDoc { doc_id: r.doc_id, status: r.status, fetched_at: r.fetched_at, source_title: r.source_title }).collect(),
        };
        log.result(&result)?;
    }

    Ok(())
}
