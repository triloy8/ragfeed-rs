use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::stats::Phase as StatsPhase;
use crate::stats::types::*;

pub async fn summary(pool: &PgPool) -> Result<()> {
    let log = telemetry::stats();
    let _s = log.span(&StatsPhase::Summary).entered();

    // feeds listing
    log.info("📡 Feeds:");
    let feeds = sqlx::query!(
        r#"
        SELECT feed_id, name, url, is_active, added_at
        FROM rag.feed
        ORDER BY feed_id
        "#
    )
    .fetch_all(pool)
    .await?;
    for f in &feeds {
        log.info(format!(
            "  #{}  active={}  name={}  url={}  added_at={:?}",
            f.feed_id,
            f.is_active.unwrap_or(true),
            f.name.clone().unwrap_or_default(),
            f.url,
            f.added_at
        ));
    }

    // documents by status
    log.info("📄 Documents by status:");
    let docs = sqlx::query!(
        r#"
        SELECT COALESCE(status,'') AS status, COUNT(*)::bigint AS cnt
        FROM rag.document
        GROUP BY status
        ORDER BY status
        "#
    )
    .fetch_all(pool)
    .await?;
    for r in &docs {
        log.info(format!("  {:10} {}", r.status.clone().unwrap_or_default(), r.cnt.unwrap_or(0)));
    }

    if let Ok(row) = sqlx::query!("SELECT MAX(fetched_at) AS last_fetched FROM rag.document")
        .fetch_one(pool)
        .await
    {
        log.info(format!("  Last fetched: {:?}", row.last_fetched));
    }

    // chunks summary
    if let Ok(row) = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS total_chunks,
               AVG(token_count)::float8 AS avg_tokens
        FROM rag.chunk
        "#
    )
    .fetch_one(pool)
    .await
    {
        log.info(format!("🧩 Chunks: total={} avg_tokens={:.1}", row.total_chunks.unwrap_or(0), row.avg_tokens.unwrap_or(0.0)));
    }

    // embeddings summary
    let emb_total = sqlx::query!("SELECT COUNT(*)::bigint AS total FROM rag.embedding")
        .fetch_one(pool)
        .await?
        .total
        .unwrap_or(0);
    log.info(format!("🔢 Embeddings: total={}", emb_total));

    // model metadata
    let models = sqlx::query!(
        r#"
        SELECT model, COUNT(*)::bigint AS cnt, MAX(created_at) AS last
        FROM rag.embedding
        GROUP BY model
        ORDER BY cnt DESC
        "#
    )
    .fetch_all(pool)
    .await?;
    match models.len() {
        0 => log.info("   Model: (none)"),
        1 => {
            let m = &models[0];
            log.info(format!("   Model: {} ({} vectors, last={:?})", m.model, m.cnt.unwrap_or(0), m.last));
        }
        _ => {
            let mut labels: Vec<String> = Vec::new();
            for m in models.iter().take(3) { labels.push(format!("{} ({} )", m.model, m.cnt.unwrap_or(0))); }
            if models.len() > 3 { labels.push("...".to_string()); }
            log.info(format!("   Models: {}", labels.join(", ")));
        }
    }

    // index metadata
    let idx_row = sqlx::query!(
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
    let lists_val: Option<i32> = idx_row.as_ref().and_then(|r| r.lists.as_ref()).and_then(|s| s.parse::<i32>().ok());

    let size_row = sqlx::query!(r#"SELECT pg_size_pretty(pg_relation_size('rag.embedding_vec_ivf_idx')) AS size"#)
        .fetch_optional(pool)
        .await?;
    let size_pretty = size_row.and_then(|r| r.size);

    let analyze_row = sqlx::query!(
        r#"
        SELECT last_analyze
        FROM pg_stat_user_tables
        WHERE schemaname = 'rag' AND relname = 'embedding'
        "#
    )
    .fetch_optional(pool)
    .await?;

    let mut line = String::from("ivfflat");
    if let Some(k) = lists_val { line.push_str(&format!(" lists={}", k)); }
    if let Some(s) = size_pretty.as_deref() { line.push_str(&format!(" size={}", s)); }
    if let Some(ts) = analyze_row.as_ref().and_then(|r| r.last_analyze.as_ref()) { line.push_str(&format!(" last_analyze={:?}", ts)); }
    log.info(format!("🧭 Index: {}", line));

    // coverage
    let totals = sqlx::query!(
        r#"
        SELECT
          (SELECT COUNT(*)::bigint FROM rag.chunk) AS chunks,
          (SELECT COUNT(*)::bigint FROM rag.embedding) AS embedded
        "#
    )
    .fetch_one(pool)
    .await?;

    let chunks = totals.chunks.unwrap_or(0) as f64;
    let embedded = totals.embedded.unwrap_or(0) as f64;
    let pct = if chunks > 0.0 { (embedded / chunks) * 100.0 } else { 0.0 };
    log.info(format!("📈 Coverage: {}/{} ({:.1}%)", embedded as i64, chunks as i64, pct));

    // missing count
    let missing = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS missing
        FROM rag.chunk c
        LEFT JOIN rag.embedding e
          ON e.chunk_id = c.chunk_id
        WHERE e.chunk_id IS NULL
        "#
    )
    .fetch_one(pool)
    .await?
    .missing
    .unwrap_or(0);
    log.info(format!("   Missing embeddings: {}", missing));

    // JSON envelope
    if telemetry::config::json_mode() {
        let feeds_out: Vec<StatsFeedRow> = feeds
            .into_iter()
            .map(|f| StatsFeedRow { feed_id: f.feed_id, name: f.name, url: f.url, is_active: f.is_active, added_at: f.added_at })
            .collect();
        let docs_out: Vec<StatsDocStatus> = docs
            .into_iter()
            .map(|r| StatsDocStatus { status: r.status.unwrap_or_default(), cnt: r.cnt.unwrap_or(0) })
            .collect();
        let last_fetched = sqlx::query!("SELECT MAX(fetched_at) AS last_fetched FROM rag.document")
            .fetch_one(pool)
            .await?
            .last_fetched;
        let chunk_row = sqlx::query!(r#"SELECT COUNT(*)::bigint AS total, AVG(token_count)::float8 AS avg FROM rag.chunk"#)
            .fetch_one(pool)
            .await?;
        let chunks_out = StatsChunksSummary { total: chunk_row.total.unwrap_or(0), avg_tokens: chunk_row.avg.unwrap_or(0.0) };
        let models_out: Vec<StatsModelInfo> = models
            .into_iter()
            .map(|m| StatsModelInfo { model: m.model, cnt: m.cnt.unwrap_or(0), last: m.last })
            .collect();
        let embeddings_out = StatsEmbeddings { total: emb_total, models: models_out };
        let index_out = StatsIndexMeta { lists: lists_val, size_pretty, last_analyze: analyze_row.and_then(|r| r.last_analyze) };
        let totals2 = sqlx::query!(
            r#"SELECT (SELECT COUNT(*)::bigint FROM rag.chunk) AS chunks,
                      (SELECT COUNT(*)::bigint FROM rag.embedding) AS embedded"#
        )
        .fetch_one(pool)
        .await?;
        let chunks_i64 = totals2.chunks.unwrap_or(0);
        let embedded_i64 = totals2.embedded.unwrap_or(0);
        let pct2 = if chunks_i64 > 0 { (embedded_i64 as f64 / chunks_i64 as f64) * 100.0 } else { 0.0 };
        let coverage_out = StatsCoverage { chunks: chunks_i64, embedded: embedded_i64, pct: pct2, missing };
        let result = StatsSummary { feeds: feeds_out, documents_by_status: docs_out, last_fetched, chunks: chunks_out, embeddings: embeddings_out, index: index_out, coverage: coverage_out };
        log.result(&result)?;
    }

    Ok(())
}
