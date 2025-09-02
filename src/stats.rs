use anyhow::Result;
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;

use crate::out::{self};
use crate::out::stats::Phase as StatsPhase;

#[derive(Args, Debug)]
pub struct StatsCmd {
    #[arg(long)] feed: Option<i32>, // show a scoped view for this feed ID
    #[arg(long)] doc: Option<i64>, // show a snapshot for this document ID (replaces `inspect doc <id>`)
    #[arg(long)] chunk: Option<i64>, // show a snapshot for this chunk ID (replaces `inspect chunk <id>`)

    /// Number of docs to list in --feed view (default: 10)
    #[arg(long, default_value_t = 10)]
    pub doc_limit: i64,

    /// Number of chunks to list in --doc view (default: 10)
    #[arg(long, default_value_t = 10)]
    pub chunk_limit: i64,
}

pub async fn run(pool: &PgPool, args: StatsCmd) -> Result<()> {
    let log = out::stats();
    let _g = log
        .root_span_kv([
            ("feed", format!("{:?}", args.feed)),
            ("doc", format!("{:?}", args.doc)),
            ("chunk", format!("{:?}", args.chunk)),
            ("doc_limit", args.doc_limit.to_string()),
            ("chunk_limit", args.chunk_limit.to_string()),
        ])
        .entered();
    // snapshot modes fully replace the old `inspect` command
    if let Some(id) = args.doc {
        return snapshot_doc(pool, id, args.chunk_limit).await;
    }
    if let Some(id) = args.chunk {
        return snapshot_chunk(pool, id).await;
    }
    if let Some(feed_id) = args.feed {
        return feed_stats(pool, feed_id, args.doc_limit).await;
    }

    let _s = log.span(&StatsPhase::Summary).entered();
    // feeds listing (verbose)
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

    // model metadata (single-model expected; show all if multiple exist)
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
            // show top 3 succinctly
            let mut labels: Vec<String> = Vec::new();
            for m in models.iter().take(3) {
                labels.push(format!("{} ({} )", m.model, m.cnt.unwrap_or(0)));
            }
            if models.len() > 3 { labels.push("...".to_string()); }
            log.info(format!("   Models: {}", labels.join(", ")));
        }
    }

    // index metadata: ivfflat lists, size, last_analyze
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
    let lists_val: Option<i32> = idx_row
        .as_ref()
        .and_then(|r| r.lists.as_ref())
        .and_then(|s| s.parse::<i32>().ok());

    let size_row = sqlx::query!(
        r#"SELECT pg_size_pretty(pg_relation_size('rag.embedding_vec_ivf_idx')) AS size"#
    )
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

    // coverage (single-model assumption): embedded vs total chunks
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

    // missing count (no model filter)
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

    // JSON envelope for summary, if requested
    if out::json_mode() {
        #[derive(Serialize)]
        struct FeedRow { feed_id: i32, name: Option<String>, url: String, is_active: Option<bool>, added_at: Option<chrono::DateTime<chrono::Utc>> }
        #[derive(Serialize)]
        struct DocStatus { status: String, cnt: i64 }
        #[derive(Serialize)]
        struct ChunksSummary { total: i64, avg_tokens: f64 }
        #[derive(Serialize)]
        struct ModelInfo { model: String, cnt: i64, last: Option<chrono::DateTime<chrono::Utc>> }
        #[derive(Serialize)]
        struct Embeddings { total: i64, models: Vec<ModelInfo> }
        #[derive(Serialize)]
        struct IndexMeta { lists: Option<i32>, size_pretty: Option<String>, last_analyze: Option<chrono::DateTime<chrono::Utc>> }
        #[derive(Serialize)]
        struct Coverage { chunks: i64, embedded: i64, pct: f64, missing: i64 }
        #[derive(Serialize)]
        struct SummaryResult {
            feeds: Vec<FeedRow>,
            documents_by_status: Vec<DocStatus>,
            last_fetched: Option<chrono::DateTime<chrono::Utc>>,
            chunks: ChunksSummary,
            embeddings: Embeddings,
            index: IndexMeta,
            coverage: Coverage,
        }
        let feeds_out: Vec<FeedRow> = feeds
            .into_iter()
            .map(|f| FeedRow { feed_id: f.feed_id, name: f.name, url: f.url, is_active: f.is_active, added_at: f.added_at })
            .collect();
        let docs_out: Vec<DocStatus> = docs
            .into_iter()
            .map(|r| DocStatus { status: r.status.unwrap_or_default(), cnt: r.cnt.unwrap_or(0) })
            .collect();
        let last_fetched = sqlx::query!("SELECT MAX(fetched_at) AS last_fetched FROM rag.document")
            .fetch_one(pool)
            .await?
            .last_fetched;
        let chunk_row = sqlx::query!(
            r#"SELECT COUNT(*)::bigint AS total, AVG(token_count)::float8 AS avg FROM rag.chunk"#
        )
        .fetch_one(pool)
        .await?;
        let chunks_out = ChunksSummary { total: chunk_row.total.unwrap_or(0), avg_tokens: chunk_row.avg.unwrap_or(0.0) };
        let models_out: Vec<ModelInfo> = models
            .into_iter()
            .map(|m| ModelInfo { model: m.model, cnt: m.cnt.unwrap_or(0), last: m.last })
            .collect();
        let embeddings_out = Embeddings { total: emb_total, models: models_out };
        let index_out = IndexMeta { lists: lists_val, size_pretty, last_analyze: analyze_row.and_then(|r| r.last_analyze) };
        let totals2 = sqlx::query!(
            r#"SELECT (SELECT COUNT(*)::bigint FROM rag.chunk) AS chunks,
                      (SELECT COUNT(*)::bigint FROM rag.embedding) AS embedded"#
        )
        .fetch_one(pool)
        .await?;
        let chunks_i64 = totals2.chunks.unwrap_or(0);
        let embedded_i64 = totals2.embedded.unwrap_or(0);
        let pct2 = if chunks_i64 > 0 { (embedded_i64 as f64 / chunks_i64 as f64) * 100.0 } else { 0.0 };
        let coverage_out = Coverage { chunks: chunks_i64, embedded: embedded_i64, pct: pct2, missing };
        let result = SummaryResult { feeds: feeds_out, documents_by_status: docs_out, last_fetched, chunks: chunks_out, embeddings: embeddings_out, index: index_out, coverage: coverage_out };
        log.result(&result)?;
    }

    Ok(())
}

async fn snapshot_doc(pool: &PgPool, id: i64, chunk_limit: i64) -> Result<()> {
    let log = out::stats();
    let _s = log.span(&StatsPhase::DocSnapshot).entered();
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

    log.info(format!("📄 Document {}:", row.doc_id));
    log.info(format!("  Feed ID: {:?}", row.feed_id));
    log.info(format!("  URL: {}", row.source_url));
    log.info(format!("  Title: {:?}", row.source_title));
    log.info(format!("  Published: {:?}", row.published_at));
    log.info(format!("  Fetched: {:?}", row.fetched_at));
    log.info(format!("  Status: {:?}", row.status));
    log.info(format!("  Error: {:?}", row.error_msg));
    log.info(format!("  Preview: {:?}", row.preview));

    // list chunks (IDs visible)
    let rows = sqlx::query!(
        r#"
        SELECT chunk_id, chunk_index, token_count
        FROM rag.chunk
        WHERE doc_id = $1
        ORDER BY chunk_index ASC
        LIMIT $2
        "#,
        id,
        chunk_limit
    )
    .fetch_all(pool)
    .await?;
    if !rows.is_empty() {
        log.info(format!("  Chunks (first {}):", rows.len()));
        for r in &rows {
            log.info(format!("    chunk_id={}  idx={}  tokens={}", r.chunk_id, r.chunk_index.unwrap_or(0), r.token_count.unwrap_or(0)));
        }
    }

    if out::json_mode() {
        #[derive(Serialize)]
        struct DocInfo {
            doc_id: i64,
            feed_id: Option<i32>,
            source_url: String,
            source_title: Option<String>,
            published_at: Option<chrono::DateTime<chrono::Utc>>,
            fetched_at: Option<chrono::DateTime<chrono::Utc>>,
            status: Option<String>,
            error_msg: Option<String>,
            preview: Option<String>,
        }
        #[derive(Serialize)]
        struct ChunkInfo { chunk_id: i64, chunk_index: Option<i32>, token_count: Option<i32> }
        #[derive(Serialize)]
        struct DocSnapshot { doc: DocInfo, chunks: Vec<ChunkInfo> }
        let doc = DocInfo {
            doc_id: row.doc_id,
            feed_id: row.feed_id,
            source_url: row.source_url,
            source_title: row.source_title,
            published_at: row.published_at,
            fetched_at: row.fetched_at,
            status: row.status,
            error_msg: row.error_msg,
            preview: row.preview,
        };
        let chunks: Vec<ChunkInfo> = rows
            .into_iter()
            .map(|r| ChunkInfo { chunk_id: r.chunk_id, chunk_index: r.chunk_index, token_count: r.token_count })
            .collect();
        log.result(&DocSnapshot { doc, chunks })?;
    }

    Ok(())
}

async fn feed_stats(pool: &PgPool, feed_id: i32, doc_limit: i64) -> Result<()> {
    let log = out::stats();
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

    if out::json_mode() {
        #[derive(Serialize)]
        struct FeedMeta { feed_id: i32, name: Option<String>, url: String, is_active: Option<bool>, added_at: Option<chrono::DateTime<chrono::Utc>> }
        #[derive(Serialize)]
        struct DocStatus { status: String, cnt: i64 }
        #[derive(Serialize)]
        struct ChunksSummary { total_chunks: i64, avg_tokens: f64 }
        #[derive(Serialize)]
        struct Coverage { chunks: i64, embedded: i64, pct: f64, last: Option<chrono::DateTime<chrono::Utc>> }
        #[derive(Serialize)]
        struct ModelInfo { model: String, cnt: i64, last: Option<chrono::DateTime<chrono::Utc>> }
        #[derive(Serialize)]
        struct PendingTopDoc { doc_id: i64, source_title: Option<String>, pending: i64 }
        #[derive(Serialize)]
        struct LatestDoc { doc_id: i64, status: Option<String>, fetched_at: Option<chrono::DateTime<chrono::Utc>>, source_title: Option<String> }
        #[derive(Serialize)]
        struct FeedStatsResult {
            feed: FeedMeta,
            documents_by_status: Vec<DocStatus>,
            last_fetched: Option<chrono::DateTime<chrono::Utc>>,
            chunks: ChunksSummary,
            coverage: Coverage,
            missing: i64,
            models: Vec<ModelInfo>,
            pending_top_docs: Vec<PendingTopDoc>,
            latest_docs: Vec<LatestDoc>,
        }
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
        let latest_docs = sqlx::query!(
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
        let result = FeedStatsResult {
            feed: FeedMeta { feed_id: f.feed_id, name: f.name, url: f.url, is_active: f.is_active, added_at: f.added_at },
            documents_by_status: docs.into_iter().map(|r| DocStatus { status: r.status.unwrap_or_default(), cnt: r.cnt.unwrap_or(0) }).collect(),
            last_fetched,
            chunks: ChunksSummary { total_chunks: chunks_row.total_chunks.unwrap_or(0), avg_tokens: chunks_row.avg_tokens.unwrap_or(0.0) },
            coverage: Coverage { chunks: chunks as i64, embedded: embedded as i64, pct, last: cov.last },
            missing,
            models: feed_models.into_iter().map(|m| ModelInfo { model: m.model, cnt: m.cnt.unwrap_or(0), last: m.last }).collect(),
            pending_top_docs: pending_rows.into_iter().map(|r| PendingTopDoc { doc_id: r.doc_id, source_title: r.source_title, pending: r.pending.unwrap_or(0) }).collect(),
            latest_docs: latest_docs.into_iter().map(|r| LatestDoc { doc_id: r.doc_id, status: r.status, fetched_at: r.fetched_at, source_title: r.source_title }).collect(),
        };
        log.result(&result)?;
    }

    Ok(())
}

async fn snapshot_chunk(pool: &PgPool, id: i64) -> Result<()> {
    let log = out::stats();
    let _s = log.span(&StatsPhase::ChunkSnapshot).entered();
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

    log.info(format!("🧩 Chunk {} (Doc {:?}):", row.chunk_id, row.doc_id));
    log.info(format!("  Index: {:?}", row.chunk_index));
    log.info(format!("  Tokens: {:?}", row.token_count));
    log.info(format!("  Preview: {:?}", row.preview));

    if out::json_mode() {
        #[derive(Serialize)]
        struct ChunkSnap { chunk_id: i64, doc_id: Option<i64>, chunk_index: Option<i32>, token_count: Option<i32>, preview: Option<String> }
        log.result(&ChunkSnap {
            chunk_id: row.chunk_id,
            doc_id: row.doc_id,
            chunk_index: row.chunk_index,
            token_count: row.token_count,
            preview: row.preview,
        })?;
    }

    Ok(())
}
