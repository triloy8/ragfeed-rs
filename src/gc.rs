use anyhow::Result;
use clap::Args;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum VacuumMode {
    #[value(name = "analyze")] Analyze,
    #[value(name = "full")] Full,
    #[value(name = "off")] Off,
}

#[derive(Args, Debug)]
pub struct GcCmd {
    #[arg(long, default_value_t = false)] apply: bool, // default is plan-only; use --apply to execute
    #[arg(long, default_value = "30d")] older_than: String, // consider items older than this window as stale (e.g., "30d" or "2025-01-01")
    #[arg(long, default_value_t = 10_000)] max: i64, // max rows to delete per batch
    #[arg(long)] feed: Option<i32>, // scope operations to a single feed ID
    #[arg(long, value_enum, default_value_t = VacuumMode::Analyze)] vacuum: VacuumMode, // vacuum/Analyze mode after GC
    #[arg(long, default_value_t = false)] drop_temp_indexes: bool, // drop stray temporary reindex artifacts (embedding_vec_ivf_idx_new)
    #[arg(long, default_value_t = false)] fix_status: bool, // normalize document.status based on chunk/embedding presence
}

pub async fn run(pool: &PgPool, args: GcCmd) -> Result<()> {
    let cutoff = parse_cutoff(&args.older_than);
    let execute = args.apply;
    let mode = if execute { "apply" } else { "plan" };

    println!(
        "📝 GC plan — mode={} feed={:?} cutoff={:?} max={} vacuum={:?} fix_status={} drop_temp_indexes={}",
        mode,
        args.feed,
        cutoff,
        args.max,
        args.vacuum,
        args.fix_status,
        args.drop_temp_indexes
    );
    if !execute { println!("   Use --apply to execute."); }

    // orphan chunks
    let orphan_chunks = count_orphan_chunks(pool, args.feed).await?;
    println!("🧱 Orphan chunks: {}", orphan_chunks);
    if execute && orphan_chunks > 0 { delete_orphan_chunks(pool, args.feed, args.max).await?; }

    // orphan embeddings (note: FK should prevent these; no feed scope possible)
    let orphan_emb = count_orphan_embeddings(pool).await?;
    println!("🧬 Orphan embeddings: {}", orphan_emb);
    if execute && orphan_emb > 0 { delete_orphan_embeddings(pool, args.max).await?; }

    // error docs older than cutoff
    let err_docs = count_error_docs(pool, cutoff, args.feed).await?;
    println!("⚠️  Error docs (> cutoff): {}", err_docs);
    if execute && err_docs > 0 { delete_error_docs(pool, cutoff, args.feed, args.max).await?; }

    // never-chunked docs older than cutoff
    let stale_docs = count_never_chunked_docs(pool, cutoff, args.feed).await?;
    println!("⏳ Never-chunked docs (> cutoff): {}", stale_docs);
    if execute && stale_docs > 0 { delete_never_chunked_docs(pool, cutoff, args.feed, args.max).await?; }

    // bad chunks
    let bad_chunks = count_bad_chunks(pool, args.feed).await?;
    println!("🧹 Bad chunks (empty/≤0 tokens): {}", bad_chunks);
    if execute && bad_chunks > 0 { delete_bad_chunks(pool, args.feed, args.max).await?; }

    // fix status
    if args.fix_status {
        if execute { fix_statuses(pool, args.feed).await?; }
        else { println!("🔎 Would normalize document.status based on chunk/embedding presence"); }
    }

    // drop temp indexes
    if args.drop_temp_indexes {
        if execute { drop_temp_indexes(pool).await?; }
        else { println!("🔎 Would DROP INDEX CONCURRENTLY rag.embedding_vec_ivf_idx_new if exists"); }
    }

    // vacuum/Analyze
    match args.vacuum {
        VacuumMode::Off => {}
        VacuumMode::Analyze => {
            if execute { analyze_tables(pool).await?; }
            else { println!("🔎 Would ANALYZE rag.document, rag.chunk, rag.embedding"); }
        }
        VacuumMode::Full => {
            if execute { vacuum_full(pool).await?; }
            else { println!("🔎 Would VACUUM (ANALYZE, FULL) rag.document, rag.chunk, rag.embedding"); }
        }
    }

    Ok(())
}

fn parse_cutoff(s: &str) -> Option<DateTime<Utc>> {
    // "30d" → now - 30 days
    if let Some(stripped) = s.strip_suffix('d') {
        if let Ok(days) = stripped.parse::<i64>() {
            if days > 0 { return Some(Utc::now() - Duration::days(days)); }
        }
    }
    // "YYYY-MM-DD"
    if let Ok(nd) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = nd.and_hms_opt(0,0,0).unwrap();
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }
    // RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    None
}

async fn count_orphan_embeddings(pool: &PgPool) -> Result<i64> {
    let n = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)::bigint
        FROM rag.embedding e
        WHERE NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.chunk_id = e.chunk_id)
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(n.unwrap_or(0))
}

async fn delete_orphan_embeddings(pool: &PgPool, max: i64) -> Result<()> {
    loop {
        let res = sqlx::query!(
            r#"
            DELETE FROM rag.embedding e
            WHERE e.ctid IN (
                SELECT e2.ctid
                FROM rag.embedding e2
                WHERE NOT EXISTS (
                    SELECT 1 FROM rag.chunk c WHERE c.chunk_id = e2.chunk_id
                )
                LIMIT $1
            )
            "#,
            max
        )
        .execute(pool)
        .await?;
        if res.rows_affected() == 0 { break; }
        println!("  🗑️ Deleted {} orphan embeddings", res.rows_affected());
    }
    Ok(())
}

async fn count_orphan_chunks(pool: &PgPool, feed: Option<i32>) -> Result<i64> {
    let n = match feed {
        None => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint
            FROM rag.chunk c
            WHERE NOT EXISTS (SELECT 1 FROM rag.document d WHERE d.doc_id = c.doc_id)
            "#
        )
        .fetch_one(pool)
        .await?,
        Some(fid) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint
            FROM rag.chunk c
            WHERE NOT EXISTS (SELECT 1 FROM rag.document d WHERE d.doc_id = c.doc_id)
              AND EXISTS (SELECT 1 FROM rag.document d2 WHERE d2.doc_id = c.doc_id AND d2.feed_id = $1)
            "#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

async fn delete_orphan_chunks(pool: &PgPool, feed: Option<i32>, max: i64) -> Result<()> {
    loop {
        let res = match feed {
            None => sqlx::query!(
                r#"
                DELETE FROM rag.chunk c
                WHERE c.ctid IN (
                    SELECT c2.ctid
                    FROM rag.chunk c2
                    WHERE NOT EXISTS (
                        SELECT 1 FROM rag.document d WHERE d.doc_id = c2.doc_id
                    )
                    LIMIT $1
                )
                "#,
                max
            )
            .execute(pool)
            .await?,
            Some(fid) => sqlx::query!(
                r#"
                DELETE FROM rag.chunk c
                WHERE c.ctid IN (
                    SELECT c2.ctid
                    FROM rag.chunk c2
                    JOIN rag.document d2 ON d2.doc_id = c2.doc_id
                    WHERE d2.feed_id = $1
                      AND NOT EXISTS (
                        SELECT 1 FROM rag.document d WHERE d.doc_id = c2.doc_id
                      )
                    LIMIT $2
                )
                "#,
                fid,
                max
            )
            .execute(pool)
            .await?,
        };
        if res.rows_affected() == 0 { break; }
        println!("  🗑️ Deleted {} orphan chunks", res.rows_affected());
    }
    Ok(())
}

async fn count_error_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>) -> Result<i64> {
    let n = match (cutoff, feed) {
        (Some(ts), None) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'error' AND d.fetched_at < $1
            "#,
            ts
        )
        .fetch_one(pool)
        .await?,
        (Some(ts), Some(fid)) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'error' AND d.fetched_at < $1 AND d.feed_id = $2
            "#,
            ts,
            fid
        )
        .fetch_one(pool)
        .await?,
        (None, None) => sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM rag.document d WHERE d.status = 'error'"#
        )
        .fetch_one(pool)
        .await?,
        (None, Some(fid)) => sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM rag.document d WHERE d.status = 'error' AND d.feed_id = $1"#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

async fn delete_error_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>, max: i64) -> Result<()> {
    loop {
        let res = match (cutoff, feed) {
            (Some(ts), None) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'error' AND d2.fetched_at < $1
                    LIMIT $2
                )
                "#,
                ts,
                max
            )
            .execute(pool)
            .await?,
            (Some(ts), Some(fid)) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'error' AND d2.fetched_at < $1 AND d2.feed_id = $2
                    LIMIT $3
                )
                "#,
                ts,
                fid,
                max
            )
            .execute(pool)
            .await?,
            (None, None) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'error'
                    LIMIT $1
                )
                "#,
                max
            )
            .execute(pool)
            .await?,
            (None, Some(fid)) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'error' AND d2.feed_id = $1
                    LIMIT $2
                )
                "#,
                fid,
                max
            )
            .execute(pool)
            .await?,
        };
        if res.rows_affected() == 0 { break; }
        println!("  🗑️ Deleted {} error docs", res.rows_affected());
    }
    Ok(())
}

async fn count_never_chunked_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>) -> Result<i64> {
    let n = match (cutoff, feed) {
        (Some(ts), None) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest' AND d.fetched_at < $1
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#,
            ts
        )
        .fetch_one(pool)
        .await?,
        (Some(ts), Some(fid)) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest' AND d.fetched_at < $1 AND d.feed_id = $2
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#,
            ts,
            fid
        )
        .fetch_one(pool)
        .await?,
        (None, None) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest'
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#
        )
        .fetch_one(pool)
        .await?,
        (None, Some(fid)) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest' AND d.feed_id = $1
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

async fn delete_never_chunked_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>, max: i64) -> Result<()> {
    loop {
        let res = match (cutoff, feed) {
            (Some(ts), None) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'ingest' AND d2.fetched_at < $1
                      AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                    LIMIT $2
                )
                "#,
                ts,
                max
            )
            .execute(pool)
            .await?,
            (Some(ts), Some(fid)) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'ingest' AND d2.fetched_at < $1 AND d2.feed_id = $2
                      AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                    LIMIT $3
                )
                "#,
                ts,
                fid,
                max
            )
            .execute(pool)
            .await?,
            (None, None) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'ingest'
                      AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                    LIMIT $1
                )
                "#,
                max
            )
            .execute(pool)
            .await?,
            (None, Some(fid)) => sqlx::query!(
                r#"
                DELETE FROM rag.document d
                WHERE d.ctid IN (
                    SELECT d2.ctid FROM rag.document d2
                    WHERE d2.status = 'ingest' AND d2.feed_id = $1
                      AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                    LIMIT $2
                )
                "#,
                fid,
                max
            )
            .execute(pool)
            .await?,
        };
        if res.rows_affected() == 0 { break; }
        println!("  🗑️ Deleted {} never-chunked docs", res.rows_affected());
    }
    Ok(())
}

async fn count_bad_chunks(pool: &PgPool, feed: Option<i32>) -> Result<i64> {
    let n = match feed {
        None => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.chunk c
            WHERE (c.text IS NULL OR btrim(c.text) = '' OR c.token_count <= 0)
            "#
        )
        .fetch_one(pool)
        .await?,
        Some(fid) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.chunk c
            JOIN rag.document d ON d.doc_id = c.doc_id
            WHERE d.feed_id = $1 AND (c.text IS NULL OR btrim(c.text) = '' OR c.token_count <= 0)
            "#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

async fn delete_bad_chunks(pool: &PgPool, feed: Option<i32>, max: i64) -> Result<()> {
    loop {
        let res = match feed {
            None => sqlx::query!(
                r#"
                DELETE FROM rag.chunk c
                WHERE c.ctid IN (
                    SELECT c2.ctid FROM rag.chunk c2
                    WHERE (c2.text IS NULL OR btrim(c2.text) = '' OR c2.token_count <= 0)
                    LIMIT $1
                )
                "#,
                max
            )
            .execute(pool)
            .await?,
            Some(fid) => sqlx::query!(
                r#"
                DELETE FROM rag.chunk c
                WHERE c.ctid IN (
                    SELECT c2.ctid FROM rag.chunk c2
                    JOIN rag.document d ON d.doc_id = c2.doc_id
                    WHERE d.feed_id = $1
                      AND (c2.text IS NULL OR btrim(c2.text) = '' OR c2.token_count <= 0)
                    LIMIT $2
                )
                "#,
                fid,
                max
            )
            .execute(pool)
            .await?,
        };
        if res.rows_affected() == 0 { break; }
        println!("  🗑️ Deleted {} bad chunks", res.rows_affected());
    }
    Ok(())
}

async fn fix_statuses(pool: &PgPool, feed: Option<i32>) -> Result<()> {
    // embedded
    let res = match feed {
        None => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='embedded'
            WHERE EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND NOT EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'embedded')
            "#
        )
        .execute(pool)
        .await?,
        Some(fid) => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='embedded'
            WHERE d.feed_id = $1
              AND EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND NOT EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'embedded')
            "#,
            fid
        )
        .execute(pool)
        .await?,
    };
    println!("✅ Set status=embedded on {} doc(s)", res.rows_affected());

    // chunked
    let res = match feed {
        None => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='chunked'
            WHERE EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'chunked')
            "#
        )
        .execute(pool)
        .await?,
        Some(fid) => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='chunked'
            WHERE d.feed_id = $1
              AND EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'chunked')
            "#,
            fid
        )
        .execute(pool)
        .await?,
    };
    println!("✅ Set status=chunked on {} doc(s)", res.rows_affected());

    // ingest
    let res = match feed {
        None => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='ingest'
            WHERE NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND (d.status IS DISTINCT FROM 'ingest')
            "#
        )
        .execute(pool)
        .await?,
        Some(fid) => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='ingest'
            WHERE d.feed_id = $1
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND (d.status IS DISTINCT FROM 'ingest')
            "#,
            fid
        )
        .execute(pool)
        .await?,
    };
    println!("✅ Set status=ingest on {} doc(s)", res.rows_affected());

    Ok(())
}

async fn drop_temp_indexes(pool: &PgPool) -> Result<()> {
    sqlx::query("DROP INDEX CONCURRENTLY IF EXISTS rag.embedding_vec_ivf_idx_new")
        .execute(pool)
        .await?;
    println!("🧼 Dropped rag.embedding_vec_ivf_idx_new (if existed)");
    Ok(())
}

async fn analyze_tables(pool: &PgPool) -> Result<()> {
    sqlx::query("ANALYZE rag.document")
        .execute(pool)
        .await?;
    sqlx::query("ANALYZE rag.chunk")
        .execute(pool)
        .await?;
    sqlx::query("ANALYZE rag.embedding")
        .execute(pool)
        .await?;
    println!("📊 Analyzed rag.document, rag.chunk, rag.embedding");
    Ok(())
}

async fn vacuum_full(pool: &PgPool) -> Result<()> {
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
    println!("🧽 Vacuumed (FULL) rag.document, rag.chunk, rag.embedding");
    Ok(())
}
