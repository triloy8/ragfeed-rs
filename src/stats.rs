use anyhow::Result;
use clap::Args;
use sqlx::PgPool;

#[derive(Args, Debug)]
pub struct StatsCmd {
    #[arg(long)] feed: Option<i32>, // show a scoped view for this feed ID
    #[arg(long)] doc: Option<i64>, // show a snapshot for this document ID (replaces `inspect doc <id>`)
    #[arg(long)] chunk: Option<i64>, // show a snapshot for this chunk ID (replaces `inspect chunk <id>`)
}

pub async fn run(pool: &PgPool, args: StatsCmd) -> Result<()> {
    // snapshot modes fully replace the old `inspect` command
    if let Some(id) = args.doc {
        return snapshot_doc(pool, id).await;
    }
    if let Some(id) = args.chunk {
        return snapshot_chunk(pool, id).await;
    }
    if let Some(feed_id) = args.feed {
        return feed_stats(pool, feed_id).await;
    }

    // feeds listing (verbose)
    println!("📡 Feeds:");
    let feeds = sqlx::query!(
        r#"
        SELECT feed_id, name, url, is_active, added_at
        FROM rag.feed
        ORDER BY feed_id
        "#
    )
    .fetch_all(pool)
    .await?;
    for f in feeds {
        println!(
            "  #{}  active={}  name={}  url={}  added_at={:?}",
            f.feed_id,
            f.is_active.unwrap_or(true),
            f.name.unwrap_or_default(),
            f.url,
            f.added_at
        );
    }

    // documents by status
    println!("📄 Documents by status:");
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
    for r in docs {
        println!("  {:10} {}", r.status.unwrap_or_default(), r.cnt.unwrap_or(0));
    }

    if let Ok(row) = sqlx::query!("SELECT MAX(fetched_at) AS last_fetched FROM rag.document")
        .fetch_one(pool)
        .await
    {
        println!("  Last fetched: {:?}", row.last_fetched);
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
        println!("🧩 Chunks: total={} avg_tokens={:.1}", row.total_chunks.unwrap_or(0), row.avg_tokens.unwrap_or(0.0));
    }

    // embeddings summary
    let emb_total = sqlx::query!("SELECT COUNT(*)::bigint AS total FROM rag.embedding")
        .fetch_one(pool)
        .await?
        .total
        .unwrap_or(0);
    println!("🔢 Embeddings: total={}", emb_total);

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
        0 => println!("   Model: (none)"),
        1 => {
            let m = &models[0];
            println!("   Model: {} ({} vectors, last={:?})", m.model, m.cnt.unwrap_or(0), m.last);
        }
        _ => {
            // show top 3 succinctly
            let mut first = true;
            print!("   Models: ");
            for m in models.iter().take(3) {
                if !first { print!(", "); } else { first = false; }
                print!("{} ({} )", m.model, m.cnt.unwrap_or(0));
            }
            if models.len() > 3 { print!(", ..."); }
            println!("");
        }
    }

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
    println!("📈 Coverage: {}/{} ({:.1}%)", embedded as i64, chunks as i64, pct);

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
    println!("   Missing embeddings: {}", missing);

    Ok(())
}

async fn snapshot_doc(pool: &PgPool, id: i64) -> Result<()> {
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

    println!("📄 Document {}:", row.doc_id);
    println!("  Feed ID: {:?}", row.feed_id);
    println!("  URL: {}", row.source_url);
    println!("  Title: {:?}", row.source_title);
    println!("  Published: {:?}", row.published_at);
    println!("  Fetched: {:?}", row.fetched_at);
    println!("  Status: {:?}", row.status);
    println!("  Error: {:?}", row.error_msg);
    println!("  Preview: {:?}", row.preview);

    Ok(())
}

async fn feed_stats(pool: &PgPool, feed_id: i32) -> Result<()> {
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
    println!("📡 Feed #{}:", f.feed_id);
    println!("  Name: {}", f.name.unwrap_or_default());
    println!("  URL: {}", f.url);
    println!("  Active: {}", f.is_active.unwrap_or(true));
    println!("  Added: {:?}", f.added_at);

    // documents by status within this feed
    println!("📄 Documents by status:");
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
    for r in docs {
        println!("  {:10} {}", r.status.unwrap_or_default(), r.cnt.unwrap_or(0));
    }
    if let Ok(row) = sqlx::query!(
        r#"SELECT MAX(fetched_at) AS last_fetched FROM rag.document WHERE feed_id = $1"#,
        feed_id
    )
    .fetch_one(pool)
    .await
    {
        println!("  Last fetched: {:?}", row.last_fetched);
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
        println!("🧩 Chunks: total={} avg_tokens={:.1}", row.total_chunks.unwrap_or(0), row.avg_tokens.unwrap_or(0.0));
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
    println!("📈 Coverage: {}/{} ({:.1}%)  last_embedded={:?}", embedded as i64, chunks as i64, pct, cov.last);

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
    println!("   Missing embeddings: {}", missing);

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
        0 => println!("   Model: (none)"),
        1 => {
            let m = &feed_models[0];
            println!("   Model: {} ({} vectors, last={:?})", m.model, m.cnt.unwrap_or(0), m.last);
        }
        _ => {
            let mut first = true;
            print!("   Models: ");
            for m in feed_models.iter().take(3) {
                if !first { print!(", "); } else { first = false; }
                print!("{} ({} )", m.model, m.cnt.unwrap_or(0));
            }
            if feed_models.len() > 3 { print!(", ..."); }
            println!("");
        }
    }

    // top documents in this feed with pending embeddings
    if missing > 0 {
        println!("   Top docs with pending embeddings:");
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
            println!("     {:>6}  doc={}  {}", r.pending.unwrap_or(0), r.doc_id, r.source_title.unwrap_or_default());
        }
    }

    Ok(())
}

async fn snapshot_chunk(pool: &PgPool, id: i64) -> Result<()> {
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

    println!("🧩 Chunk {} (Doc {:?}):", row.chunk_id, row.doc_id);
    println!("  Index: {:?}", row.chunk_index);
    println!("  Tokens: {:?}", row.token_count);
    println!("  Preview: {:?}", row.preview);

    Ok(())
}
