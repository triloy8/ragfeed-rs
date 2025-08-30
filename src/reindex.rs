use anyhow::{Context, Result};
use clap::Args;
use sqlx::PgPool;

#[derive(Args, Debug)]
pub struct ReindexCmd {
    #[arg(long)] lists: Option<i32>, // force a specific number of IVF lists (K). If omitted, uses sqrt(n) heuristic.
    #[arg(long, default_value_t = false)] dry_run: bool, // print planned actions and exit without changing anything.
}

pub async fn run(pool: &PgPool, args: ReindexCmd) -> Result<()> {
    // count embeddings to drive heuristic
    let n = sqlx::query!("SELECT COUNT(*)::bigint AS n FROM rag.embedding")
        .fetch_one(pool)
        .await?
        .n
        .unwrap_or(0);

    // discover index existence and current lists from index definition
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
    let index_exists = idx_row.is_some();
    let current_lists = idx_row
        .and_then(|r| r.lists)
        .and_then(|s| s.parse::<i32>().ok());

    // choose desired lists
    let desired_lists = if let Some(k) = args.lists {
        k.max(1)
    } else {
        heuristic_lists(n as i64)
    };

    // decide action
    let action = if !index_exists {
        Action::Create(desired_lists)
    } else if let Some(k) = current_lists {
        if k == desired_lists { Action::Reindex } else { Action::Swap(desired_lists) }
    } else {
        // index exists but lists not parsed (older pgvector or unknown). Be conservative: reindex in place.
        Action::Reindex
    };

    // report plan
    println!(
        "Reindex plan: rows={} current_lists={:?} desired_lists={} action={:?} analyze=TRUE",
        n, current_lists, desired_lists, action
    );
    if args.dry_run { return Ok(()); }

    // execute
    match action {
        Action::Create(k) => {
            create_new_index(pool, k, false).await?;
            // rename new to canonical (no old index present)
            sqlx::query("ALTER INDEX rag.embedding_vec_ivf_idx_new RENAME TO embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
        }
        Action::Reindex => {
            sqlx::query("REINDEX INDEX CONCURRENTLY rag.embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
        }
        Action::Swap(k) => {
            create_new_index(pool, k, true).await?;
            // drop old and rename new
            sqlx::query("DROP INDEX CONCURRENTLY IF EXISTS rag.embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
            sqlx::query("ALTER INDEX rag.embedding_vec_ivf_idx_new RENAME TO embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
        }
    }

    // always analyze after reindex to refresh planner stats
    sqlx::query("ANALYZE rag.embedding")
        .execute(pool)
        .await?;
    println!("Analyzed rag.embedding");

    println!("Reindex completed.");
    Ok(())
}

#[derive(Debug)]
enum Action { Create(i32), Reindex, Swap(i32) }

fn heuristic_lists(n: i64) -> i32 {
    if n <= 0 { return 50; }
    let k = (n as f64).sqrt().round() as i32;
    k.clamp(50, 8192)
}

async fn create_new_index(pool: &PgPool, lists: i32, _concurrently: bool) -> Result<()> {
    // always build concurrently and schema-qualify the index name for clarity
    let sql = format!(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS rag.embedding_vec_ivf_idx_new \
         ON rag.embedding USING ivfflat (vec vector_cosine_ops) WITH (lists = {})",
        lists
    );
    sqlx::query(&sql).execute(pool).await.context("create ivfflat index")?;
    Ok(())
}
