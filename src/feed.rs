use anyhow::Result;
use clap::{Args, Subcommand};
use sqlx::PgPool;

/// rag feed add/ls
#[derive(Args)]
pub struct FeedCmd {
    #[command(subcommand)]
    pub cmd: FeedSub,
}

#[derive(Subcommand)]
pub enum FeedSub {
    // add a new feed (plan-only by default; use --apply to write)
    Add {
        url: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value_t = true)]
        active: bool,
        #[arg(long, default_value_t = false)]
        apply: bool,
    },
    // list feeds
    Ls {
        #[arg(long)]
        active_only: bool,
    },
}

pub async fn run(pool: &PgPool, args: FeedCmd) -> Result<()> {
    match args.cmd {
        FeedSub::Add { url, name, active, apply } => add_feed(pool, url, name, active, apply).await?,
        FeedSub::Ls { active_only } => ls_feeds(pool, active_only).await?,
    }
    Ok(())
}

async fn add_feed(pool: &PgPool, url: String, name: Option<String>, active: bool, apply: bool) -> Result<()> {
    if !apply {
        println!("📝 Feed plan — add url={} name={:?} active={}", url, name, active);
        println!("   Use --apply to execute.");
        return Ok(());
    }
    sqlx::query!(
        r#"
        INSERT INTO rag.feed (url, name, is_active)
        VALUES ($1, $2, $3)
        ON CONFLICT (url) DO UPDATE SET name=EXCLUDED.name, is_active=EXCLUDED.is_active
        "#,
        url,
        name,
        active
    )
    .execute(pool)
    .await?;
    println!("✅ Feed added: {url}");
    Ok(())
}

async fn ls_feeds(pool: &PgPool, active_only: bool) -> Result<()> {
    println!("📡 Feeds:");
    let rows = sqlx::query!(
        r#"
        SELECT feed_id, url, name, is_active, added_at
        FROM rag.feed
        WHERE ($1::bool IS NULL OR is_active = $1)
        ORDER BY feed_id
        "#,
        if active_only { Some(true) } else { None }
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        println!(
            "[{}] {} ({:?}) active={:?} added_at={:?}",
            row.feed_id, row.url, row.name, row.is_active, row.added_at
        );
    }
    Ok(())
}
