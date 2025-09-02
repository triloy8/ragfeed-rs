use anyhow::Result;
use clap::{Args, Subcommand};
use serde::Serialize;
use sqlx::PgPool;

use crate::out::{self};
use crate::out::feed::Phase as FeedPhase;

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
    let log = out::feed();
    let _g = log.root_span().entered();
    match args.cmd {
        FeedSub::Add { url, name, active, apply } => add_feed(pool, url, name, active, apply).await?,
        FeedSub::Ls { active_only } => ls_feeds(pool, active_only).await?,
    }
    Ok(())
}

async fn add_feed(pool: &PgPool, url: String, name: Option<String>, active: bool, apply: bool) -> Result<()> {
    let log = out::feed();
    let _g = log.root_span_kv([
        ("mode", if apply { "apply".to_string() } else { "plan".to_string() }),
        ("url", url.clone()),
        ("name", format!("{:?}", name)),
        ("active", active.to_string()),
    ]).entered();

    if !apply {
        if out::json_mode() {
            #[derive(Serialize)]
            struct FeedAddPlan { action: &'static str, url: String, name: Option<String>, active: bool }
            let plan = FeedAddPlan { action: "add", url: url.clone(), name: name.clone(), active };
            log.plan(&plan)?;
        } else {
            let _s = log.span(&FeedPhase::Plan).entered();
            log.info(format!("📝 Feed plan — add url={} name={:?} active={}", url, name, active));
            log.info("   Use --apply to execute.");
        }
        return Ok(());
    }
    let _s = log.span(&FeedPhase::Add).entered();
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
    if out::json_mode() {
        #[derive(Serialize)]
        struct FeedAddResult { added: bool, url: String }
        log.result(&FeedAddResult { added: true, url })?;
    } else {
        log.info("✅ Feed added");
    }
    Ok(())
}

async fn ls_feeds(pool: &PgPool, active_only: bool) -> Result<()> {
    let log = out::feed();
    let _g = log.root_span_kv([("active_only", active_only.to_string())]).entered();
    let _s = log.span(&FeedPhase::List).entered();
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
    if out::json_mode() {
        #[derive(Serialize)]
        struct FeedRow { feed_id: i32, url: String, name: Option<String>, is_active: Option<bool>, added_at: Option<chrono::DateTime<chrono::Utc>> }
        #[derive(Serialize)]
        struct FeedList { feeds: Vec<FeedRow> }
        let feeds: Vec<FeedRow> = rows.into_iter().map(|r| FeedRow {
            feed_id: r.feed_id,
            url: r.url,
            name: r.name,
            is_active: r.is_active,
            added_at: r.added_at,
        }).collect();
        log.result(&FeedList { feeds })?;
    } else {
        log.info("📡 Feeds:");
        for row in rows {
            log.info(format!(
                "[{}] {} ({:?}) active={:?} added_at={:?}",
                row.feed_id, row.url, row.name, row.is_active, row.added_at
            ));
        }
    }
    Ok(())
}
