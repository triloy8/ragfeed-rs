use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use sqlx::PgPool;
use url::Url;

use crate::telemetry::{self};
use crate::telemetry::ops::feed::Phase as FeedPhase;

mod db;
pub mod types;

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
        /// Filter by active status: true/false. Omit to show all.
        #[arg(long)]
        active: Option<bool>,
    },
}

pub async fn run(pool: &PgPool, args: FeedCmd) -> Result<()> {
    let log = telemetry::feed();
    let _g = log.root_span().entered();
    match args.cmd {
        FeedSub::Add { url, name, active, apply } => add_feed(pool, url, name, active, apply).await?,
        FeedSub::Ls { active } => ls_feeds(pool, active).await?,
    }
    Ok(())
}

async fn add_feed(pool: &PgPool, url: String, name: Option<String>, active: bool, apply: bool) -> Result<()> {
    let log = telemetry::feed();
    let _g = log.root_span_kv([
        ("mode", if apply { "apply".to_string() } else { "plan".to_string() }),
        ("url", url.clone()),
        ("name", format!("{:?}", name)),
        ("active", active.to_string()),
    ]).entered();

    // URL validation (friendly error before DB I/O)
    if Url::parse(&url).is_err() { bail!("Invalid URL: {}", url); }

    if !apply {
        let _s = log.span(&FeedPhase::Plan).entered();
        // Always log plan summary
        log.info(format!("📝 Feed plan — add url={} name={:?} active={}", url, name, active));
        log.info("   Use --apply to execute.");
        // Emit structured plan when in JSON mode (stdout)
        if telemetry::config::json_mode() {
            let plan = types::FeedAddPlan { action: "add", url: url.clone(), name: name.clone(), active };
            log.plan(&plan)?;
        }
        return Ok(());
    }
    let _s = log.span(&FeedPhase::Add).entered();
    let inserted = db::upsert_feed(pool, &url, name.as_deref(), active).await?;
    // Always log human summary
    if inserted { log.info("➕ Feed added"); } else { log.info("♻️ Feed updated"); }
    // Emit structured result when in JSON mode (stdout)
    if telemetry::config::json_mode() {
        let result = types::FeedAddResult { inserted, url };
        log.result(&result)?;
    }
    Ok(())
}

async fn ls_feeds(pool: &PgPool, active: Option<bool>) -> Result<()> {
    let log = telemetry::feed();
    let _g = log.root_span_kv([("active", format!("{:?}", active))]).entered();
    let _s = log.span(&FeedPhase::List).entered();
    let feeds = db::list_feeds(pool, active).await?;
    // Always log listing
    log.info("📡 Feeds:");
    for row in &feeds {
        log.info(format!(
            "[{}] {} ({:?}) active={:?} added_at={:?}",
            row.feed_id, row.url, row.name, row.is_active, row.added_at
        ));
    }
    // Emit structured list when in JSON mode (stdout)
    if telemetry::config::json_mode() {
        let list = types::FeedList { feeds };
        log.result(&list)?;
    }
    Ok(())
}
