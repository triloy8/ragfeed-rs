use anyhow::Result;
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::init::Phase as InitPhase;

#[derive(Args)]
pub struct InitCmd {
    #[arg(long, default_value_t = false)]
    apply: bool, // default is plan-only
}

pub async fn run(pool: &PgPool, args: InitCmd) -> Result<()> {
    let log = telemetry::init();
    let _g = log.root_span_kv([("apply", args.apply.to_string())]).entered();

    if !args.apply {
        if telemetry::config::json_mode() {
            #[derive(Serialize)]
            struct InitPlan { actions: Vec<&'static str> }
            let plan = InitPlan { actions: vec!["migrate ./migrations"] };
            log.plan(&plan)?;
        } else {
            let _s = log.span(&InitPhase::Plan).entered();
            log.info("📝 Init plan — would run migrations from ./migrations");
            log.info("   Use --apply to execute migrations.");
        }
        return Ok(());
    }

    let _s = log.span(&InitPhase::Migrate).entered();
    sqlx::migrate!("./migrations").run(pool).await?;
    if telemetry::config::json_mode() {
        #[derive(Serialize)]
        struct InitResult { migrated: bool }
        log.result(&InitResult { migrated: true })?;
    } else {
        log.info("✅ Database initialized");
    }
    Ok(())
}
