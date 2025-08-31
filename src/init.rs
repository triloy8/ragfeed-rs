use anyhow::Result;
use clap::Args;
use sqlx::PgPool;

#[derive(Args)]
pub struct InitCmd {
    #[arg(long, default_value_t = false)]
    apply: bool, // default is plan-only
}

pub async fn run(pool: &PgPool, args: InitCmd) -> Result<()> {
    if !args.apply {
        println!("📝 Init plan — would run migrations from ./migrations");
        println!("   Use --apply to execute migrations.");
        return Ok(());
    }

    sqlx::migrate!("./migrations").run(pool).await?;
    println!("✅ Database initialized");
    Ok(())
}
