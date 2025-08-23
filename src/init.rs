use anyhow::Result;
use clap::Args;
use sqlx::PgPool;

#[derive(Args)]
pub struct InitCmd {}

pub async fn run(pool: &PgPool, _args: InitCmd) -> Result<()> {
    sqlx::migrate!("./migrations").run(pool).await?;

    println!("✅ Database initialized");
    Ok(())
}