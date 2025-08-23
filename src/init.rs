use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, PgPool};

pub async fn init_db(dsn: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(dsn)
        .await?;

    // Apply any pending migrations (idempotent)
    sqlx::migrate!().run(&pool).await?;

    println!("Database initialized successfully");
    Ok(pool)
}