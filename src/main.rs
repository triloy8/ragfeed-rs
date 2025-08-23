use clap::{Parser, Subcommand};
use sqlx::{postgres::PgPoolOptions, PgPool};
use anyhow::Result;
use dotenvy::dotenv;
use std::env;

#[derive(Parser)]
#[command(name = "rag", about = "RAG pipeline CLI")]
struct Cli {
    #[arg(global = true, short, long)]
    dsn: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Feed {
        #[command(subcommand)]
        action: FeedAction,
    },
    Ingest,
    Chunk,
    Embed,
    Query {
        query: String,
    },
    Eval,
    Reindex,
    Gc,
}

#[derive(Subcommand)]
enum FeedAction {
    Add { url: String },
    Ls,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let cli = Cli::parse();
    let dsn = cli
        .dsn
        .or_else(|| env::var("DATABASE_URL").ok())
        .expect("Please provide --dsn or set DATABASE_URL in .env");

    match cli.command {
        Commands::Init => {
            let _pool = init_db(&dsn).await?;
        }
        Commands::Feed { action } => match action {
            FeedAction::Add { url } => {
                add_feed(&dsn, &url).await?;
            }
            FeedAction::Ls => {
                list_feeds(&dsn).await?;
            }
        },
        Commands::Ingest => println!("TODO: ingest"),
        Commands::Chunk => println!("TODO: chunk"),
        Commands::Embed => println!("TODO: embed"),
        Commands::Query { query } => println!("TODO: query: {query}"),
        Commands::Eval => println!("TODO: eval"),
        Commands::Reindex => println!("TODO: reindex"),
        Commands::Gc => println!("TODO: gc"),
    }

    Ok(())
}

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

pub async fn add_feed(dsn: &str, url: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(dsn)
        .await?;

    // Check if feed already exists
    let existing_feed = sqlx::query!(
        "SELECT * FROM rag.feed WHERE url = $1",
        url
    )
    .fetch_one(&pool)
    .await;

    match existing_feed {
        Ok(_) => println!("Feed with URL {} already exists", url),
        Err(sqlx::Error::RowNotFound) => {
            // Insert new feed
            let _result = sqlx::query!(
                "INSERT INTO rag.feed (url, name) VALUES ($1, $2)",
                url,
                url.split('/').last().unwrap_or_default()
            )
            .execute(&pool)
            .await?;
            println!("Feed with URL {} added successfully", url);
        }
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

pub async fn list_feeds(dsn: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(dsn)
        .await?;

    // List all feeds
    let feeds = sqlx::query!(
        "SELECT * FROM rag.feed"
    )
    .fetch_all(&pool)
    .await?;

    for feed in feeds {
        println!("Feed ID: {}, URL: {}", feed.feed_id, feed.url);
    }

    Ok(())
}
