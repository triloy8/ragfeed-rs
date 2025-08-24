use clap::{Parser, Subcommand};
use sqlx::{PgPool};
use anyhow::Result;
use dotenvy::dotenv;
use std::env;

mod init;
mod feed;
mod ingest;

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
    Init(init::InitCmd),
    Feed(feed::FeedCmd),
    Ingest(ingest::IngestCmd),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();
    let dsn = cli
        .dsn
        .or_else(|| env::var("DATABASE_URL").ok())
        .expect("Please provide --dsn or set DATABASE_URL in .env");

    let pool = PgPool::connect(&dsn).await?;

    match cli.command {
        Commands::Init(args) => init::run(&pool, args).await?,
        Commands::Feed(args) => feed::run(&pool, args).await?,
        Commands::Ingest(args) => ingest::run(&pool, args).await?,
        // Commands::Chunk => println!("TODO: chunk"),
        // Commands::Embed => println!("TODO: embed"),
        // Commands::Query { query } => println!("TODO: query: {query}"),
        // Commands::Eval => println!("TODO: eval"),
        // Commands::Reindex => println!("TODO: reindex"),
        // Commands::Gc => println!("TODO: gc"),
    }

    Ok(())
}
