use clap::{Parser, Subcommand};
use sqlx::{PgPool};
use anyhow::Result;
use dotenvy::dotenv;
use std::env;

mod init;
mod feed;
mod ingest;
mod chunk;
mod tokenizer;
mod encoder;
mod embed;
mod stats;
mod reindex;
mod gc;

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
    Chunk(chunk::ChunkCmd),
    Embed(embed::EmbedCmd),
    Stats(stats::StatsCmd),
    Reindex(reindex::ReindexCmd),
    Gc(gc::GcCmd),
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
        Commands::Chunk(args) => chunk::run(&pool, args).await?,
        Commands::Embed(args) => embed::run(&pool, args).await?,
        Commands::Stats(args) => stats::run(&pool, args).await?,
        Commands::Reindex(args) => reindex::run(&pool, args).await?,
        Commands::Gc(args) => gc::run(&pool, args).await?,
        // Commands::Query { query } => println!("TODO: query: {query}"),
        // Commands::Eval => println!("TODO: eval"),
    }

    Ok(())
}
