use clap::{Parser, Subcommand};
use sqlx::{PgPool};
use anyhow::Result;
use dotenvy::dotenv;
use std::env;
use std::time::Instant;


// mod init; // removed (hard removal of `init` subcommand)
mod feed;
mod ingestion;
mod tokenizer;
mod encoder;
mod stats;
mod query;
mod util;
mod maintenance;
mod telemetry;
mod pipeline;
mod output;

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
    Feed(feed::FeedCmd),
    Ingest(ingestion::IngestCmd),
    Chunk(pipeline::chunk::ChunkCmd),
    Embed(pipeline::embed::EmbedCmd),
    Stats(stats::StatsCmd),
    Reindex(maintenance::reindex::ReindexCmd),
    Gc(maintenance::gc::GcCmd),
    Query(query::QueryCmd),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();
    let _t0 = Instant::now();

    // initialize logging/tracing (stderr). Respect RUST_LOG and RAG_LOG_FORMAT
    telemetry::config::init_tracing();
    let dsn = cli
        .dsn
        .or_else(|| env::var("DATABASE_URL").ok())
        .expect("Please provide --dsn or set DATABASE_URL in .env");

    let pool = PgPool::connect(&dsn).await?;

    match cli.command {
        Commands::Feed(args) => feed::run(&pool, args).await?,
        Commands::Ingest(args) => ingestion::run(&pool, args).await?,
        Commands::Chunk(args) => pipeline::chunk::run(&pool, args).await?,
        Commands::Embed(args) => pipeline::embed::run(&pool, args).await?,
        Commands::Stats(args) => stats::run(&pool, args).await?,
        Commands::Reindex(args) => maintenance::reindex::run(&pool, args).await?,
        Commands::Gc(args) => maintenance::gc::run(&pool, args).await?,
        Commands::Query(args) => query::run(&pool, args).await?,
        // Commands::Eval => println!("TODO: eval"),
    }

    Ok(())
}

// init_tracing moved to telemetry::config::init_tracing
