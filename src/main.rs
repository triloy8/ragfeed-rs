use clap::{Parser, Subcommand};
use sqlx::{PgPool};
use anyhow::Result;
use dotenvy::dotenv;
use std::env;
use std::time::Instant;

mod out;

mod init;
mod feed;
mod ingest;
mod tokenizer;
mod extractor;
mod encoder;
mod stats;
mod reindex;
mod query;
mod util;
mod maintenance;
mod telemetry;
mod pipeline;

#[derive(Parser)]
#[command(name = "rag", about = "RAG pipeline CLI")]
struct Cli {
    #[arg(global = true, short, long)]
    dsn: Option<String>,
    /// Emit a single JSON envelope to stdout; logs go to stderr
    #[arg(global = true, long, default_value_t = false)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init(init::InitCmd),
    Feed(feed::FeedCmd),
    Ingest(ingest::IngestCmd),
    Chunk(pipeline::chunk::ChunkCmd),
    Embed(pipeline::embed::EmbedCmd),
    Stats(stats::StatsCmd),
    Reindex(reindex::ReindexCmd),
    Gc(maintenance::gc::GcCmd),
    Query(query::QueryCmd),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();
    out::set_json_mode(cli.json);
    let _t0 = Instant::now();

    // initialize logging/tracing (stderr). Respect RUST_LOG and RAG_LOG_FORMAT
    telemetry::config::init_tracing();
    let dsn = cli
        .dsn
        .or_else(|| env::var("DATABASE_URL").ok())
        .expect("Please provide --dsn or set DATABASE_URL in .env");

    let pool = PgPool::connect(&dsn).await?;

    match cli.command {
        Commands::Init(args) => init::run(&pool, args).await?,
        Commands::Feed(args) => feed::run(&pool, args).await?,
        Commands::Ingest(args) => ingest::run(&pool, args).await?,
        Commands::Chunk(args) => pipeline::chunk::run(&pool, args).await?,
        Commands::Embed(args) => pipeline::embed::run(&pool, args).await?,
        Commands::Stats(args) => stats::run(&pool, args).await?,
        Commands::Reindex(args) => reindex::run(&pool, args).await?,
        Commands::Gc(args) => maintenance::gc::run(&pool, args).await?,
        Commands::Query(args) => query::run(&pool, args).await?,
        // Commands::Eval => println!("TODO: eval"),
    }

    Ok(())
}

// init_tracing moved to telemetry::config::init_tracing
