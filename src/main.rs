use clap::{Parser, Subcommand};
use anyhow::Result;
use dotenvy::dotenv;
use std::env;

mod init;
mod feed;

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
            let _pool = init::init_db(&dsn).await?;
        }
        Commands::Feed { action } => match action {
            FeedAction::Add { url } => {
                feed::add_feed(&dsn, &url).await?;
            }
            FeedAction::Ls => {
                feed::list_feeds(&dsn).await?;
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
