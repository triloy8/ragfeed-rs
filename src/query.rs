use anyhow::{bail, Context, Result};
use clap::Args;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;
use serde::Serialize;

use crate::encoder::{Device, E5Encoder};
use pgvector::Vector as PgVector;

#[derive(Args, Debug)]
pub struct QueryCmd {
    query: String, // the user question to search for  
    #[arg(long, default_value_t = 100)] top_n: i64, // candidate pool size from ANN 
    #[arg(long, default_value_t = 6)] topk: usize, // final results to print
    #[arg(long, default_value_t = 2)] doc_cap: usize, // max results per document
    #[arg(long)] probes: Option<i32>, // override ivfflat probes (lists to scan). If omitted, uses ~lists/10
    #[arg(long)] feed: Option<i32>, // restrict results to a specific feed
    #[arg(long)] since: Option<String>, // restrict by document freshness (e.g., 7d or YYYY-MM-DD)
    #[arg(long, default_value_t = false)] show_context: bool, // print preview text for each result

    // E5Encoder config (reused from embed)
    #[arg(long, default_value = "intfloat/e5-small-v2")] pub model_id: String,
    #[arg(long)] pub onnx_filename: Option<String>,
    #[arg(long, value_enum, default_value_t = Device::Cpu)] pub device: Device,
}

#[derive(Serialize)]
struct QueryResultRow {
    rank: usize,
    distance: f32,
    chunk_id: i64,
    doc_id: i64,
    title: Option<String>,
    preview: Option<String>,
}

pub async fn run(pool: &PgPool, args: QueryCmd) -> Result<()> {
    use crate::out::{self};
    use crate::out::query::Phase as QueryPhase;
    let log = out::query();
    let _g = log
        .root_span_kv([
            ("top_n", args.top_n.to_string()),
            ("topk", args.topk.to_string()),
            ("doc_cap", args.doc_cap.to_string()),
            ("probes", format!("{:?}", args.probes)),
            ("feed", format!("{:?}", args.feed)),
            ("since", format!("{:?}", args.since)),
            ("show_context", args.show_context.to_string()),
            ("json", out::json_mode().to_string()),
            ("model_id", args.model_id.clone()),
            ("device", format!("{:?}", args.device)),
        ])
        .entered();
    let _prep = log.span(&QueryPhase::Prepare).entered();
    // ensure we have embeddings
    let dim_row = sqlx::query!("SELECT dim FROM rag.embedding LIMIT 1")
        .fetch_optional(pool)
        .await?;
    if dim_row.is_none() {
        log.info("ℹ️  No embeddings found. Run `rag embed` first.");
        return Ok(());
    }
    let db_dim = dim_row.unwrap().dim as usize;

    // build encoder and embed the query
    let mut enc = {
        let _s = log.span(&QueryPhase::Prepare).entered();
        E5Encoder::new(&args.model_id, args.onnx_filename.as_deref(), args.device)
            .context("init encoder")?
    };
    let qvec = {
        let _s = log.span(&QueryPhase::EmbedQuery).entered();
        enc.embed_query(&args.query).context("embed query")?
    };
    if qvec.len() != db_dim {
        bail!("query embedding dim={} != DB dim={}", qvec.len(), db_dim);
    }

    // set probes
    let probes = match args.probes {
        Some(p) => Some(p.max(1)),
        None => recommend_probes(pool).await?,
    };
    if let Some(p) = probes {
        let _s = log.span(&QueryPhase::SetProbes).entered();
        // SET LOCAL doesn't allow bind params in SQLx macros; build string safely
        let sql = format!("SET LOCAL ivfflat.probes = {}", p);
        sqlx::query(&sql).execute(pool).await?;
    }

    // parse filters
    let since_ts = parse_since(&args.since)?;

    let _fetch = log.span(&QueryPhase::FetchCandidates).entered();
    // fetch ANN candidates
    let candidates = fetch_ann_candidates(
        pool, 
        &qvec, 
        args.top_n.max(1), 
        args.feed, 
        since_ts,
        args.show_context,
    ).await?;
    drop(_fetch);

    if candidates.is_empty() {
        log.info("ℹ️  No results");
        return Ok(());
    }

    let _pf = log.span(&QueryPhase::PostFilter).entered();
    // apply per-doc cap and take topk
    let mut per_doc_seen: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    let mut out: Vec<QueryResultRow> = Vec::new();
    for (idx, row) in candidates.into_iter().enumerate() {
        let seen = per_doc_seen.entry(row.doc_id).or_insert(0);
        if *seen >= args.doc_cap { continue; }
        *seen += 1;
        out.push(QueryResultRow {
            rank: out.len() + 1,
            distance: row.distance,
            chunk_id: row.chunk_id,
            doc_id: row.doc_id,
            title: row.title,
            preview: row.preview,
        });
        if out.len() >= args.topk { break; }
    }
    drop(_pf);

    let _out_span = log.span(&QueryPhase::Output).entered();
    if out::json_mode() {
        log.result(&out)?;
    } else {
        log.info("🔍 Results:");
        for r in &out {
            log.info(format!("#{}  dist={:.4}  chunk={} doc={}  {:?}", r.rank, r.distance, r.chunk_id, r.doc_id, r.title));
            if args.show_context {
                if let Some(p) = &r.preview { log.info(format!("  {}", p.replace('\n', " "))); }
            }
        }
    }

    Ok(())
}

struct CandRow { chunk_id: i64, doc_id: i64, title: Option<String>, preview: Option<String>, distance: f32 }

async fn fetch_ann_candidates(
    pool: &PgPool,
    qvec: &Vec<f32>,
    top_n: i64,
    feed: Option<i32>,
    since: Option<DateTime<Utc>>,
    want_preview: bool,
) -> Result<Vec<CandRow>> {
    if feed.is_none() && since.is_none() {
        use sqlx::Row;
        let rows = sqlx::query(
            r#"
            SELECT c.chunk_id, c.doc_id, d.source_title AS title,
                   (e.vec <-> $1) AS distance,
                   CASE WHEN $3 THEN substring(c.text, 1, 300) ELSE NULL END AS preview
            FROM rag.embedding e
            JOIN rag.chunk c ON c.chunk_id = e.chunk_id
            JOIN rag.document d ON d.doc_id = c.doc_id
            ORDER BY distance ASC
            LIMIT $2
            "#
        )
        .bind(PgVector::from(qvec.clone()))
        .bind(top_n)
        .bind(want_preview)
        .fetch_all(pool)
        .await?;
        let out = rows
            .into_iter()
            .map(|row| CandRow {
                chunk_id: row.get::<i64, _>("chunk_id"),
                doc_id: row.get::<i64, _>("doc_id"),
                title: row.get::<Option<String>, _>("title"),
                preview: row.get::<Option<String>, _>("preview"),
                distance: row.get::<f64, _>("distance") as f32,
            })
            .collect();
        return Ok(out);
    }

    // with filters
    use sqlx::Row;
    let rows = sqlx::query(
        r#"
        SELECT c.chunk_id, c.doc_id, d.source_title AS title,
               (e.vec <-> $1) AS distance,
               CASE WHEN $5 THEN substring(c.text, 1, 300) ELSE NULL END AS preview
        FROM rag.embedding e
        JOIN rag.chunk c ON c.chunk_id = e.chunk_id
        JOIN rag.document d ON d.doc_id = c.doc_id
        WHERE ($2::int4 IS NULL OR d.feed_id = $2)
          AND ($3::timestamptz IS NULL OR d.fetched_at >= $3)
        ORDER BY distance ASC
        LIMIT $4
        "#
    )
    .bind(PgVector::from(qvec.clone()))
    .bind(feed)
    .bind(since)
    .bind(top_n)
    .bind(want_preview)
    .fetch_all(pool)
    .await?;
    let out = rows
        .into_iter()
        .map(|row| CandRow {
            chunk_id: row.get::<i64, _>("chunk_id"),
            doc_id: row.get::<i64, _>("doc_id"),
            title: row.get::<Option<String>, _>("title"),
            preview: row.get::<Option<String>, _>("preview"),
            distance: row.get::<f64, _>("distance") as f32,
        })
        .collect();
    Ok(out)
}

async fn recommend_probes(pool: &PgPool) -> Result<Option<i32>> {
    let row = sqlx::query!(
        r#"
        SELECT substring(pg_get_indexdef(i.indexrelid) from 'lists = ([0-9]+)') AS lists
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
        WHERE nsp.nspname = 'rag' AND c.relname = 'embedding_vec_ivf_idx'
        "#
    )
    .fetch_optional(pool)
    .await?;
    let lists = row.and_then(|r| r.lists).and_then(|s| s.parse::<i32>().ok());
    Ok(lists.map(|k| (k / 10).max(1)))
}

fn parse_since(since: &Option<String>) -> Result<Option<DateTime<Utc>>> {
    let Some(s) = since.as_ref() else { return Ok(None); };

    // "2d" → now - 2 days
    if let Some(stripped) = s.strip_suffix('d') {
        if let Ok(days) = stripped.parse::<i64>() {
            if days > 0 { return Ok(Some(Utc::now() - Duration::days(days))); }
        }
    }
    // "YYYY-MM-DD"
    if let Ok(nd) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = nd.and_hms_opt(0,0,0).unwrap();
        return Ok(Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc)));
    }
    // RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(Some(dt.with_timezone(&Utc)));
    }
    // if unparseable -> ignore filter
    Ok(None)
}
