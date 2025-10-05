use serde::Serialize;

use super::db::CandRow;

#[derive(Debug, Clone, Serialize)]
pub struct QueryResultRow {
    pub rank: usize,
    pub distance: f32,
    pub chunk_id: i64,
    pub doc_id: i64,
    pub title: Option<String>,
    pub preview: Option<String>,
}

pub fn shape_results(candidates: Vec<CandRow>, topk: usize, doc_cap: usize) -> Vec<QueryResultRow> {
    let mut per_doc_seen: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    let mut out: Vec<QueryResultRow> = Vec::new();
    for row in candidates.into_iter() {
        let seen = per_doc_seen.entry(row.doc_id).or_insert(0);
        if *seen >= doc_cap { continue; }
        *seen += 1;
        out.push(QueryResultRow {
            rank: out.len() + 1,
            distance: row.distance,
            chunk_id: row.chunk_id,
            doc_id: row.doc_id,
            title: row.title,
            preview: row.preview,
        });
        if out.len() >= topk { break; }
    }
    out
}
