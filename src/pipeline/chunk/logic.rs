// Core chunking logic extracted from crate::chunk

pub fn chunk_token_ids<'a>(
    ids: &'a [u32],
    target: usize,
    overlap: usize,
    max_chunks: usize,
) -> Vec<&'a [u32]> {
    let target = target.max(1);
    let overlap = overlap.min(target.saturating_sub(1));

    let mut out = Vec::new();
    let mut start = 0usize;

    while start < ids.len() && out.len() < max_chunks {
        let end = (start + target).min(ids.len());
        out.push(&ids[start..end]);
        if end == ids.len() { break; }
        start = end.saturating_sub(overlap);
    }
    out
}

