use anyhow::{anyhow, bail, Context, Result};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Debug)]
pub struct Gpt2Tokenizer {
    // string token -> id
    encoder: HashMap<String, usize>,
    // id -> string token
    decoder: HashMap<usize, String>,
    // pair -> rank
    bpe_ranks: HashMap<(String, String), usize>,
    // byte-level mappings used before/after BPE
    byte_encoder: HashMap<u8, char>,
    byte_decoder: HashMap<char, u8>,
    // GPT-2 style pretokenizer pattern
    pat: Regex,
    // optional speed cache for BPE results
    bpe_cache: HashMap<String, Vec<String>>,
}

impl Gpt2Tokenizer {
    /// Build from GPT-2 style `vocab.json` and `merges.txt`.
    pub fn from_files<P: AsRef<Path>>(vocab_path: P, merges_path: P) -> Result<Self> {
        // load vocab.json
        let vocab_file = File::open(&vocab_path).with_context(|| {
            format!("Failed to open vocab file: {}", vocab_path.as_ref().display())
        })?;
        let reader = BufReader::new(vocab_file);

        // vocab.json is a map of "token" -> id
        let encoder: HashMap<String, usize> = serde_json::from_reader(reader)
            .with_context(|| "Failed to parse vocab.json")?;

        // build decoder (id->token)
        let mut decoder: HashMap<usize, String> = HashMap::with_capacity(encoder.len());
        for (tok, id) in &encoder {
            decoder.insert(*id, tok.clone());
        }

        // load merges.txt
        let merges_file = File::open(&merges_path).with_context(|| {
            format!("Failed to open merges file: {}", merges_path.as_ref().display())
        })?;
        let reader = BufReader::new(merges_file);
        let mut bpe_ranks: HashMap<(String, String), usize> = HashMap::new();

        // GPT-2 merges first line is a comment like "#version: 0.2"
        // We skip lines starting with '#', and each remaining line is "A B".
        let mut rank: usize = 0;
        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let a = parts.next();
            let b = parts.next();
            if let (Some(a), Some(b)) = (a, b) {
                bpe_ranks.insert((a.to_string(), b.to_string()), rank);
                rank += 1;
            }
        }

        // byte <-> unicode trick (GPT-2 style)
        let (byte_encoder, byte_decoder) = bytes_to_unicode();

        // GPT-2 pretokenization regex
        let pat = Regex::new(
            r"(?:'s|'t|'re|'ve|'m|'ll|'d| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?:\S|\z))",
        )?;

        Ok(Self {
            encoder,
            decoder,
            bpe_ranks,
            byte_encoder,
            byte_decoder,
            pat,
            bpe_cache: HashMap::new(),
        })
    }

    /// Encode user-visible text into GPT-2 style token IDs.
    pub fn encode(&mut self, text: &str) -> Result<Vec<usize>> {
        let mut ids: Vec<usize> = Vec::new();

        // end the immutable borrow of self.pat here:
        let spans: Vec<(usize, usize)> = self
            .pat
            .find_iter(text)
            .map(|m| (m.start(), m.end()))
            .collect();

        for (start, end) in spans {
            let piece = &text[start..end];

            // byte-level transform: bytes -> "unicode-safe" chars
            let mut transformed = String::with_capacity(piece.len());
            for b in piece.as_bytes() {
                let ch = self
                    .byte_encoder
                    .get(b)
                    .ok_or_else(|| anyhow!("Missing byte encoder for byte {}", b))?;
                transformed.push(*ch);
            }

            // run BPE on the transformed piece
            for bpe_tok in self.bpe(&transformed) {
                if let Some(&id) = self.encoder.get(&bpe_tok) {
                    ids.push(id);
                } else {
                    // fallback: break unknown into its constituent chars (safe for GPT-2 vocabs)
                    for ch in bpe_tok.chars() {
                        let s = ch.to_string();
                        if let Some(&id) = self.encoder.get(&s) {
                            ids.push(id);
                        } else {
                            bail!("Unknown token not found in vocab: {:?}", s);
                        }
                    }
                }
            }
        }

        Ok(ids)
    }

    /// Decode token IDs back to user-visible text.
    pub fn decode(&self, tokens: &[usize]) -> Result<String> {
        let mut bytes: Vec<u8> = Vec::new();

        for id in tokens {
            let s = self
                .decoder
                .get(id)
                .ok_or_else(|| anyhow!("Token id {} not in decoder", id))?;

            for ch in s.chars() {
                let b = self
                    .byte_decoder
                    .get(&ch)
                    .ok_or_else(|| anyhow!("Missing byte for char {:?}", ch))?;
                bytes.push(*b);
            }
        }

        // GPT-2’s byte trick preserves original bytes; this should be valid UTF-8.
        let text = String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).to_string());
        Ok(text)
    }

    // byte-pair algo
    fn bpe(&mut self, token: &str) -> Vec<String> {
        if let Some(cached) = self.bpe_cache.get(token) {
            return cached.clone();
        }

        // A token is initially split into individual "characters" of the transformed alphabet.
        let mut word: Vec<String> = token.chars().map(|c| c.to_string()).collect();
        if word.len() == 1 {
            self.bpe_cache.insert(token.to_string(), word.clone());
            return word;
        }

        let mut pairs = get_pairs(&word);

        loop {
            // pick the lowest-rank pair present in the word
            let mut min_rank: Option<(usize, (String, String))> = None;
            for (a, b) in &pairs {
                if let Some(&rank) = self.bpe_ranks.get(&(a.clone(), b.clone())) {
                    match &mut min_rank {
                        None => min_rank = Some((rank, (a.clone(), b.clone()))),
                        Some((best_rank, _)) if rank < *best_rank => {
                            min_rank = Some((rank, (a.clone(), b.clone())))
                        }
                        _ => {}
                    }
                }
            }

            let best = match min_rank {
                None => break, // no more applicable merges
                Some((_, pair)) => pair,
            };

            // merge all occurrences of best pair
            let (first, second) = (&best.0, &best.1);
            let mut i = 0usize;
            let mut new_word: Vec<String> = Vec::with_capacity(word.len());

            while i < word.len() {
                if i + 1 < word.len() && &word[i] == first && &word[i + 1] == second {
                    let merged = format!("{}{}", word[i], word[i + 1]);
                    new_word.push(merged);
                    i += 2;
                } else {
                    new_word.push(word[i].clone());
                    i += 1;
                }
            }

            word = new_word;
            if word.len() == 1 {
                break;
            }
            pairs = get_pairs(&word);
        }

        self.bpe_cache.insert(token.to_string(), word.clone());
        word
    }
}

// Collect all adjacent pairs in a word.
fn get_pairs(word: &[String]) -> HashSet<(String, String)> {
    let mut pairs = HashSet::new();
    if word.len() < 2 {
        return pairs;
    }
    for i in 0..(word.len() - 1) {
        pairs.insert((word[i].clone(), word[i + 1].clone()));
    }
    pairs
}

/// GPT-2 "bytes -> unique unicode" mapping and its inverse.
///
/// Port of OpenAI's encoder.py `bytes_to_unicode()`.
fn bytes_to_unicode() -> (HashMap<u8, char>, HashMap<char, u8>) {
    let mut bs: Vec<u16> = (b'!' as u16..=b'~' as u16).collect(); // 33..126
    bs.extend(0x00A1..=0x00AC); // 161..172
    bs.extend(0x00AE..=0x00FF); // 174..255

    let mut cs = bs.clone();
    let mut n: u16 = 0;
    for b in 0u16..=255 {
        if !bs.contains(&b) {
            bs.push(b);
            cs.push(256 + n);
            n += 1;
        }
    }

    let mut byte_encoder: HashMap<u8, char> = HashMap::with_capacity(256);
    let mut byte_decoder: HashMap<char, u8> = HashMap::with_capacity(256);
    for (b, c) in bs.into_iter().zip(cs.into_iter()) {
        let ch = char::from_u32(c as u32).unwrap();
        byte_encoder.insert(b as u8, ch);
        byte_decoder.insert(ch, b as u8);
    }

    (byte_encoder, byte_decoder)
}

// // demo
// fn main() -> Result<()> {
//     let args: Vec<String> = std::env::args().collect();
//     let vocab = args.get(1).map(String::as_str).unwrap_or("./data/vocab.json");
//     let merges = args.get(2).map(String::as_str).unwrap_or("./data/merges.txt");

//     let mut tok = Gpt2Tokenizer::from_files(vocab, merges)?;

//     let input = "Hello, world! I can't believe it's working w/ emojis: 🤖🔥";
//     let ids = tok.encode(input)?;
//     println!("Encoded IDs: {:?}", ids);

//     let roundtrip = tok.decode(&ids)?;
//     println!("Decoded text: {}", roundtrip);

//     Ok(())
// }