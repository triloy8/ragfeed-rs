pub fn heuristic_lists(n: i64) -> i32 {
    if n <= 0 { return 50; }
    let k = (n as f64).sqrt().round() as i32;
    k.clamp(50, 8192)
}

