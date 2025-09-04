use std::collections::HashMap;

/// GPT-2 bytes -> unique unicode mapping and its inverse.
/// Port of OpenAI's encoder.py `bytes_to_unicode()`.
pub fn bytes_to_unicode() -> (HashMap<u8, char>, HashMap<char, u8>) {
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

