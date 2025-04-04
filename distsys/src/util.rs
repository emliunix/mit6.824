// hash function

pub(crate) fn hash(key: &str) -> u32 {
    let mut h: u32 = 0;
    for c in key.chars() {
        h = (h << 5).wrapping_sub(h).wrapping_add(c as u32);
    }
    h
}
