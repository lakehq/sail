use datafusion_common::plan_err;

pub fn char_to_u8(c: char, option: &str) -> datafusion_common::Result<u8> {
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        plan_err!("invalid {option} character '{c}': must be an ASCII character")
    }
}
