use log::error;

pub mod error;
pub mod file_listing_cache;
pub mod file_metadata_cache;
pub mod file_statistics_cache;

#[allow(dead_code)]
pub(crate) fn try_parse_memory_limit(limit: &str) -> Option<usize> {
    let (number, unit) = limit.split_at(limit.len() - 1);
    let number: f64 = match number.parse() {
        Ok(n) => n,
        Err(_) => {
            error!("Memory limit not set! Failed to parse number from '{limit}'");
            return None;
        }
    };
    match unit {
        "K" => Some((number * 1024.0) as usize),
        "M" => Some((number * 1024.0 * 1024.0) as usize),
        "G" => Some((number * 1024.0 * 1024.0 * 1024.0) as usize),
        _ => {
            error!("Memory limit not set! Unsupported unit '{unit}' in memory limit '{limit}'.");
            None
        }
    }
}
