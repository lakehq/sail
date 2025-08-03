use log::error;

pub mod error;
pub mod file_metadata_cache;
pub mod list_file_cache;

pub(crate) fn try_parse_memory_limit(limit: &str) -> Option<u64> {
    let (number, unit) = limit.split_at(limit.len() - 1);
    let number: f64 = match number.parse() {
        Ok(n) => n,
        Err(_) => {
            error!("Memory limit not set! Failed to parse number from '{limit}'");
            return None;
        }
    };
    match unit {
        "K" => Some((number * 1024.0) as u64),
        "M" => Some((number * 1024.0 * 1024.0) as u64),
        "G" => Some((number * 1024.0 * 1024.0 * 1024.0) as u64),
        _ => {
            error!("Memory limit not set! Unsupported unit '{unit}' in memory limit '{limit}'.");
            None
        }
    }
}

pub(crate) fn try_parse_non_zero_u64(number: &str) -> Option<u64> {
    match number.parse::<u64>() {
        Ok(n) => {
            if n == 0 {
                None
            } else {
                Some(n)
            }
        }
        Err(_) => {
            error!("Failed to parse '{number}' as u64");
            None
        }
    }
}
