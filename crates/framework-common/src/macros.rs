#[macro_export]
macro_rules! unwrap_or {
    ($opt:expr, $default:expr) => {
        match $opt {
            Some(value) => value,
            None => $default,
        }
    };
}
