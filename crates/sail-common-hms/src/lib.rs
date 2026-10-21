pub mod hms {
    include!(concat!(env!("OUT_DIR"), "/hms.rs"));
}

#[cfg(test)]
mod tests;
