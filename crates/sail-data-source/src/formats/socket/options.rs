use std::collections::HashMap;

use datafusion_common::Result;

use crate::options::internal::SocketReadOptions;
use crate::options::{load_default_options, load_options};

#[derive(Debug, Clone, Default)]
pub struct TableSocketOptions {
    pub host: String,
    pub port: u16,
    pub max_batch_size: usize,
    pub timeout_sec: u64,
}

fn apply_socket_read_options(from: SocketReadOptions, to: &mut TableSocketOptions) -> Result<()> {
    let SocketReadOptions {
        host,
        port,
        max_batch_size,
        timeout_sec,
    } = from;
    if let Some(v) = host {
        to.host = v;
    }
    if let Some(v) = port {
        to.port = v;
    }
    if let Some(v) = max_batch_size {
        to.max_batch_size = v;
    }
    if let Some(v) = timeout_sec {
        to.timeout_sec = v;
    }
    Ok(())
}

pub fn resolve_socket_read_options(
    options: Vec<HashMap<String, String>>,
) -> Result<TableSocketOptions> {
    let mut socket_options = TableSocketOptions::default();
    apply_socket_read_options(load_default_options()?, &mut socket_options)?;
    for opt in options {
        apply_socket_read_options(load_options(opt)?, &mut socket_options)?;
    }
    Ok(socket_options)
}
