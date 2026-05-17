use std::ffi::CString;
use std::path::Path;

use pyo3::ffi::{PyUnicode_AsWideCharString, PyUnicode_FromString};

fn usage_and_exit() -> ! {
    eprintln!(
        "Usage: cargo run -p sail-cli --example python_preinit_argv_ub -- [--iters N] [--arg-bytes N] [--print-every N]\n\
         \n\
         Reproduces the Sail CLI pattern of calling Python C-API (PyUnicode_*) before the\n\
         interpreter is initialized, and intentionally leaks the returned allocations.\n\
         \n\
         Options:\n\
           --iters N        Number of iterations (default: 200000)\n\
           --arg-bytes N    Size of the argv string each iteration (default: 4096)\n\
           --print-every N  Print RSS every N iterations (default: 10000)\n"
    );
    std::process::exit(2);
}

fn parse_u64(name: &str, value: &str) -> u64 {
    value.parse::<u64>().unwrap_or_else(|_| {
        eprintln!("Invalid value for {name}: {value}");
        usage_and_exit();
    })
}

fn rss_kb_linux() -> Option<u64> {
    // Linux-only MRE: read RSS from /proc/self/status.
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.trim().split_whitespace().next()?;
            return kb.parse::<u64>().ok();
        }
    }
    None
}

fn main() {
    let mut iters: u64 = 200_000;
    let mut arg_bytes: u64 = 4_096;
    let mut print_every: u64 = 10_000;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iters" => {
                iters = parse_u64("--iters", args.next().as_deref().unwrap_or(""));
            }
            "--arg-bytes" => {
                arg_bytes = parse_u64("--arg-bytes", args.next().as_deref().unwrap_or(""));
            }
            "--print-every" => {
                print_every = parse_u64("--print-every", args.next().as_deref().unwrap_or(""));
            }
            "--help" | "-h" => usage_and_exit(),
            other => {
                eprintln!("Unknown argument: {other}");
                usage_and_exit();
            }
        }
    }

    if arg_bytes == 0 || arg_bytes > (usize::MAX as u64) {
        eprintln!("--arg-bytes must be between 1 and usize::MAX");
        std::process::exit(2);
    }
    if print_every == 0 {
        eprintln!("--print-every must be >= 1");
        std::process::exit(2);
    }

    // Ensure we're not accidentally “fixing” the repro by initializing Python via PyO3.
    // This example intentionally calls the Python C-API *before* initialization.

    let payload = "A".repeat(arg_bytes as usize);
    let payload = CString::new(payload).expect("payload must not contain NUL");

    eprintln!(
        "mre: pid={} iters={} arg_bytes={} print_every={}",
        std::process::id(),
        iters,
        arg_bytes,
        print_every
    );
    eprintln!(
        "mre: python pre-init={} (expected true)",
        // If this file exists, we're likely on Linux; the example still works elsewhere.
        Path::new("/proc/self/status").exists()
    );

    let mut last_rss = rss_kb_linux();
    if let Some(rss) = last_rss {
        eprintln!("mre: rss_kb(start)={rss}");
    } else {
        eprintln!("mre: rss_kb(start)=<unavailable>");
    }

    for i in 1..=iters {
        unsafe {
            // Matches the pattern in `crates/sail-cli/src/main.rs`:
            // - Call `PyUnicode_FromString` before interpreter init
            // - Call `PyUnicode_AsWideCharString`
            // - Leak both the returned wchar_t* and the PyObject reference (no DECREF)
            let obj = PyUnicode_FromString(payload.as_ptr());
            if obj.is_null() {
                eprintln!("mre: PyUnicode_FromString returned NULL at iter={i}");
                std::process::exit(1);
            }
            let wide = PyUnicode_AsWideCharString(obj, std::ptr::null_mut());
            if wide.is_null() {
                eprintln!("mre: PyUnicode_AsWideCharString returned NULL at iter={i}");
                std::process::exit(1);
            }
        }

        if i % print_every == 0 {
            let rss = rss_kb_linux();
            if rss != last_rss {
                if let Some(rss) = rss {
                    eprintln!("mre: rss_kb(iter={i})={rss}");
                } else {
                    eprintln!("mre: rss_kb(iter={i})=<unavailable>");
                }
                last_rss = rss;
            }
        }
    }

    let rss = rss_kb_linux();
    if let Some(rss) = rss {
        eprintln!("mre: rss_kb(end)={rss}");
    } else {
        eprintln!("mre: rss_kb(end)=<unavailable>");
    }
}

