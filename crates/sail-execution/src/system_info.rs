use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use crate::profiling::{ProfileEvent, ProfileHandle};

/// Sample this process and the host's network interfaces once per second.
/// Network counters include traffic from other processes on the same host.
pub(crate) fn start_system_info_profile(profile: ProfileHandle) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut previous = None;
        loop {
            interval.tick().await;
            match sample() {
                Ok((cpu_seconds, rss_bytes, host_network_tx_bytes)) => {
                    let now = Instant::now();
                    let cpu_percent = previous.map(|(at, cpu, _): (Instant, f64, u64)| {
                        100.0 * (cpu_seconds - cpu) / now.duration_since(at).as_secs_f64()
                    });
                    let network_tx_delta_bytes =
                        previous.map(|(_, _, tx)| host_network_tx_bytes.saturating_sub(tx));
                    profile.record(ProfileEvent::SystemSample {
                        pid: std::process::id(),
                        process_cpu_percent: cpu_percent,
                        process_rss_bytes: rss_bytes,
                        host_network_tx_bytes,
                        host_network_tx_delta_bytes: network_tx_delta_bytes,
                    });
                    previous = Some((now, cpu_seconds, host_network_tx_bytes));
                }
                Err(error) => profile.diagnostic("system_sample_error", || error.to_string()),
            }
        }
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn sample() -> std::io::Result<(f64, u64, u64)> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes a complete rusage on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: getrusage succeeded above.
    let usage = unsafe { usage.assume_init() };
    let cpu_seconds = (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0;
    Ok((cpu_seconds, resident_bytes()?, network_tx_bytes()?))
}

#[cfg(target_os = "linux")]
fn resident_bytes() -> std::io::Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    let pages = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| std::io::Error::other("missing resident pages in /proc/self/statm"))?;
    let pages = pages.parse::<u64>().map_err(std::io::Error::other)?;
    // SAFETY: sysconf is read-only and does not access application memory.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(pages.saturating_mul(page_size as u64))
}

#[cfg(target_os = "macos")]
fn resident_bytes() -> std::io::Result<u64> {
    let mut task = std::mem::MaybeUninit::<libc::proc_taskinfo>::uninit();
    // SAFETY: proc_pidinfo writes a complete proc_taskinfo when it returns its size.
    let size = unsafe {
        libc::proc_pidinfo(
            std::process::id() as i32,
            libc::PROC_PIDTASKINFO,
            0,
            task.as_mut_ptr().cast(),
            std::mem::size_of::<libc::proc_taskinfo>() as i32,
        )
    };
    if size != std::mem::size_of::<libc::proc_taskinfo>() as i32 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: proc_pidinfo returned the complete structure above.
    Ok(unsafe { task.assume_init() }.pti_resident_size)
}

#[cfg(target_os = "linux")]
fn network_tx_bytes() -> std::io::Result<u64> {
    let counters = std::fs::read_to_string("/proc/net/dev")?;
    counters.lines().skip(2).try_fold(0u64, |sum, line| {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let bytes = fields
            .get(9)
            .ok_or_else(|| std::io::Error::other("missing transmitted bytes in /proc/net/dev"))?;
        Ok(sum.saturating_add(bytes.parse::<u64>().map_err(std::io::Error::other)?))
    })
}

#[cfg(target_os = "macos")]
fn network_tx_bytes() -> std::io::Result<u64> {
    let mut addresses = std::ptr::null_mut();
    // SAFETY: getifaddrs initializes addresses on success; freeifaddrs releases it below.
    if unsafe { libc::getifaddrs(&mut addresses) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut total = 0u64;
    let mut current = addresses;
    while !current.is_null() {
        // SAFETY: the linked list remains valid until freeifaddrs is called.
        let address = unsafe { &*current };
        if !address.ifa_addr.is_null()
            // SAFETY: ifa_addr points to a valid sockaddr for this list entry.
            && unsafe { (*address.ifa_addr).sa_family as i32 } == libc::AF_LINK
            && !address.ifa_data.is_null()
        {
            // SAFETY: AF_LINK entries expose if_data through ifa_data.
            let data = unsafe { &*address.ifa_data.cast::<libc::if_data>() };
            total = total.saturating_add(u64::from(data.ifi_obytes));
        }
        current = address.ifa_next;
    }
    // SAFETY: addresses was allocated by getifaddrs above.
    unsafe { libc::freeifaddrs(addresses) };
    Ok(total)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn sample() -> std::io::Result<(f64, u64, u64)> {
    Err(std::io::Error::other(
        "system profiling is unsupported on this platform",
    ))
}
