use std::time::Duration;

use sysinfo::{Networks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::task::JoinHandle;

use crate::profiling::{ProfileEvent, ProfileHandle};

/// Sample this process and the host's network interfaces once per second.
/// Network counters include traffic from other processes on the same host.
pub(crate) fn start_system_info_profile(profile: ProfileHandle) -> JoinHandle<()> {
    tokio::spawn(async move {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        let mut networks = Networks::new();
        let process_refresh = ProcessRefreshKind::nothing().with_cpu().with_memory();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut has_previous_sample = false;

        loop {
            interval.tick().await;
            system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                process_refresh,
            );
            networks.refresh(true);

            let Some(process) = system.process(pid) else {
                profile.diagnostic("system_sample_error", || {
                    format!("process {pid} was not found by sysinfo")
                });
                continue;
            };

            let (
                host_network_rx_bytes,
                host_network_rx_delta,
                host_network_tx_bytes,
                host_network_tx_delta,
            ) = networks
                .iter()
                .fold((0u64, 0u64, 0u64, 0u64), |totals, (_, data)| {
                    (
                        totals.0.saturating_add(data.total_received()),
                        totals.1.saturating_add(data.received()),
                        totals.2.saturating_add(data.total_transmitted()),
                        totals.3.saturating_add(data.transmitted()),
                    )
                });

            profile.record(ProfileEvent::SystemSample {
                pid: std::process::id(),
                process_cpu_percent: has_previous_sample.then_some(f64::from(process.cpu_usage())),
                process_rss_bytes: process.memory(),
                host_network_rx_bytes,
                host_network_rx_delta_bytes: has_previous_sample.then_some(host_network_rx_delta),
                host_network_tx_bytes,
                host_network_tx_delta_bytes: has_previous_sample.then_some(host_network_tx_delta),
            });
            has_previous_sample = true;
        }
    })
}
