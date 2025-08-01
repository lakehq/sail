mod error;

use sail_common::config::RuntimeConfig;
use tokio::runtime::{Handle, Runtime};

use crate::error::{RuntimeError, RuntimeResult};

#[derive(Debug)]
pub struct RuntimeManager {
    primary: Runtime,
    cpu: Runtime,
    use_aware_object_store: bool,
}

impl RuntimeManager {
    pub fn try_new(config: &RuntimeConfig, name: &str) -> RuntimeResult<Self> {
        let primary = Self::build_runtime(config.stack_size, name)?;
        let cpu = Self::build_cpu_runtime(config.stack_size, name)?;

        let primary_metrics = primary.handle().metrics();
        let cpu_metrics = cpu.handle().metrics();

        println!(
            "Primary Runtime - Workers: {}",
            primary_metrics.num_workers(),
        );
        println!("CPU Runtime - Workers: {}", cpu_metrics.num_workers(),);

        // use std::str::FromStr;
        // if std::env::var("SAIL_USE_CONSOLE_SUBSCRIBER")
        //     .is_ok_and(|v| bool::from_str(&v).unwrap_or(false))
        // {
        //     println!("[sail_cli::runner] Initializing runtime logging");
        //     let cpu_handle = cpu.handle().clone();
        //     let primary_handle = primary.handle().clone();
        //
        //     primary.spawn(async move {
        //         let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        //         loop {
        //             interval.tick().await;
        //
        //             let cpu_metrics = cpu_handle.metrics();
        //             let primary_metrics = primary_handle.metrics();
        //
        //             let cpu_busy: std::time::Duration = (0..cpu_metrics.num_workers())
        //                 .map(|i| cpu_metrics.worker_total_busy_duration(i))
        //                 .sum();
        //
        //             let primary_busy: std::time::Duration = (0..primary_metrics.num_workers())
        //                 .map(|i| primary_metrics.worker_total_busy_duration(i))
        //                 .sum();
        //
        //             println!("=== Runtime Metrics ===");
        //             println!(
        //                 "CPU Runtime - Tasks: {}, Workers: {}, Total Busy: {:?}",
        //                 cpu_metrics.num_alive_tasks(),
        //                 cpu_metrics.num_workers(),
        //                 cpu_busy
        //             );
        //
        //             println!(
        //                 "Primary Runtime - Tasks: {}, Workers: {}, Total Busy: {:?}",
        //                 primary_metrics.num_alive_tasks(),
        //                 primary_metrics.num_workers(),
        //                 primary_busy
        //             );
        //             println!("===================\n");
        //         }
        //     });
        // }
        Ok(Self {
            primary,
            cpu,
            use_aware_object_store: config.enable_secondary,
        })
    }

    pub fn handle(&self) -> RuntimeHandle {
        let primary = self.primary.handle().clone();
        let cpu = self.cpu.handle().clone();

        RuntimeHandle {
            primary,
            cpu,
            use_aware_object_store: self.use_aware_object_store,
        }
    }

    fn build_runtime(stack_size: usize, name: &str) -> RuntimeResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name(format!("Primary-{name}"))
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .map_err(|e| RuntimeError::internal(e.to_string()))
    }

    fn build_cpu_runtime(stack_size: usize, name: &str) -> RuntimeResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name(format!("CPU-{name}"))
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .map_err(|e| RuntimeError::internal(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeHandle {
    primary: Handle,
    cpu: Handle,
    use_aware_object_store: bool,
}

impl RuntimeHandle {
    pub fn primary(&self) -> &Handle {
        &self.primary
    }

    pub fn cpu(&self) -> &Handle {
        &self.cpu
    }

    pub fn use_aware_object_store(&self) -> bool {
        self.use_aware_object_store
    }
}
