use sail_common::config::AppConfig;

pub struct DriverOptions {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: Option<u16>,
    // TODO: support dynamic worker allocation
    pub worker_count_per_job: usize,
    pub job_output_buffer: usize,
}

impl From<&AppConfig> for DriverOptions {
    fn from(config: &AppConfig) -> Self {
        Self {
            enable_tls: config.network.enable_tls,
            driver_listen_host: config.driver.listen_host.clone(),
            driver_listen_port: config.driver.listen_port,
            driver_external_host: config.driver.external_host.clone(),
            driver_external_port: config.driver.external_port,
            worker_count_per_job: config.driver.worker_count_per_job,
            job_output_buffer: config.driver.job_output_buffer,
        }
    }
}
