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
