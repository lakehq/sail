use sail_telemetry::telemetry::init_telemetry;

pub fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(sail_execution::run_worker())?;

    fastrace::flush();

    Ok(())
}
