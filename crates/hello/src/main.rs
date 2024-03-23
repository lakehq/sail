use framework_telemetry::telemetry::init_telemetry;
use std::{thread, time::Duration};
use tracing::{error, error_span, info, info_span, instrument, span, trace, warn, debug};

#[instrument]
#[inline]
fn expensive_work() -> &'static str {
    span!(tracing::Level::INFO, "expensive_step_1")
        .in_scope(|| thread::sleep(Duration::from_millis(25)));
    span!(tracing::Level::INFO, "expensive_step_2")
        .in_scope(|| thread::sleep(Duration::from_millis(25)));

    "success"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Sleep for a while for the telemetry to be initialized
    tokio::time::sleep(tokio::time::Duration::from_millis(10000)).await;

    init_telemetry()?;
    println!("Hello, world!");

    trace!(
        meow_1 = "Meow1",
        meow_2 = "Meow2",
        "Trace Log: Meow Meow Meow"
    );
    debug!(
        meow_1 = "Meow1",
        meow_2 = "Meow2",
        "Debug Log: Meow Meow Meow"
    );
    info!(
        meow_1 = "Meow1",
        meow_2 = "Meow2",
        "Info Log: Meow Meow Meow"
    );
    error!(
        meow_1 = "Meow1",
        meow_2 = "Meow2",
        "Error Log: Meow Meow Meow"
    );

    {
        let root = error_span!("Error Span: Meow Meow");
        let _enter = root.enter();
        error!("Meow Meow Meow I am Error Log in Error Span");
        trace!("Trace: ");
    }

    {
        let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
        let _enter = root.enter();

        let work_result = expensive_work();

        span!(tracing::Level::INFO, "Info Span: Meow!!!!")
            .in_scope(|| thread::sleep(Duration::from_millis(10)));

        info_span!("Info Span: Meow");
        error_span!("Error Span: Meow");

        warn!("Warn Log: About to exit!");
        trace!("status: {}", work_result);
    }

    Ok(())
}
