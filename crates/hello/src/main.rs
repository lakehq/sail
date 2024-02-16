use framework_telemetry::telemetry::init_telemetry;
use tracing::{error, error_span, info, span};

fn main() {
    init_telemetry();
    info!("Meow Meow Meow");
}
