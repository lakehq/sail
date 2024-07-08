use framework_common::python::init_python;
use framework_spark_connect::entrypoint::serve;
use framework_telemetry::telemetry::init_telemetry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;
    init_python()?;
    serve().await
}
