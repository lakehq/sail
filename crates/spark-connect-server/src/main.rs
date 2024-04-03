use tonic::codegen::http;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{debug, Span};

use framework_telemetry::telemetry::init_telemetry;
use spark_connect_server::server::SparkConnectServer;
use spark_connect_server::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

use pyo3::prelude::*;
use pyo3::types::IntoPyDict;

// fn main() -> PyResult<()> {
//     Python::with_gil(|py| {
//         let sys = py.import_bound("sys")?;
//         let version: String = sys.getattr("version")?.extract()?;
//
//         let locals = [("os", py.import_bound("os")?)].into_py_dict_bound(py);
//         let code = "os.getenv('USER') or os.getenv('USERNAME') or 'Unknown'";
//         let user: String = py.eval_bound(code, None, Some(&locals))?.extract()?;
//
//         println!("Hello {}, I'm Python {}", user, version);
//
//         let cloudpickle = PyModule::import_bound(py, "pyspark.cloudpickle")
//             .expect("Unable to import 'pyspark.cloudpickle'")
//             .getattr("loads")
//             .unwrap();
//
//         Ok(())
//     })
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_telemetry()?;

    // A secure connection can be handled by a gateway in production.
    let address = "0.0.0.0:50051".parse()?;

    let (mut health_reporter, health_server) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SparkConnectServiceServer<SparkConnectServer>>()
        .await;

    let reflect_server = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(
            spark_connect_server::spark::connect::FILE_DESCRIPTOR_SET,
        )
        .build()?;

    let server = SparkConnectServer::default();

    let layer = ServiceBuilder::new()
        .layer(
            TraceLayer::new_for_grpc()
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_request(|request: &http::Request<_>, _: &Span| {
                    debug!("{:?}", request);
                })
                .on_response(|response: &http::response::Response<_>, _, _: &Span| {
                    debug!("{:?}", response);
                }),
        )
        .into_inner();

    tonic::transport::Server::builder()
        .layer(layer)
        .add_service(reflect_server)
        .add_service(health_server)
        .add_service(SparkConnectServiceServer::new(server))
        .serve(address)
        .await?;

    Ok(())
}
