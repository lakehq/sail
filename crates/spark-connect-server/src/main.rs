use tonic::codegen::http;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{debug, Span};

use framework_telemetry::telemetry::init_telemetry;
use spark_connect_server::server::SparkConnectServer;
use spark_connect_server::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, IntoPyDict};

// fn main() -> PyResult<()> {
//     Python::with_gil(|py| {
//         let sys = py.import_bound("sys")?;
//         let version: String = sys.getattr("version")?.extract()?;
//         let locals = [("os", py.import_bound("os")?)].into_py_dict_bound(py);
//         let code = "os.getenv('USER') or os.getenv('USERNAME') or 'Unknown'";
//         let user: String = py.eval_bound(code, None, Some(&locals))?.extract()?;
//         println!("Hello {}, I'm Python {}", user, version);
//
//         let bytes: Vec<u8> = vec![
//             128, 5, 149, 102, 2, 0, 0, 0, 0, 0, 0, 140, 31, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 148, 140, 14, 95, 109, 97, 107, 101, 95, 102, 117, 110, 99, 116, 105, 111, 110, 148, 147, 148, 40, 104, 0, 140, 13, 95, 98, 117, 105, 108, 116, 105, 110, 95, 116, 121, 112, 101, 148, 147, 148, 140, 8, 67, 111, 100, 101, 84, 121, 112, 101, 148, 133, 148, 82, 148, 40, 75, 1, 75, 0, 75, 0, 75, 1, 75, 2, 75, 67, 67, 20, 124, 0, 100, 0, 117, 0, 114, 6, 100, 0, 83, 0, 124, 0, 100, 1, 23, 0, 83, 0, 148, 78, 75, 1, 134, 148, 41, 140, 1, 120, 148, 133, 148, 140, 104, 47, 85, 115, 101, 114, 115, 47, 114, 47, 68, 101, 115, 107, 116, 111, 112, 47, 108, 97, 107, 101, 115, 97, 105, 108, 47, 102, 114, 97, 109, 101, 119, 111, 114, 107, 47, 99, 114, 97, 116, 101, 115, 47, 115, 112, 97, 114, 107, 45, 99, 111, 110, 110, 101, 99, 116, 45, 115, 101, 114, 118, 101, 114, 47, 101, 120, 97, 109, 112, 108, 101, 115, 47, 112, 121, 115, 112, 97, 114, 107, 45, 115, 113, 108, 47, 115, 114, 99, 47, 97, 112, 112, 47, 95, 95, 109, 97, 105, 110, 95, 95, 46, 112, 121, 148, 140, 7, 97, 100, 100, 95, 111, 110, 101, 148, 75, 9, 67, 6, 8, 2, 4, 1, 8, 1, 148, 41, 41, 116, 148, 82, 148, 125, 148, 40, 140, 11, 95, 95, 112, 97, 99, 107, 97, 103, 101, 95, 95, 148, 140, 3, 97, 112, 112, 148, 140, 8, 95, 95, 110, 97, 109, 101, 95, 95, 148, 140, 8, 95, 95, 109, 97, 105, 110, 95, 95, 148, 140, 8, 95, 95, 102, 105, 108, 101, 95, 95, 148, 104, 12, 117, 78, 78, 78, 116, 148, 82, 148, 140, 36, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 95, 102, 97, 115, 116, 148, 140, 18, 95, 102, 117, 110, 99, 116, 105, 111, 110, 95, 115, 101, 116, 115, 116, 97, 116, 101, 148, 147, 148, 104, 24, 125, 148, 125, 148, 40, 104, 20, 104, 13, 140, 12, 95, 95, 113, 117, 97, 108, 110, 97, 109, 101, 95, 95, 148, 104, 13, 140, 15, 95, 95, 97, 110, 110, 111, 116, 97, 116, 105, 111, 110, 115, 95, 95, 148, 125, 148, 140, 14, 95, 95, 107, 119, 100, 101, 102, 97, 117, 108, 116, 115, 95, 95, 148, 78, 140, 12, 95, 95, 100, 101, 102, 97, 117, 108, 116, 115, 95, 95, 148, 78, 140, 10, 95, 95, 109, 111, 100, 117, 108, 101, 95, 95, 148, 104, 21, 140, 7, 95, 95, 100, 111, 99, 95, 95, 148, 78, 140, 11, 95, 95, 99, 108, 111, 115, 117, 114, 101, 95, 95, 148, 78, 140, 23, 95, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 95, 115, 117, 98, 109, 111, 100, 117, 108, 101, 115, 148, 93, 148, 140, 11, 95, 95, 103, 108, 111, 98, 97, 108, 115, 95, 95, 148, 125, 148, 117, 134, 148, 134, 82, 48, 140, 17, 112, 121, 115, 112, 97, 114, 107, 46, 115, 113, 108, 46, 116, 121, 112, 101, 115, 148, 140, 11, 73, 110, 116, 101, 103, 101, 114, 84, 121, 112, 101, 148, 147, 148, 41, 129, 148, 134, 148, 46,
//         ];
//
//         let cloudpickle = PyModule::import_bound(py, "pyspark.cloudpickle")
//             .expect("Unable to import 'pyspark.cloudpickle'")
//             .getattr("loads")
//             .unwrap();
//
//         let binary_sequence = PyBytes::new_bound(py, &bytes);
//         let python_function = cloudpickle
//             .call1((binary_sequence, ))?;
//
//         let result = python_function.get_item(0)?.call1((1, ))?; // Call the function with an argument
//         println!("Result: {:?}", result);
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
