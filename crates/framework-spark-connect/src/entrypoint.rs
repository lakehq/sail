use tonic::codegen::http;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{debug, Span};

use crate::server::SparkConnectServer;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    // A secure connection can be handled by a gateway in production.
    let address = "0.0.0.0:50051".parse()?;

    let (mut health_reporter, health_server) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SparkConnectServiceServer<SparkConnectServer>>()
        .await;

    let reflect_server = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(crate::spark::connect::FILE_DESCRIPTOR_SET)
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
