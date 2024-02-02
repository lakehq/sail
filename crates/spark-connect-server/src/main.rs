use spark_connect_server::server::SparkConnectServer;
use spark_connect_server::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Use TLS
    let address = "127.0.0.1:50051".parse()?;

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

    tonic::transport::Server::builder()
        .add_service(reflect_server)
        .add_service(health_server)
        .add_service(SparkConnectServiceServer::new(server))
        .serve(address)
        .await?;

    Ok(())
}
