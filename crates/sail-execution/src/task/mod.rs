pub mod definition;
pub mod scheduling;

#[allow(clippy::all)]
pub mod gen {
    tonic::include_proto!("sail.task");
}
