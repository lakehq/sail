pub mod definition;
pub mod scheduling;

#[expect(clippy::allow_attributes)]
pub mod r#gen {
    tonic::include_proto!("sail.task");
}
