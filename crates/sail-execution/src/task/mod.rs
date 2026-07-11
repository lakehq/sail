pub mod definition;
pub mod scheduling;

pub mod r#gen {
    tonic::include_proto!("sail.task");
}
