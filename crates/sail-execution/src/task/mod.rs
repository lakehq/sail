pub mod definition;
pub mod scheduling;
mod statistics;

pub mod r#gen {
    tonic::include_proto!("sail.task");
}
