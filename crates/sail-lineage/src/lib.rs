//! [OpenLineage](https://openlineage.io/) integration for Sail.
//!
//! Datasets are tracked during plan resolution. Data sources are registered with
//! the [`LineageService`] when they are created, and the inputs of a query are found
//! by looking up the table scans in the final logical plan. Outputs are recorded
//! when the write, delete, or merge plans are resolved. An OpenLineage run is then
//! started for each query execution, and the run is finished when the result stream
//! of the query ends. Query runs are children of an application run for each session,
//! which is finished when the session is deleted or the server shuts down.

mod dataset;
mod error;
mod event;
mod execute;
mod jdbc;
mod options;
mod plan;
mod service;
mod stream;
mod transport;

pub use dataset::{DatasetIdentifier, LineageDataset};
pub use event::LifecycleStateChange;
pub use execute::execute_with_lineage;
pub use jdbc::jdbc_input_datasets;
pub use options::LineageSessionOptions;
pub use plan::{LineageOperation, LineagePlan};
pub use service::LineageService;
