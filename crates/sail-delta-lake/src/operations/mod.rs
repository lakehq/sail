pub use datafusion::physical_plan::common::collect as collect_sendable_stream;
use deltalake::{DeltaResult, DeltaTable};

pub use self::load::LoadBuilder;
pub use self::write::WriteBuilder;

pub mod add_column;
mod cast;
mod cdc;
// pub mod constraints;
// pub mod delete;
pub mod load;
pub mod load_cdf;
// pub mod merge;
// pub mod optimize;
// pub mod update;
pub mod write;

pub struct SailDeltaOps(pub DeltaTable);

impl SailDeltaOps {
    /// Create a new SailDeltaOps from a DeltaTable
    pub fn new(table: DeltaTable) -> Self {
        Self(table)
    }

    /// Load data from the Delta table
    pub fn load(self) -> DeltaResult<LoadBuilder> {
        let snapshot = self.0.snapshot()?;
        Ok(LoadBuilder::new(self.0.log_store(), snapshot.clone()))
    }

    /// Write data to the Delta table
    pub fn write(self) -> WriteBuilder {
        let snapshot = self.0.snapshot().ok().cloned();
        WriteBuilder::new(self.0.log_store(), snapshot)
    }
}

impl From<deltalake::operations::DeltaOps> for SailDeltaOps {
    fn from(ops: deltalake::operations::DeltaOps) -> Self {
        Self(ops.0)
    }
}

impl From<DeltaTable> for SailDeltaOps {
    fn from(table: DeltaTable) -> Self {
        Self(table)
    }
}

impl From<SailDeltaOps> for DeltaTable {
    fn from(ops: SailDeltaOps) -> Self {
        ops.0
    }
}

impl AsRef<DeltaTable> for SailDeltaOps {
    fn as_ref(&self) -> &DeltaTable {
        &self.0
    }
}

// Future operations to be implemented:
// use self::optimize::OptimizeBuilder;
// use self::{
//     constraints::ConstraintBuilder, datafusion_utils::Expression, delete::DeleteBuilder,
//     drop_constraints::DropConstraintBuilder, load_cdf::CdfLoadBuilder,
//     merge::MergeBuilder, update::UpdateBuilder, write::WriteBuilder,
// };
