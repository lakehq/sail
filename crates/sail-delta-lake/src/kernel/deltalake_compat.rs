use deltalake::kernel::models::{
    DeletionVectorDescriptor as DeltaRsDeletionVectorDescriptor, StorageType as DeltaRsStorageType,
};
use deltalake::kernel::{
    Action as DeltaRsAction, Add as DeltaRsAdd, AddCDCFile as DeltaRsAddCDCFile,
    CheckpointMetadata as DeltaRsCheckpointMetadata, CommitInfo as DeltaRsCommitInfo,
    DomainMetadata as DeltaRsDomainMetadata, IsolationLevel as DeltaRsIsolationLevel,
    Remove as DeltaRsRemove, Sidecar as DeltaRsSidecar, Transaction as DeltaRsTransaction,
};

use crate::kernel::models::{
    Action, Add, AddCDCFile, CheckpointMetadata, CommitInfo, DeletionVectorDescriptor,
    DomainMetadata, IsolationLevel, Remove, Sidecar, StorageType, Transaction,
};

impl From<DeltaRsStorageType> for StorageType {
    fn from(value: DeltaRsStorageType) -> Self {
        match value {
            DeltaRsStorageType::UuidRelativePath => StorageType::UuidRelativePath,
            DeltaRsStorageType::Inline => StorageType::Inline,
            DeltaRsStorageType::AbsolutePath => StorageType::AbsolutePath,
        }
    }
}

impl From<StorageType> for DeltaRsStorageType {
    fn from(value: StorageType) -> Self {
        match value {
            StorageType::UuidRelativePath => DeltaRsStorageType::UuidRelativePath,
            StorageType::Inline => DeltaRsStorageType::Inline,
            StorageType::AbsolutePath => DeltaRsStorageType::AbsolutePath,
        }
    }
}

impl From<&DeltaRsDeletionVectorDescriptor> for DeletionVectorDescriptor {
    fn from(value: &DeltaRsDeletionVectorDescriptor) -> Self {
        Self {
            storage_type: value.storage_type.into(),
            path_or_inline_dv: value.path_or_inline_dv.clone(),
            offset: value.offset,
            size_in_bytes: value.size_in_bytes,
            cardinality: value.cardinality,
        }
    }
}

impl From<DeltaRsDeletionVectorDescriptor> for DeletionVectorDescriptor {
    fn from(value: DeltaRsDeletionVectorDescriptor) -> Self {
        Self::from(&value)
    }
}

impl From<&DeletionVectorDescriptor> for DeltaRsDeletionVectorDescriptor {
    fn from(value: &DeletionVectorDescriptor) -> Self {
        DeltaRsDeletionVectorDescriptor {
            storage_type: value.storage_type.into(),
            path_or_inline_dv: value.path_or_inline_dv.clone(),
            offset: value.offset,
            size_in_bytes: value.size_in_bytes,
            cardinality: value.cardinality,
        }
    }
}

impl From<DeletionVectorDescriptor> for DeltaRsDeletionVectorDescriptor {
    fn from(value: DeletionVectorDescriptor) -> Self {
        Self::from(&value)
    }
}

impl From<IsolationLevel> for DeltaRsIsolationLevel {
    fn from(value: IsolationLevel) -> Self {
        match value {
            IsolationLevel::Serializable => DeltaRsIsolationLevel::Serializable,
            IsolationLevel::WriteSerializable => DeltaRsIsolationLevel::WriteSerializable,
            IsolationLevel::SnapshotIsolation => DeltaRsIsolationLevel::SnapshotIsolation,
        }
    }
}

impl From<DeltaRsIsolationLevel> for IsolationLevel {
    fn from(value: DeltaRsIsolationLevel) -> Self {
        match value {
            DeltaRsIsolationLevel::Serializable => IsolationLevel::Serializable,
            DeltaRsIsolationLevel::WriteSerializable => IsolationLevel::WriteSerializable,
            DeltaRsIsolationLevel::SnapshotIsolation => IsolationLevel::SnapshotIsolation,
        }
    }
}

impl From<&Add> for DeltaRsAdd {
    fn from(value: &Add) -> Self {
        DeltaRsAdd {
            path: value.path.clone(),
            partition_values: value.partition_values.clone(),
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats.clone(),
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone().map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider.clone(),
        }
    }
}

impl From<Add> for DeltaRsAdd {
    fn from(value: Add) -> Self {
        DeltaRsAdd {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
        }
    }
}

impl From<&DeltaRsAdd> for Add {
    fn from(value: &DeltaRsAdd) -> Self {
        Add {
            path: value.path.clone(),
            partition_values: value.partition_values.clone(),
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats.clone(),
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone().map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider.clone(),
        }
    }
}

impl From<DeltaRsAdd> for Add {
    fn from(value: DeltaRsAdd) -> Self {
        Add {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
        }
    }
}

impl From<&Remove> for DeltaRsRemove {
    fn from(value: &Remove) -> Self {
        DeltaRsRemove {
            path: value.path.clone(),
            deletion_timestamp: value.deletion_timestamp,
            data_change: value.data_change,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values.clone(),
            size: value.size,
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone().map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<Remove> for DeltaRsRemove {
    fn from(value: Remove) -> Self {
        DeltaRsRemove {
            path: value.path,
            deletion_timestamp: value.deletion_timestamp,
            data_change: value.data_change,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values,
            size: value.size,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<&DeltaRsRemove> for Remove {
    fn from(value: &DeltaRsRemove) -> Self {
        Remove {
            path: value.path.clone(),
            data_change: value.data_change,
            deletion_timestamp: value.deletion_timestamp,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values.clone(),
            size: value.size,
            tags: value.tags.clone(),
            deletion_vector: value.deletion_vector.clone().map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<DeltaRsRemove> for Remove {
    fn from(value: DeltaRsRemove) -> Self {
        Remove {
            path: value.path,
            data_change: value.data_change,
            deletion_timestamp: value.deletion_timestamp,
            extended_file_metadata: value.extended_file_metadata,
            partition_values: value.partition_values,
            size: value.size,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(Into::into),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
        }
    }
}

impl From<AddCDCFile> for DeltaRsAddCDCFile {
    fn from(value: AddCDCFile) -> Self {
        DeltaRsAddCDCFile {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            data_change: value.data_change,
            tags: value.tags,
        }
    }
}

impl From<DeltaRsAddCDCFile> for AddCDCFile {
    fn from(value: DeltaRsAddCDCFile) -> Self {
        AddCDCFile {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            data_change: value.data_change,
            tags: value.tags,
        }
    }
}

impl From<Transaction> for DeltaRsTransaction {
    fn from(value: Transaction) -> Self {
        DeltaRsTransaction {
            app_id: value.app_id,
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

impl From<DeltaRsTransaction> for Transaction {
    fn from(value: DeltaRsTransaction) -> Self {
        Transaction {
            app_id: value.app_id,
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

impl From<CommitInfo> for DeltaRsCommitInfo {
    fn from(value: CommitInfo) -> Self {
        DeltaRsCommitInfo {
            timestamp: value.timestamp,
            user_id: value.user_id,
            user_name: value.user_name,
            operation: value.operation,
            operation_parameters: value.operation_parameters,
            read_version: value.read_version,
            isolation_level: value.isolation_level.map(Into::into),
            is_blind_append: value.is_blind_append,
            engine_info: value.engine_info,
            info: value.info,
            user_metadata: value.user_metadata,
        }
    }
}

impl From<DeltaRsCommitInfo> for CommitInfo {
    fn from(value: DeltaRsCommitInfo) -> Self {
        CommitInfo {
            timestamp: value.timestamp,
            user_id: value.user_id,
            user_name: value.user_name,
            operation: value.operation,
            operation_parameters: value.operation_parameters,
            read_version: value.read_version,
            isolation_level: value.isolation_level.map(Into::into),
            is_blind_append: value.is_blind_append,
            engine_info: value.engine_info,
            info: value.info,
            user_metadata: value.user_metadata,
        }
    }
}

impl From<DomainMetadata> for DeltaRsDomainMetadata {
    fn from(value: DomainMetadata) -> Self {
        DeltaRsDomainMetadata {
            domain: value.domain,
            configuration: value.configuration,
            removed: value.removed,
        }
    }
}

impl From<DeltaRsDomainMetadata> for DomainMetadata {
    fn from(value: DeltaRsDomainMetadata) -> Self {
        DomainMetadata {
            domain: value.domain,
            configuration: value.configuration,
            removed: value.removed,
        }
    }
}

impl From<CheckpointMetadata> for DeltaRsCheckpointMetadata {
    fn from(value: CheckpointMetadata) -> Self {
        DeltaRsCheckpointMetadata {
            flavor: value.flavor,
            tags: value.tags,
        }
    }
}

impl From<DeltaRsCheckpointMetadata> for CheckpointMetadata {
    fn from(value: DeltaRsCheckpointMetadata) -> Self {
        CheckpointMetadata {
            flavor: value.flavor,
            tags: value.tags,
        }
    }
}

impl From<Sidecar> for DeltaRsSidecar {
    fn from(value: Sidecar) -> Self {
        DeltaRsSidecar {
            file_name: value.file_name,
            size_in_bytes: value.size_in_bytes,
            modification_time: value.modification_time,
            sidecar_type: value.sidecar_type,
            tags: value.tags,
        }
    }
}

impl From<DeltaRsSidecar> for Sidecar {
    fn from(value: DeltaRsSidecar) -> Self {
        Sidecar {
            file_name: value.file_name,
            size_in_bytes: value.size_in_bytes,
            modification_time: value.modification_time,
            sidecar_type: value.sidecar_type,
            tags: value.tags,
        }
    }
}

impl From<Action> for DeltaRsAction {
    fn from(value: Action) -> Self {
        match value {
            Action::Metadata(m) => DeltaRsAction::Metadata(m),
            Action::Protocol(p) => DeltaRsAction::Protocol(p),
            Action::Add(a) => DeltaRsAction::Add(a.into()),
            Action::Remove(r) => DeltaRsAction::Remove(r.into()),
            Action::Cdc(c) => DeltaRsAction::Cdc(c.into()),
            Action::Txn(t) => DeltaRsAction::Txn(t.into()),
            Action::CommitInfo(c) => DeltaRsAction::CommitInfo(c.into()),
            Action::DomainMetadata(d) => DeltaRsAction::DomainMetadata(d.into()),
            Action::CheckpointMetadata(_) => {
                panic!("CheckpointMetadata actions are not supported in delta-rs conversions")
            }
            Action::Sidecar(_) => {
                panic!("Sidecar actions are not supported in delta-rs conversions")
            }
        }
    }
}

impl From<&Action> for DeltaRsAction {
    fn from(value: &Action) -> Self {
        match value {
            Action::Metadata(m) => DeltaRsAction::Metadata(m.clone()),
            Action::Protocol(p) => DeltaRsAction::Protocol(p.clone()),
            Action::Add(a) => DeltaRsAction::Add(a.into()),
            Action::Remove(r) => DeltaRsAction::Remove(r.into()),
            Action::Cdc(c) => DeltaRsAction::Cdc(c.clone().into()),
            Action::Txn(t) => DeltaRsAction::Txn(t.clone().into()),
            Action::CommitInfo(c) => DeltaRsAction::CommitInfo(c.clone().into()),
            Action::DomainMetadata(d) => DeltaRsAction::DomainMetadata(d.clone().into()),
            Action::CheckpointMetadata(_) => {
                panic!("CheckpointMetadata actions are not supported in delta-rs conversions")
            }
            Action::Sidecar(_) => {
                panic!("Sidecar actions are not supported in delta-rs conversions")
            }
        }
    }
}

impl From<DeltaRsAction> for Action {
    fn from(value: DeltaRsAction) -> Self {
        match value {
            DeltaRsAction::Metadata(m) => Action::Metadata(m),
            DeltaRsAction::Protocol(p) => Action::Protocol(p),
            DeltaRsAction::Add(a) => Action::Add(a.into()),
            DeltaRsAction::Remove(r) => Action::Remove(r.into()),
            DeltaRsAction::Cdc(c) => Action::Cdc(c.into()),
            DeltaRsAction::Txn(t) => Action::Txn(t.into()),
            DeltaRsAction::CommitInfo(c) => Action::CommitInfo(c.into()),
            DeltaRsAction::DomainMetadata(d) => Action::DomainMetadata(d.into()),
        }
    }
}

impl From<&DeltaRsAction> for Action {
    fn from(value: &DeltaRsAction) -> Self {
        match value {
            DeltaRsAction::Metadata(m) => Action::Metadata(m.clone()),
            DeltaRsAction::Protocol(p) => Action::Protocol(p.clone()),
            DeltaRsAction::Add(a) => Action::Add(a.into()),
            DeltaRsAction::Remove(r) => Action::Remove(r.into()),
            DeltaRsAction::Cdc(c) => Action::Cdc(c.clone().into()),
            DeltaRsAction::Txn(t) => Action::Txn(t.clone().into()),
            DeltaRsAction::CommitInfo(c) => Action::CommitInfo(c.clone().into()),
            DeltaRsAction::DomainMetadata(d) => Action::DomainMetadata(d.clone().into()),
        }
    }
}
