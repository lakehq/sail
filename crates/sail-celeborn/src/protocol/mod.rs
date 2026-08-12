//! Common Celeborn wire protocol types and transport-message decoding.

#[expect(clippy::enum_variant_names)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/celeborn.rs"));
}

pub(crate) mod transport;

/// Celeborn operation status codes, mirroring the `StatusCode` enum in Java.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StatusCode {
    // 1/0 Status
    Success = 0,
    PartialSuccess = 1,
    RequestFailed = 2,

    // Specific Status
    ShuffleAlreadyRegistered = 3,
    ShuffleUnregistered = 4,
    ReserveSlotsFailed = 5,
    SlotNotAvailable = 6,
    WorkerNotFound = 7,
    PartitionNotFound = 8,
    ReplicaPartitionNotFound = 9,
    DeleteFilesFailed = 10,
    PartitionExists = 11,
    ReviveFailed = 12,
    ReplicateDataFailed = 13,
    NumMapperZero = 14,
    MapEnded = 15,
    StageEnded = 16,

    // push data fail causes
    PushDataFailNonCriticalCausePrimary = 17,
    PushDataWriteFailReplica = 18,
    PushDataWriteFailPrimary = 19,
    PushDataFailPartitionNotFound = 20,

    HardSplit = 21,
    SoftSplit = 22,

    StageEndTimeout = 23,
    ShuffleDataLost = 24,
    WorkerShutdown = 25,
    NoAvailableWorkingDir = 26,
    WorkerExcluded = 27,
    WorkerUnknown = 28,

    CommitFileException = 29,

    // Rate limit statuses
    PushDataSuccessPrimaryCongested = 30,
    PushDataSuccessReplicaCongested = 31,

    PushDataHandshakeFailReplica = 32,
    PushDataHandshakeFailPrimary = 33,
    RegionStartFailReplica = 34,
    RegionStartFailPrimary = 35,
    RegionFinishFailReplica = 36,
    RegionFinishFailPrimary = 37,

    PushDataCreateConnectionFailPrimary = 38,
    PushDataCreateConnectionFailReplica = 39,
    PushDataConnectionExceptionPrimary = 40,
    PushDataConnectionExceptionReplica = 41,
    PushDataTimeoutPrimary = 42,
    PushDataTimeoutReplica = 43,
    PushDataPrimaryWorkerExcluded = 44,
    PushDataReplicaWorkerExcluded = 45,

    FetchDataTimeout = 46,
    ReviveInitialized = 47,
    DestroySlotsMockFailure = 48,
    CommitFilesMockFailure = 49,
    PushDataFailNonCriticalCauseReplica = 50,
    OpenStreamFailed = 51,
    SegmentStartFailReplica = 52,
    SegmentStartFailPrimary = 53,
    NoSplit = 54,
    WorkerUnresponsive = 55,
    ReadReducerPartitionEndFailed = 56,
}
