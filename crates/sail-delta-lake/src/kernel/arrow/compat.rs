/// Arrow version compatibility utilities.
///
/// delta_kernel 0.18.2 uses arrow 57 internally, while the workspace uses arrow 58.
/// These functions convert between the two versions using IPC serialization.
use datafusion::arrow::array::RecordBatch;

use crate::kernel::DeltaTableError;

pub(crate) type RecordBatch57 = arrow_57::array::RecordBatch;

pub(crate) fn arrow57_to_arrow58(batch: RecordBatch57) -> Result<RecordBatch, DeltaTableError> {
    let mut buf = vec![];
    {
        let mut writer =
            arrow_57::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
                .map_err(|e| DeltaTableError::generic(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| DeltaTableError::generic(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| DeltaTableError::generic(e.to_string()))?;
    }
    let cursor = std::io::Cursor::new(buf);
    let mut reader = datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| DeltaTableError::generic(e.to_string()))?;
    reader
        .next()
        .ok_or_else(|| DeltaTableError::generic("IPC conversion produced no batches"))?
        .map_err(|e| DeltaTableError::generic(e.to_string()))
}

pub(crate) fn arrow58_to_arrow57(batch: &RecordBatch) -> Result<RecordBatch57, DeltaTableError> {
    let mut buf = vec![];
    {
        let mut writer =
            datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref())
                .map_err(|e| DeltaTableError::generic(e.to_string()))?;
        writer
            .write(batch)
            .map_err(|e| DeltaTableError::generic(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| DeltaTableError::generic(e.to_string()))?;
    }
    let cursor = std::io::Cursor::new(buf);
    let mut reader = arrow_57::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| DeltaTableError::generic(e.to_string()))?;
    reader
        .next()
        .ok_or_else(|| DeltaTableError::generic("IPC conversion produced no batches"))?
        .map_err(|e| DeltaTableError::generic(e.to_string()))
}
