/// Arrow version compatibility utilities.
///
/// delta_kernel 0.18.2 uses arrow 57 internally, while the workspace uses arrow 58.
/// These functions convert between the two versions using IPC serialization.
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::schema::{DataType as KernelDataType, StructField, StructType};

use crate::kernel::DeltaTableError;

pub(crate) type RecordBatch57 = arrow_57::array::RecordBatch;
type Schema57 = arrow_57::datatypes::Schema;

fn err(e: impl ToString) -> DeltaTableError {
    DeltaTableError::generic(e.to_string())
}

pub(crate) fn arrow57_to_arrow58(batch: RecordBatch57) -> Result<RecordBatch, DeltaTableError> {
    let mut buf = vec![];
    {
        let mut writer =
            arrow_57::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).map_err(err)?;
        writer.write(&batch).map_err(err)?;
        writer.finish().map_err(err)?;
    }
    let cursor = std::io::Cursor::new(buf);
    let mut reader =
        datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None).map_err(err)?;
    reader
        .next()
        .ok_or_else(|| DeltaTableError::generic("IPC conversion produced no batches"))?
        .map_err(err)
}

pub(crate) fn arrow58_to_arrow57(batch: &RecordBatch) -> Result<RecordBatch57, DeltaTableError> {
    let mut buf = vec![];
    {
        let mut writer =
            datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref())
                .map_err(err)?;
        writer.write(batch).map_err(err)?;
        writer.finish().map_err(err)?;
    }
    let cursor = std::io::Cursor::new(buf);
    let mut reader = arrow_57::ipc::reader::StreamReader::try_new(cursor, None).map_err(err)?;
    reader
        .next()
        .ok_or_else(|| DeltaTableError::generic("IPC conversion produced no batches"))?
        .map_err(err)
}

/// Convert an arrow 57 Schema to an arrow 58 Schema via IPC.
pub(crate) fn arrow57_schema_to_arrow58(schema57: &Schema57) -> Result<Schema, DeltaTableError> {
    let batch57 = arrow_57::array::RecordBatch::new_empty(Arc::new(schema57.clone()));
    let batch58 = arrow57_to_arrow58(batch57)?;
    Ok((*batch58.schema()).clone())
}

/// Convert an arrow 58 Schema to an arrow 57 Schema via IPC.
pub(crate) fn arrow58_schema_to_arrow57(
    schema58: &Schema,
) -> Result<Arc<Schema57>, DeltaTableError> {
    let batch58 = RecordBatch::new_empty(Arc::new(schema58.clone()));
    let batch57 = arrow58_to_arrow57(&batch58)?;
    Ok(batch57.schema())
}

/// Convert a delta_kernel StructType to an arrow 58 Schema.
pub(crate) fn kernel_struct_to_arrow58_schema(st: &StructType) -> Result<Schema, DeltaTableError> {
    let schema57: Schema57 = st.try_into_arrow().map_err(err)?;
    arrow57_schema_to_arrow58(&schema57)
}

/// Convert an arrow 58 Schema to a delta_kernel StructType.
pub(crate) fn arrow58_schema_to_kernel_struct(
    schema: &Schema,
) -> Result<StructType, DeltaTableError> {
    let schema57 = arrow58_schema_to_arrow57(schema)?;
    schema57.as_ref().try_into_kernel().map_err(err)
}

/// Convert a delta_kernel StructField to an arrow 58 Field.
pub(crate) fn kernel_field_to_arrow58_field(
    field: &StructField,
) -> Result<Field, DeltaTableError> {
    let field57: arrow_57::datatypes::Field = field.try_into_arrow().map_err(err)?;
    let schema57 = Schema57::new(vec![field57]);
    let schema58 = arrow57_schema_to_arrow58(&schema57)?;
    Ok(schema58.fields()[0].as_ref().clone())
}

/// Convert an arrow 58 Field to a delta_kernel StructField.
pub(crate) fn arrow58_field_to_kernel_field(field: &Field) -> Result<StructField, DeltaTableError> {
    let schema58 = Schema::new(vec![field.clone()]);
    let schema57 = arrow58_schema_to_arrow57(&schema58)?;
    schema57.fields()[0].as_ref().try_into_kernel().map_err(err)
}

/// Convert an arrow 58 DataType to a delta_kernel DataType.
pub(crate) fn arrow58_datatype_to_kernel_datatype(
    dt: &datafusion::arrow::datatypes::DataType,
) -> Result<KernelDataType, DeltaTableError> {
    let field = datafusion::arrow::datatypes::Field::new("__tmp__", dt.clone(), true);
    let kernel_field = arrow58_field_to_kernel_field(&field)?;
    Ok(kernel_field.data_type().clone())
}
