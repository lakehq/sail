//! Built-in Jev functions. Only the kind, never credentials or clients, is serialized.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, MapArray, StructArray, new_empty_array};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use async_trait::async_trait;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, plan_err};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use parquet_variant_compute::{VariantArray, VariantType, unshred_variant};
use parquet_variant_json::VariantToJson;
use sail_common_datafusion::variant::{
    VARIANT_VALUE_FIELD_NAME, is_variant_storage_field, variant_metadata_field,
};
use serde::Serialize;
use serde_json::value::{RawValue, to_raw_value};
use serde_json::{Map, Value};

pub(crate) mod contract;
mod output;
pub(crate) mod transport;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JevKind {
    Noul,
    Choice,
    Score,
    SystemOne,
    Models,
}

impl JevKind {
    pub const ALL: [Self; 5] = [
        Self::Noul,
        Self::Choice,
        Self::Score,
        Self::SystemOne,
        Self::Models,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Noul => "jev_noul",
            Self::Choice => "jev_choice",
            Self::Score => "jev_score",
            Self::SystemOne => "jev_system_one",
            Self::Models => "jev_models",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }

    fn arity(self) -> (usize, usize) {
        match self {
            Self::Noul => (2, 4),
            Self::Choice | Self::Score => (3, 4),
            Self::SystemOne => (2, 3),
            Self::Models => (0, 1),
        }
    }

    pub fn options_index(self) -> usize {
        match self {
            Self::Noul | Self::Choice | Self::Score => 3,
            Self::SystemOne => 2,
            Self::Models => 0,
        }
    }

    pub fn udf(self) -> ScalarUDF {
        AsyncScalarUDF::new(Arc::new(Jev::new(self))).into_scalar_udf()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Jev {
    // Distinct volatile call sites must not compare equal in the async mapper.
    call_id: uuid::Uuid,
    kind: JevKind,
    signature: Signature,
}

impl Jev {
    fn new(kind: JevKind) -> Self {
        let (min, max) = kind.arity();
        Self {
            call_id: uuid::Uuid::new_v4(),
            kind,
            signature: Signature::one_of(
                (min..=max)
                    .map(|count| {
                        if count == 0 {
                            TypeSignature::Nullary
                        } else {
                            TypeSignature::Any(count)
                        }
                    })
                    .collect(),
                Volatility::Volatile,
            ),
        }
    }

    fn validate_fields(&self, fields: &[FieldRef]) -> Result<()> {
        let (min, max) = self.kind.arity();
        if !(min..=max).contains(&fields.len()) {
            return plan_err!("{} requires {min} to {max} arguments", self.name());
        }
        for (index, field) in fields.iter().enumerate() {
            let ty = field.data_type();
            let valid = if matches!(ty, DataType::Null) {
                true
            } else if index == self.kind.options_index() {
                string_map(ty)
            } else if self.kind == JevKind::SystemOne && index == 1 {
                is_variant_storage_field(field)
            } else if index == 2 {
                is_variant_storage_field(field)
                    || match self.kind {
                        JevKind::Score => {
                            matches!(ty, DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) | DataType::ListView(item) | DataType::LargeListView(item) if string_type(item.data_type()))
                        }
                        _ => string_map(ty),
                    }
            } else {
                string_type(ty) || is_variant_storage_field(field)
            };
            if !valid {
                return plan_err!(
                    "{} argument {} has unsupported type {ty}; use STRING/VARIANT for content, the documented criteria type, and MAP<STRING,STRING> for options",
                    self.name(),
                    index + 1
                );
            }
        }
        Ok(())
    }
}

fn string_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn string_map(ty: &DataType) -> bool {
    matches!(ty, DataType::Map(entry, _) if matches!(entry.data_type(), DataType::Struct(fields)
        if fields.len() == 2 && string_type(fields[0].data_type())
            && (string_type(fields[1].data_type()) || fields[1].data_type() == &DataType::Null)))
}

impl ScalarUDFImpl for Jev {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.validate_fields(
            &arg_types
                .iter()
                .map(|ty| Arc::new(Field::new("argument", ty.clone(), true)))
                .collect::<Vec<_>>(),
        )?;
        Ok(DataType::Struct(output::fields(self.kind)))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.validate_fields(args.arg_fields)?;
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Struct(output::fields(self.kind)),
            true,
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("Jev requires asynchronous evaluation")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for Jev {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.validate_fields(&args.arg_fields)?;
        let count = args.number_rows;
        // number_rows is authoritative even when every argument is scalar or there are none.
        if count == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(args.return_type())));
        }
        let inputs = args
            .args
            .iter()
            .zip(&args.arg_fields)
            .map(|(value, field)| Input::new(value, field, count))
            .collect::<Result<Vec<_>>>()?;
        let options_index = self.kind.options_index();
        let constant_options = inputs
            .get(options_index)
            .is_none_or(|input| matches!(input, Input::Scalar(_)));
        let mut parsed_options = None;
        let mut rows = transport::evaluate(self.kind, count, |index| {
            let state = if self.kind == JevKind::Models {
                InputValue::from_value(Value::Null)?
            } else {
                let Some(state) = inputs[0].get(index)? else {
                    return Ok(None);
                };
                state
            };
            let options = if constant_options {
                if parsed_options.is_none() {
                    let value = inputs
                        .get(options_index)
                        .map(|input| input.get(index))
                        .transpose()?
                        .flatten();
                    parsed_options = Some(contract::Options::parse(
                        value.as_ref().map(|value| value.parsed.as_ref()),
                        self.kind == JevKind::Models,
                    )?);
                }
                parsed_options
                    .clone()
                    .ok_or_else(|| exec_datafusion_err!("missing Jev options"))?
            } else {
                let value = inputs[options_index].get(index)?;
                contract::Options::parse(
                    value.as_ref().map(|value| value.parsed.as_ref()),
                    self.kind == JevKind::Models,
                )?
            };
            let questions = match self.kind {
                JevKind::Models => BTreeMap::new(),
                JevKind::SystemOne => inputs[1]
                    .get(index)?
                    .ok_or_else(|| {
                        exec_datafusion_err!("jev_system_one questions must be a VARIANT object")
                    })?
                    .object_fields()?,
                kind => {
                    let mut question = BTreeMap::new();
                    question.insert(
                        "type".to_owned(),
                        InputValue::from_value(kind.name()[4..].into())?,
                    );
                    if let Some(instructions) = inputs[1].get(index)? {
                        question.insert("instructions".to_owned(), instructions);
                    }
                    if let Some(criteria) = inputs
                        .get(2)
                        .map(|input| input.get(index))
                        .transpose()?
                        .flatten()
                        && (kind != JevKind::Noul || !criteria.parsed.is_null())
                    {
                        question.insert("criteria".to_owned(), criteria);
                    }
                    BTreeMap::from_iter([("result".to_owned(), InputValue::object(question)?)])
                }
            };
            Ok(Some(transport::RequestRow {
                state,
                questions,
                options,
            }))
        })
        .await?;
        if rows.len() != count {
            return exec_err!("Jev returned an incorrect number of rows");
        }
        if matches!(self.kind, JevKind::Noul | JevKind::Choice | JevKind::Score) {
            for row in rows.iter_mut().flatten() {
                let row = &mut row.value;
                let answer = row["answers"]["result"]
                    .as_object()
                    .cloned()
                    .ok_or_else(|| exec_datafusion_err!("missing Jev answer"))?;
                let object = row
                    .as_object_mut()
                    .ok_or_else(|| exec_datafusion_err!("invalid Jev response"))?;
                object.remove("answers");
                // Provider extensions belong inside generic answers, never over the
                // validated request metadata shared by the rows in this batch.
                for field in match self.kind {
                    JevKind::Noul => &["noul"][..],
                    JevKind::Choice => &["choice", "probabilities", "confidence"][..],
                    JevKind::Score => &["score", "probabilities", "confidence", "legend"][..],
                    _ => &[],
                } {
                    if let Some(value) = answer.get(*field) {
                        object.insert((*field).to_owned(), value.clone());
                    }
                }
            }
        }
        output::array(self.kind, &rows).map(ColumnarValue::Array)
    }
}

// Keep the original JSON for transport; the parsed view is only for shape validation.
// In particular, serde_json::Value cannot represent every VARIANT decimal exactly.
#[derive(Clone)]
pub(crate) struct InputValue {
    parsed: Arc<Value>,
    json: Arc<RawValue>,
}

impl InputValue {
    fn from_json(json: String) -> Result<Self> {
        let parsed = serde_json::from_str(&json)
            .map_err(|_| exec_datafusion_err!("Jev received invalid JSON"))?;
        let json = RawValue::from_string(json)
            .map_err(|_| exec_datafusion_err!("Jev received invalid JSON"))?;
        Ok(Self {
            parsed: Arc::new(parsed),
            json: Arc::from(json),
        })
    }

    fn from_value(value: Value) -> Result<Self> {
        let json =
            to_raw_value(&value).map_err(|_| exec_datafusion_err!("Could not encode Jev input"))?;
        Ok(Self {
            parsed: Arc::new(value),
            json: Arc::from(json),
        })
    }

    fn object(fields: BTreeMap<String, Self>) -> Result<Self> {
        let json = to_raw_value(&fields)
            .map_err(|_| exec_datafusion_err!("Could not encode Jev question"))?;
        let parsed = fields
            .into_iter()
            .map(|(key, value)| (key, Arc::unwrap_or_clone(value.parsed)))
            .collect();
        Ok(Self {
            parsed: Arc::new(Value::Object(parsed)),
            json: Arc::from(json),
        })
    }

    fn object_fields(&self) -> Result<BTreeMap<String, Self>> {
        let fields: BTreeMap<String, Box<RawValue>> = serde_json::from_str(self.json.get())
            .map_err(|_| {
                exec_datafusion_err!("jev_system_one questions must be a VARIANT object")
            })?;
        fields
            .into_iter()
            .map(|(key, value)| Ok((key, Self::from_json(value.get().to_owned())?)))
            .collect()
    }
}

impl Serialize for InputValue {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.json.serialize(serializer)
    }
}

enum Input {
    Scalar(Option<InputValue>),
    Array(ArrayRef, FieldRef),
}

impl Input {
    fn new(value: &ColumnarValue, field: &FieldRef, count: usize) -> Result<Self> {
        match value {
            ColumnarValue::Scalar(value) => Ok(Self::Scalar(json_input(
                value.to_array_of_size(1)?.as_ref(),
                field,
                0,
            )?)),
            ColumnarValue::Array(array) if array.len() == count => {
                Ok(Self::Array(Arc::clone(array), Arc::clone(field)))
            }
            ColumnarValue::Array(_) => exec_err!("Jev argument length differs from number_rows"),
        }
    }

    fn get(&self, index: usize) -> Result<Option<InputValue>> {
        match self {
            Self::Scalar(value) => Ok(value.clone()),
            Self::Array(array, field) => json_input(array.as_ref(), field, index),
        }
    }
}

fn json_input(array: &dyn Array, field: &Field, index: usize) -> Result<Option<InputValue>> {
    if array.data_type() == &DataType::Null || array.is_null(index) {
        return Ok(None);
    }
    if is_variant_storage_field(field) {
        let row = array.slice(index, 1);
        let variant = unshred_variant(&VariantArray::try_new(row.as_ref())?)?;
        return InputValue::from_json(variant.try_value(0)?.to_json_string()?).map(Some);
    }
    json_value(array, field, index)?
        .map(InputValue::from_value)
        .transpose()
}

fn json_value(array: &dyn Array, _field: &Field, index: usize) -> Result<Option<Value>> {
    if array.data_type() == &DataType::Null || array.is_null(index) {
        return Ok(None);
    }
    let value = match ScalarValue::try_from_array(array, index)? {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Value::String(value),
        ScalarValue::List(value) => {
            let DataType::List(field) = value.data_type() else {
                return exec_err!("invalid Jev array argument");
            };
            json_list(value.value(0), field)?
        }
        ScalarValue::LargeList(value) => {
            let DataType::LargeList(field) = value.data_type() else {
                return exec_err!("invalid Jev array argument");
            };
            json_list(value.value(0), field)?
        }
        ScalarValue::FixedSizeList(value) => {
            let DataType::FixedSizeList(field, _) = value.data_type() else {
                return exec_err!("invalid Jev array argument");
            };
            json_list(value.value(0), field)?
        }
        ScalarValue::ListView(value) => {
            let DataType::ListView(field) = value.data_type() else {
                return exec_err!("invalid Jev array argument");
            };
            json_list(value.value(0), field)?
        }
        ScalarValue::LargeListView(value) => {
            let DataType::LargeListView(field) = value.data_type() else {
                return exec_err!("invalid Jev array argument");
            };
            json_list(value.value(0), field)?
        }
        ScalarValue::Map(value) => {
            let entries = value.value(0);
            let mut object = Map::new();
            for i in 0..entries.len() {
                let key = json_value(entries.column(0).as_ref(), &entries.fields()[0], i)?
                    .and_then(|key| key.as_str().map(str::to_owned))
                    .ok_or_else(|| exec_datafusion_err!("Jev maps require non-null STRING keys"))?;
                let value = json_value(entries.column(1).as_ref(), &entries.fields()[1], i)?
                    .unwrap_or(Value::Null);
                if object.insert(key, value).is_some() {
                    return exec_err!("Jev maps must not contain duplicate keys");
                }
            }
            Value::Object(object)
        }
        _ => return exec_err!("unsupported Jev input type {}", array.data_type()),
    };
    Ok(Some(value))
}

fn json_list(array: ArrayRef, field: &Field) -> Result<Value> {
    Ok(Value::Array(
        (0..array.len())
            .map(|i| json_value(array.as_ref(), field, i).map(|value| value.unwrap_or(Value::Null)))
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn variant_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Struct(Fields::from(vec![
            Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, false),
            variant_metadata_field(DataType::Binary, false),
        ])),
        nullable,
    )
    .with_extension_type(VariantType)
}
