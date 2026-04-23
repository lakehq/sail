//! Higher-order array/map function UDFs for Sail.

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{
    as_boolean_array, as_large_list_array, as_list_array, Array, ArrayRef, BooleanArray,
    GenericListArray, Int64Array, MapArray, OffsetSizeTrait, RecordBatch, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_physical_expr::PhysicalExpr;

static HOF_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_hof_id() -> u64 {
    HOF_COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn eval_predicate(
    lambda_expr: &Arc<dyn PhysicalExpr>,
    param_names: &[String],
    param_arrays: Vec<ArrayRef>,
) -> Result<BooleanArray> {
    let fields: Vec<Field> = param_names
        .iter()
        .zip(param_arrays.iter())
        .map(|(name, arr)| Field::new(name.as_str(), arr.data_type().clone(), true))
        .collect();
    let len = param_arrays.first().map(|a| a.len()).unwrap_or(0);
    let schema = Schema::new(fields);
    let batch = RecordBatch::try_new(Arc::new(schema), param_arrays)?;
    let result = lambda_expr.evaluate(&batch)?;
    match result {
        ColumnarValue::Array(arr) => Ok(as_boolean_array(&arr).clone()),
        ColumnarValue::Scalar(sv) => {
            let arr = sv.to_array_of_size(len)?;
            Ok(as_boolean_array(&arr).clone())
        }
    }
}

fn eval_transform(
    lambda_expr: &Arc<dyn PhysicalExpr>,
    param_names: &[String],
    param_arrays: Vec<ArrayRef>,
) -> Result<ArrayRef> {
    let fields: Vec<Field> = param_names
        .iter()
        .zip(param_arrays.iter())
        .map(|(name, arr)| Field::new(name.as_str(), arr.data_type().clone(), true))
        .collect();
    let len = param_arrays.first().map(|a| a.len()).unwrap_or(0);
    let schema = Schema::new(fields);
    let batch = RecordBatch::try_new(Arc::new(schema), param_arrays)?;
    let result = lambda_expr.evaluate(&batch)?;
    match result {
        ColumnarValue::Array(arr) => Ok(arr),
        ColumnarValue::Scalar(sv) => sv.to_array_of_size(len),
    }
}

// ============================================================
// SailArrayFilter
// ============================================================
#[derive(Debug)]
pub struct SailArrayFilter {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    param_names: Vec<String>,
    return_type: DataType,
    signature: Signature,
}

impl SailArrayFilter {
    pub fn new(
        lambda_expr: Arc<dyn PhysicalExpr>,
        param_names: Vec<String>,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            param_names,
            return_type,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailArrayFilter {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailArrayFilter {}
impl std::hash::Hash for SailArrayFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailArrayFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_array_filter"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("sail_array_filter: no args".into())
            })?
            .into_array(1)?;
        match array.data_type() {
            DataType::List(_) => {
                let list = as_list_array(&array);
                Ok(ColumnarValue::Array(array_filter_generic::<i32>(
                    list,
                    &self.lambda_expr,
                    &self.param_names,
                )?))
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&array);
                Ok(ColumnarValue::Array(array_filter_generic::<i64>(
                    list,
                    &self.lambda_expr,
                    &self.param_names,
                )?))
            }
            other => exec_err!("sail_array_filter: unsupported type {:?}", other),
        }
    }
}

fn array_filter_generic<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    lambda_expr: &Arc<dyn PhysicalExpr>,
    param_names: &[String],
) -> Result<ArrayRef> {
    let values = list.values().clone();
    let offsets = list.offsets();
    // If there is a second param (index), build an index array over all elements
    let (mask, n_total) = if param_names.len() == 2 {
        let n = values.len();
        // Build per-row indices: within each list row, indices go 0..len(row)
        let mut idx_values: Vec<Option<i64>> = Vec::with_capacity(n);
        for i in 0..list.len() {
            let start = offsets[i].as_usize();
            let end = offsets[i + 1].as_usize();
            if list.is_null(i) {
                for _ in start..end {
                    idx_values.push(None);
                }
            } else {
                for j in 0..(end - start) {
                    idx_values.push(Some(j as i64));
                }
            }
        }
        let idx: ArrayRef = Arc::new(Int64Array::from(idx_values));
        let m = eval_predicate(lambda_expr, param_names, vec![values.clone(), idx])?;
        (m, n)
    } else {
        let m = eval_predicate(lambda_expr, param_names, vec![values.clone()])?;
        (m, values.len())
    };

    let mut new_offsets: Vec<O> = Vec::with_capacity(list.len() + 1);
    new_offsets.push(O::usize_as(0));
    let mut total_kept: usize = 0;
    let mut keep: Vec<bool> = Vec::with_capacity(n_total);

    for i in 0..list.len() {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        if list.is_null(i) {
            keep.extend(std::iter::repeat_n(false, end - start));
            new_offsets.push(O::usize_as(total_kept));
        } else {
            for j in start..end {
                let v = mask.value(j);
                keep.push(v);
                if v {
                    total_kept += 1;
                }
            }
            new_offsets.push(O::usize_as(total_kept));
        }
    }

    let keep_arr = BooleanArray::from(keep);
    let new_values = compute::filter(values.as_ref(), &keep_arr)?;
    let field = match list.data_type() {
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
        _ => unreachable!(),
    };
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(new_offsets.into()),
        new_values,
        list.nulls().cloned(),
    )?))
}

// ============================================================
// SailArrayTransform
// ============================================================
#[derive(Debug)]
pub struct SailArrayTransform {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    param_names: Vec<String>,
    return_type: DataType,
    signature: Signature,
}

impl SailArrayTransform {
    pub fn new(
        lambda_expr: Arc<dyn PhysicalExpr>,
        param_names: Vec<String>,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            param_names,
            return_type,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailArrayTransform {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailArrayTransform {}
impl std::hash::Hash for SailArrayTransform {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailArrayTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_array_transform"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "sail_array_transform: no args".into(),
                )
            })?
            .into_array(1)?;
        let result_field = match &self.return_type {
            DataType::List(f) | DataType::LargeList(f) => f.clone(),
            _ => {
                return exec_err!(
                    "sail_array_transform: unexpected return type {:?}",
                    self.return_type
                )
            }
        };
        match array.data_type() {
            DataType::List(_) => {
                let list = as_list_array(&array);
                Ok(ColumnarValue::Array(array_transform_generic::<i32>(
                    list,
                    &self.lambda_expr,
                    &self.param_names,
                    result_field,
                )?))
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&array);
                Ok(ColumnarValue::Array(array_transform_generic::<i64>(
                    list,
                    &self.lambda_expr,
                    &self.param_names,
                    result_field,
                )?))
            }
            other => exec_err!("sail_array_transform: unsupported type {:?}", other),
        }
    }
}

fn array_transform_generic<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    lambda_expr: &Arc<dyn PhysicalExpr>,
    param_names: &[String],
    result_field: FieldRef,
) -> Result<ArrayRef> {
    let values = list.values().clone();
    let offsets = list.offsets();

    let mut param_arrays: Vec<ArrayRef> = vec![values];
    if param_names.len() >= 2 {
        let total = list.values().len();
        let mut indices: Vec<i64> = Vec::with_capacity(total);
        for i in 0..list.len() {
            let start = offsets[i].as_usize();
            let end = offsets[i + 1].as_usize();
            for j in start..end {
                indices.push((j - start) as i64);
            }
        }
        param_arrays.push(Arc::new(Int64Array::from(indices)));
    }

    let new_values = eval_transform(lambda_expr, param_names, param_arrays)?;
    let new_offsets: Vec<O> = offsets.iter().copied().collect();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        result_field,
        OffsetBuffer::new(new_offsets.into()),
        new_values,
        list.nulls().cloned(),
    )?))
}

// ============================================================
// SailArrayExists
// ============================================================
#[derive(Debug)]
pub struct SailArrayExists {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    param_name: String,
    signature: Signature,
}

impl SailArrayExists {
    pub fn new(lambda_expr: Arc<dyn PhysicalExpr>, param_name: String) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            param_name,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailArrayExists {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailArrayExists {}
impl std::hash::Hash for SailArrayExists {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailArrayExists {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_array_exists"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("sail_array_exists: no args".into())
            })?
            .into_array(1)?;
        match array.data_type() {
            DataType::List(_) => {
                let list = as_list_array(&array);
                Ok(ColumnarValue::Array(array_exists_generic::<i32>(
                    list,
                    &self.lambda_expr,
                    &self.param_name,
                )?))
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&array);
                Ok(ColumnarValue::Array(array_exists_generic::<i64>(
                    list,
                    &self.lambda_expr,
                    &self.param_name,
                )?))
            }
            other => exec_err!("sail_array_exists: unsupported type {:?}", other),
        }
    }
}

fn array_exists_generic<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    lambda_expr: &Arc<dyn PhysicalExpr>,
    param_name: &str,
) -> Result<ArrayRef> {
    let values = list.values().clone();
    let offsets = list.offsets();
    let mask = eval_predicate(lambda_expr, &[param_name.to_string()], vec![values])?;
    let mut result: Vec<Option<bool>> = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        if list.is_null(i) {
            result.push(None);
        } else {
            let start = offsets[i].as_usize();
            let end = offsets[i + 1].as_usize();
            let any_true = (start..end).any(|j| mask.value(j));
            result.push(Some(any_true));
        }
    }
    Ok(Arc::new(BooleanArray::from(result)))
}

// ============================================================
// SailArrayForAll
// ============================================================
#[derive(Debug)]
pub struct SailArrayForAll {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    param_name: String,
    signature: Signature,
}

impl SailArrayForAll {
    pub fn new(lambda_expr: Arc<dyn PhysicalExpr>, param_name: String) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            param_name,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailArrayForAll {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailArrayForAll {}
impl std::hash::Hash for SailArrayForAll {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailArrayForAll {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_array_forall"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("sail_array_forall: no args".into())
            })?
            .into_array(1)?;
        match array.data_type() {
            DataType::List(_) => {
                let list = as_list_array(&array);
                Ok(ColumnarValue::Array(array_forall_generic::<i32>(
                    list,
                    &self.lambda_expr,
                    &self.param_name,
                )?))
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&array);
                Ok(ColumnarValue::Array(array_forall_generic::<i64>(
                    list,
                    &self.lambda_expr,
                    &self.param_name,
                )?))
            }
            other => exec_err!("sail_array_forall: unsupported type {:?}", other),
        }
    }
}

fn array_forall_generic<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    lambda_expr: &Arc<dyn PhysicalExpr>,
    param_name: &str,
) -> Result<ArrayRef> {
    let values = list.values().clone();
    let offsets = list.offsets();
    let mask = eval_predicate(lambda_expr, &[param_name.to_string()], vec![values])?;
    let mut result: Vec<Option<bool>> = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        if list.is_null(i) {
            result.push(None);
        } else {
            let start = offsets[i].as_usize();
            let end = offsets[i + 1].as_usize();
            let all_true = (start..end).all(|j| mask.value(j));
            result.push(Some(all_true));
        }
    }
    Ok(Arc::new(BooleanArray::from(result)))
}

// ============================================================
// SailArrayAggregate
// ============================================================
#[derive(Debug)]
pub struct SailArrayAggregate {
    id: u64,
    merge_expr: Arc<dyn PhysicalExpr>,
    acc_param: String,
    elem_param: String,
    finish_expr: Option<Arc<dyn PhysicalExpr>>,
    finish_param: String,
    result_type: DataType,
    signature: Signature,
}

impl SailArrayAggregate {
    pub fn new(
        merge_expr: Arc<dyn PhysicalExpr>,
        acc_param: String,
        elem_param: String,
        finish_expr: Option<Arc<dyn PhysicalExpr>>,
        finish_param: String,
        result_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            merge_expr,
            acc_param,
            elem_param,
            finish_expr,
            finish_param,
            result_type,
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailArrayAggregate {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailArrayAggregate {}
impl std::hash::Hash for SailArrayAggregate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailArrayAggregate {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_array_aggregate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.result_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut iter = args.args.into_iter();
        let list_cv = iter.next().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("sail_array_aggregate: no args".into())
        })?;
        let init_cv = iter.next().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "sail_array_aggregate: missing init arg".into(),
            )
        })?;

        let list_arr = list_cv.into_array(1)?;
        let nrows = list_arr.len();
        let init_arr = init_cv.into_array(nrows)?;

        let result = match list_arr.data_type() {
            DataType::List(_) => {
                let list = as_list_array(&list_arr);
                array_aggregate_generic::<i32>(
                    list,
                    init_arr.as_ref(),
                    &self.merge_expr,
                    &self.acc_param,
                    &self.elem_param,
                    self.finish_expr.as_ref(),
                    &self.finish_param,
                    &self.result_type,
                )?
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&list_arr);
                array_aggregate_generic::<i64>(
                    list,
                    init_arr.as_ref(),
                    &self.merge_expr,
                    &self.acc_param,
                    &self.elem_param,
                    self.finish_expr.as_ref(),
                    &self.finish_param,
                    &self.result_type,
                )?
            }
            other => return exec_err!("sail_array_aggregate: unsupported type {:?}", other),
        };
        Ok(ColumnarValue::Array(result))
    }
}

fn array_aggregate_generic<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    init_array: &dyn Array,
    merge_expr: &Arc<dyn PhysicalExpr>,
    acc_param: &str,
    elem_param: &str,
    finish_expr: Option<&Arc<dyn PhysicalExpr>>,
    finish_param: &str,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let values = list.values();
    let offsets = list.offsets();
    let mut acc_values: Vec<ScalarValue> = Vec::with_capacity(list.len());

    for i in 0..list.len() {
        if list.is_null(i) {
            acc_values.push(ScalarValue::try_from(result_type)?);
            continue;
        }
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        let mut acc = ScalarValue::try_from_array(init_array, i)?;

        for j in start..end {
            let elem = ScalarValue::try_from_array(values.as_ref(), j)?;
            let acc_arr = acc.to_array_of_size(1)?;
            let elem_arr = elem.to_array_of_size(1)?;
            let schema = Schema::new(vec![
                Field::new(acc_param, acc_arr.data_type().clone(), true),
                Field::new(elem_param, elem_arr.data_type().clone(), true),
            ]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![acc_arr, elem_arr])?;
            let result = merge_expr.evaluate(&batch)?;
            acc = match result {
                ColumnarValue::Scalar(sv) => sv,
                ColumnarValue::Array(arr) => ScalarValue::try_from_array(&arr, 0)?,
            };
        }

        if let Some(finish) = finish_expr {
            let acc_arr = acc.to_array_of_size(1)?;
            let schema = Schema::new(vec![Field::new(
                finish_param,
                acc_arr.data_type().clone(),
                true,
            )]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![acc_arr])?;
            let result = finish.evaluate(&batch)?;
            acc = match result {
                ColumnarValue::Scalar(sv) => sv,
                ColumnarValue::Array(arr) => ScalarValue::try_from_array(&arr, 0)?,
            };
        }
        acc_values.push(acc);
    }

    ScalarValue::iter_to_array(acc_values)
}

// ============================================================
// SailArrayZipWith
// ============================================================
#[derive(Debug)]
pub struct SailArrayZipWith {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    param_names: Vec<String>,
    return_type: DataType,
    signature: Signature,
}

impl SailArrayZipWith {
    pub fn new(
        lambda_expr: Arc<dyn PhysicalExpr>,
        param_names: Vec<String>,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            param_names,
            return_type,
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailArrayZipWith {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailArrayZipWith {}
impl std::hash::Hash for SailArrayZipWith {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailArrayZipWith {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_array_zip_with"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut iter = args.args.into_iter();
        let arr1 = iter
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("sail_array_zip_with: no args".into())
            })?
            .into_array(1)?;
        let arr2 = iter
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "sail_array_zip_with: missing second arg".into(),
                )
            })?
            .into_array(arr1.len())?;

        let result_field = match &self.return_type {
            DataType::List(f) | DataType::LargeList(f) => f.clone(),
            _ => {
                return exec_err!(
                    "sail_array_zip_with: unexpected return type {:?}",
                    self.return_type
                )
            }
        };

        match (arr1.data_type(), arr2.data_type()) {
            (DataType::List(_), DataType::List(_)) => {
                let l1 = as_list_array(&arr1);
                let l2 = as_list_array(&arr2);
                Ok(ColumnarValue::Array(array_zip_with_generic::<i32>(
                    l1,
                    l2,
                    &self.lambda_expr,
                    &self.param_names,
                    result_field,
                )?))
            }
            (DataType::LargeList(_), DataType::LargeList(_)) => {
                let l1 = as_large_list_array(&arr1);
                let l2 = as_large_list_array(&arr2);
                Ok(ColumnarValue::Array(array_zip_with_generic::<i64>(
                    l1,
                    l2,
                    &self.lambda_expr,
                    &self.param_names,
                    result_field,
                )?))
            }
            (t1, t2) => exec_err!(
                "sail_array_zip_with: mismatched or unsupported types {:?} {:?}",
                t1,
                t2
            ),
        }
    }
}

fn array_zip_with_generic<O: OffsetSizeTrait>(
    list1: &GenericListArray<O>,
    list2: &GenericListArray<O>,
    lambda_expr: &Arc<dyn PhysicalExpr>,
    param_names: &[String],
    result_field: FieldRef,
) -> Result<ArrayRef> {
    if list1.len() != list2.len() {
        return exec_err!("sail_array_zip_with: arrays must have same row count");
    }
    let values1 = list1.values().clone();
    let values2 = list2.values().clone();
    let offsets1 = list1.offsets();
    let offsets2 = list2.offsets();

    let mut all_results: Vec<ArrayRef> = Vec::new();
    let mut new_offsets: Vec<O> = Vec::with_capacity(list1.len() + 1);
    new_offsets.push(O::usize_as(0));
    let mut total: usize = 0;

    for i in 0..list1.len() {
        let start1 = offsets1[i].as_usize();
        let end1 = offsets1[i + 1].as_usize();
        let start2 = offsets2[i].as_usize();
        let end2 = offsets2[i + 1].as_usize();
        let len1 = end1 - start1;
        let len2 = end2 - start2;
        let _len = len1.max(len2);

        let min_len = len1.min(len2);

        if min_len == 0 {
            new_offsets.push(O::usize_as(total));
            continue;
        }

        let v1 = values1.slice(start1, min_len);
        let v2 = values2.slice(start2, min_len);

        let batch_len = v1.len();
        let fields: Vec<Field> = param_names
            .iter()
            .zip([v1.as_ref(), v2.as_ref()].iter())
            .map(|(name, arr)| Field::new(name.as_str(), arr.data_type().clone(), true))
            .collect();
        let schema = Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![v1, v2])?;
        let result = lambda_expr.evaluate(&batch)?;
        let result_arr = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(sv) => sv.to_array_of_size(batch_len)?,
        };
        total += result_arr.len();
        all_results.push(result_arr);
        new_offsets.push(O::usize_as(total));
    }

    let combined = if all_results.is_empty() {
        datafusion::arrow::array::new_empty_array(result_field.data_type())
    } else {
        compute::concat(
            all_results
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>()
                .as_slice(),
        )?
    };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        result_field,
        OffsetBuffer::new(new_offsets.into()),
        combined,
        list1.nulls().cloned(),
    )?))
}

// ============================================================
// SailArraySort
// ============================================================
#[derive(Debug)]
pub struct SailArraySort {
    id: u64,
    lambda_expr: Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    param_names: Vec<String>,
    return_type: DataType,
    signature: Signature,
}

impl SailArraySort {
    pub fn new(
        lambda_expr: Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        param_names: Vec<String>,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            param_names,
            return_type,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailArraySort {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailArraySort {}
impl std::hash::Hash for SailArraySort {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailArraySort {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_array_sort"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("sail_array_sort: no args".into())
            })?
            .into_array(1)?;
        match array.data_type() {
            DataType::List(_) => {
                let list = as_list_array(&array);
                Ok(ColumnarValue::Array(array_sort_with_comparator::<i32>(
                    list,
                    &self.lambda_expr,
                    &self.param_names,
                )?))
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&array);
                Ok(ColumnarValue::Array(array_sort_with_comparator::<i64>(
                    list,
                    &self.lambda_expr,
                    &self.param_names,
                )?))
            }
            other => exec_err!("sail_array_sort: unsupported type {:?}", other),
        }
    }
}

fn array_sort_with_comparator<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    lambda_expr: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    param_names: &[String],
) -> Result<ArrayRef> {
    let values = list.values();
    let offsets = list.offsets();
    let field = match list.data_type() {
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
        _ => unreachable!(),
    };

    let mut new_values_parts: Vec<ArrayRef> = Vec::with_capacity(list.len());
    let mut new_offsets: Vec<O> = Vec::with_capacity(list.len() + 1);
    new_offsets.push(O::usize_as(0));
    let mut total = 0usize;

    for i in 0..list.len() {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        let len = end - start;
        total += len;
        new_offsets.push(O::usize_as(total));

        if list.is_null(i) || len <= 1 {
            new_values_parts.push(values.slice(start, len));
            continue;
        }

        // Sort indices for this row using the comparator lambda
        let mut indices: Vec<usize> = (0..len).collect();
        let row_values = values.slice(start, len);
        let mut error: Option<datafusion_common::DataFusionError> = None;

        indices.sort_by(|&a, &b| {
            if error.is_some() {
                return std::cmp::Ordering::Equal;
            }
            let va = row_values.slice(a, 1);
            let vb = row_values.slice(b, 1);
            // Evaluate lambda(va, vb) -> Int
            let fields: Vec<Field> = param_names
                .iter()
                .zip([va.as_ref(), vb.as_ref()].iter())
                .map(|(name, arr)| Field::new(name.as_str(), arr.data_type().clone(), true))
                .collect();
            let schema = Schema::new(fields);
            let batch = match RecordBatch::try_new(Arc::new(schema), vec![va.clone(), vb.clone()]) {
                Ok(b) => b,
                Err(e) => {
                    error = Some(e.into());
                    return std::cmp::Ordering::Equal;
                }
            };
            let result = match lambda_expr.evaluate(&batch) {
                Ok(r) => r,
                Err(e) => {
                    error = Some(e);
                    return std::cmp::Ordering::Equal;
                }
            };
            let arr = match result {
                ColumnarValue::Array(a) => a,
                ColumnarValue::Scalar(sv) => match sv.to_array_of_size(1) {
                    Ok(a) => a,
                    Err(e) => {
                        error = Some(e);
                        return std::cmp::Ordering::Equal;
                    }
                },
            };
            let val = arr
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .map(|a| a.value(0) as i64)
                .or_else(|| {
                    arr.as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| a.value(0))
                })
                .unwrap_or(0i64);
            val.cmp(&0)
        });

        if let Some(e) = error {
            return Err(e);
        }

        // Apply sorted order
        let index_arr = datafusion::arrow::array::UInt64Array::from(
            indices.iter().map(|&i| i as u64).collect::<Vec<_>>(),
        );
        let sorted = datafusion::arrow::compute::take(row_values.as_ref(), &index_arr, None)?;
        new_values_parts.push(sorted);
    }

    let new_values = if new_values_parts.is_empty() {
        datafusion::arrow::array::new_empty_array(values.data_type())
    } else {
        let refs: Vec<&dyn Array> = new_values_parts.iter().map(|a| a.as_ref()).collect();
        compute::concat(&refs)?
    };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(new_offsets.into()),
        new_values,
        list.nulls().cloned(),
    )?))
}

// ============================================================
// Map helpers
// ============================================================
fn map_keys_values(map: &MapArray) -> (ArrayRef, ArrayRef) {
    let entries = map.entries();
    let keys = entries.column(0).clone();
    let values = entries.column(1).clone();
    (keys, values)
}

fn rebuild_map(
    _original: &MapArray,
    new_keys: ArrayRef,
    new_values: ArrayRef,
    return_type: &DataType,
    new_offsets: Vec<i32>,
    nulls: Option<datafusion::arrow::buffer::NullBuffer>,
    sorted: bool,
) -> Result<ColumnarValue> {
    let entries_field = match return_type {
        DataType::Map(f, _) => f.clone(),
        _ => {
            return exec_err!(
                "rebuild_map: expected Map return type, got {:?}",
                return_type
            )
        }
    };
    let struct_fields = match entries_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => {
            return exec_err!(
                "rebuild_map: expected Struct in Map entries, got {:?}",
                entries_field.data_type()
            )
        }
    };

    let new_entries = StructArray::try_new(struct_fields, vec![new_keys, new_values], None)?;
    let new_map = MapArray::try_new(
        entries_field,
        OffsetBuffer::new(new_offsets.into()),
        new_entries,
        nulls,
        sorted,
    )?;
    Ok(ColumnarValue::Array(Arc::new(new_map)))
}

// ============================================================
// SailMapTransformKeys
// ============================================================
#[derive(Debug)]
pub struct SailMapTransformKeys {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    key_param: String,
    val_param: String,
    return_type: DataType,
    signature: Signature,
}

impl SailMapTransformKeys {
    pub fn new(
        lambda_expr: Arc<dyn PhysicalExpr>,
        key_param: String,
        val_param: String,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            key_param,
            val_param,
            return_type,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailMapTransformKeys {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailMapTransformKeys {}
impl std::hash::Hash for SailMapTransformKeys {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailMapTransformKeys {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_map_transform_keys"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "sail_map_transform_keys: no args".into(),
                )
            })?
            .into_array(1)?;
        let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "sail_map_transform_keys: expected MapArray".into(),
            )
        })?;

        let (keys, values) = map_keys_values(map);
        let new_keys = eval_transform(
            &self.lambda_expr,
            &[self.key_param.clone(), self.val_param.clone()],
            vec![keys, values.clone()],
        )?;
        let offsets: Vec<i32> = map.offsets().iter().copied().collect();
        rebuild_map(
            map,
            new_keys,
            values,
            &self.return_type,
            offsets,
            map.nulls().cloned(),
            false,
        )
    }
}

// ============================================================
// SailMapTransformValues
// ============================================================
#[derive(Debug)]
pub struct SailMapTransformValues {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    key_param: String,
    val_param: String,
    return_type: DataType,
    signature: Signature,
}

impl SailMapTransformValues {
    pub fn new(
        lambda_expr: Arc<dyn PhysicalExpr>,
        key_param: String,
        val_param: String,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            key_param,
            val_param,
            return_type,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailMapTransformValues {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailMapTransformValues {}
impl std::hash::Hash for SailMapTransformValues {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailMapTransformValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_map_transform_values"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "sail_map_transform_values: no args".into(),
                )
            })?
            .into_array(1)?;
        let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "sail_map_transform_values: expected MapArray".into(),
            )
        })?;

        let (keys, values) = map_keys_values(map);
        let new_values = eval_transform(
            &self.lambda_expr,
            &[self.key_param.clone(), self.val_param.clone()],
            vec![keys.clone(), values],
        )?;
        let offsets: Vec<i32> = map.offsets().iter().copied().collect();
        rebuild_map(
            map,
            keys,
            new_values,
            &self.return_type,
            offsets,
            map.nulls().cloned(),
            false,
        )
    }
}

// ============================================================
// SailMapFilter
// ============================================================
#[derive(Debug)]
pub struct SailMapFilter {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    key_param: String,
    val_param: String,
    return_type: DataType,
    signature: Signature,
}

impl SailMapFilter {
    pub fn new(
        lambda_expr: Arc<dyn PhysicalExpr>,
        key_param: String,
        val_param: String,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            key_param,
            val_param,
            return_type,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailMapFilter {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailMapFilter {}
impl std::hash::Hash for SailMapFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailMapFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_map_filter"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("sail_map_filter: no args".into())
            })?
            .into_array(1)?;
        let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "sail_map_filter: expected MapArray".into(),
            )
        })?;

        let (keys, values) = map_keys_values(map);
        let mask = eval_predicate(
            &self.lambda_expr,
            &[self.key_param.clone(), self.val_param.clone()],
            vec![keys.clone(), values.clone()],
        )?;

        let new_keys = compute::filter(keys.as_ref(), &mask)?;
        let new_values = compute::filter(values.as_ref(), &mask)?;

        let old_offsets = map.offsets();
        let mut new_offsets: Vec<i32> = Vec::with_capacity(map.len() + 1);
        new_offsets.push(0);
        let mut total: usize = 0;
        for i in 0..map.len() {
            let start = old_offsets[i] as usize;
            let end = old_offsets[i + 1] as usize;
            let kept = (start..end).filter(|&j| mask.value(j)).count();
            total += kept;
            new_offsets.push(total as i32);
        }

        rebuild_map(
            map,
            new_keys,
            new_values,
            &self.return_type,
            new_offsets,
            map.nulls().cloned(),
            false,
        )
    }
}

// ============================================================
// SailMapZipWith
// ============================================================
#[derive(Debug)]
pub struct SailMapZipWith {
    id: u64,
    lambda_expr: Arc<dyn PhysicalExpr>,
    key_param: String,
    val1_param: String,
    val2_param: String,
    return_type: DataType,
    signature: Signature,
}

impl SailMapZipWith {
    pub fn new(
        lambda_expr: Arc<dyn PhysicalExpr>,
        key_param: String,
        val1_param: String,
        val2_param: String,
        return_type: DataType,
    ) -> Self {
        Self {
            id: next_hof_id(),
            lambda_expr,
            key_param,
            val1_param,
            val2_param,
            return_type,
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
}

impl PartialEq for SailMapZipWith {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SailMapZipWith {}
impl std::hash::Hash for SailMapZipWith {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SailMapZipWith {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sail_map_zip_with"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut args_iter = args.args.into_iter();
        let arr1 = args_iter
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "sail_map_zip_with: missing first arg".into(),
                )
            })?
            .into_array(1)?;
        let arr2 = args_iter
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "sail_map_zip_with: missing second arg".into(),
                )
            })?
            .into_array(1)?;
        let map1 = arr1.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "sail_map_zip_with: expected MapArray for arg1".into(),
            )
        })?;
        let map2 = arr2.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "sail_map_zip_with: expected MapArray for arg2".into(),
            )
        })?;
        // Zip keys from map1 with values from map1 and map2. For each row, iterate keys of map1
        // and look up the corresponding value in map2.
        let (keys1, vals1) = map_keys_values(map1);
        let (keys2, vals2) = map_keys_values(map2);

        let n_rows = map1.len();
        // Build result by iterating each row.
        let mut result_vals: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = vec![0i32];
        let mut total = 0i32;

        // Use ScalarValue approach: extract scalars from keys/vals per row
        for row in 0..n_rows {
            if map1.is_null(row) {
                offsets.push(total);
                continue;
            }
            let start1 = map1.offsets()[row] as usize;
            let end1 = map1.offsets()[row + 1] as usize;
            let start2 = map2.offsets()[row] as usize;
            let end2 = map2.offsets()[row + 1] as usize;

            let row_keys1 = keys1.slice(start1, end1 - start1);
            let row_vals1 = vals1.slice(start1, end1 - start1);
            let row_keys2 = keys2.slice(start2, end2 - start2);
            let row_vals2 = vals2.slice(start2, end2 - start2);

            let row_len = end1 - start1;

            // For each entry in map1, find the corresponding value in map2 (null if not present)
            let mut val2_for_k1: Vec<Option<usize>> = Vec::with_capacity(row_len);
            for i in 0..row_len {
                let k1 = ScalarValue::try_from_array(row_keys1.as_ref(), i)?;
                let mut found = None;
                for j in 0..(end2 - start2) {
                    let k2 = ScalarValue::try_from_array(row_keys2.as_ref(), j)?;
                    if k1 == k2 {
                        found = Some(j);
                        break;
                    }
                }
                val2_for_k1.push(found);
            }

            // Build indices into val2 (using null sentinel for missing)
            // Collect aligned val2 slice with nulls for missing keys
            let aligned_val2: Vec<ScalarValue> = val2_for_k1
                .iter()
                .map(|opt| {
                    if let Some(j) = opt {
                        ScalarValue::try_from_array(row_vals2.as_ref(), *j)
                    } else {
                        ScalarValue::try_from_array(
                            &datafusion::arrow::array::new_null_array(vals2.data_type(), 1),
                            0,
                        )
                    }
                })
                .collect::<std::result::Result<_, _>>()?;

            let aligned_val2_arr = if row_len > 0 {
                ScalarValue::iter_to_array(aligned_val2)?
            } else {
                datafusion::arrow::array::new_empty_array(vals2.data_type())
            };

            let transformed = eval_transform(
                &self.lambda_expr,
                &[
                    self.key_param.clone(),
                    self.val1_param.clone(),
                    self.val2_param.clone(),
                ],
                vec![row_keys1, row_vals1, aligned_val2_arr],
            )?;

            total += row_len as i32;
            offsets.push(total);
            result_vals.push(transformed);
        }

        // Concat all result_vals
        let result_val_arr = if result_vals.is_empty() {
            let val_type = match &self.return_type {
                DataType::Map(f, _) => match f.data_type() {
                    DataType::Struct(fields) => fields[1].data_type().clone(),
                    _ => return exec_err!("sail_map_zip_with: invalid return type"),
                },
                _ => return exec_err!("sail_map_zip_with: invalid return type"),
            };
            datafusion::arrow::array::new_empty_array(&val_type)
        } else {
            let refs: Vec<&dyn Array> = result_vals.iter().map(|a| a.as_ref()).collect();
            datafusion::arrow::compute::concat(&refs)?
        };

        // Build result keys (all keys from map1 in order)
        let result_key_arr = {
            let key_slices: Vec<ArrayRef> = (0..n_rows)
                .filter(|&row| !map1.is_null(row))
                .map(|row| {
                    let start = map1.offsets()[row] as usize;
                    let end = map1.offsets()[row + 1] as usize;
                    keys1.slice(start, end - start)
                })
                .collect();
            if key_slices.is_empty() {
                datafusion::arrow::array::new_empty_array(keys1.data_type())
            } else {
                let refs: Vec<&dyn Array> = key_slices.iter().map(|a| a.as_ref()).collect();
                datafusion::arrow::compute::concat(&refs)?
            }
        };

        rebuild_map(
            map1,
            result_key_arr,
            result_val_arr,
            &self.return_type,
            offsets,
            map1.nulls().cloned(),
            false,
        )
    }
}
