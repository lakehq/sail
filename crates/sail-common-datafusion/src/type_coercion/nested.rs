use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};

/// Fills in missing field metadata of a nested data type from a data type of the same shape, and
/// makes its nested fields nullable if the corresponding fields are nullable.
pub fn with_nested_field_metadata(data_type: DataType, source: &DataType) -> DataType {
    let field = |field: &FieldRef, source: &FieldRef| {
        let data_type = with_nested_field_metadata(field.data_type().clone(), source.data_type());
        let mut field = field
            .as_ref()
            .clone()
            .with_data_type(data_type)
            .with_nullable(field.is_nullable() || source.is_nullable());
        if field.metadata().is_empty() {
            field.set_metadata(source.metadata().clone());
        }
        Arc::new(field)
    };
    match (&data_type, source) {
        (DataType::Struct(fields), DataType::Struct(sources)) => {
            let sources = match fields
                .iter()
                .map(|field| sources.find(field.name()).map(|(_, source)| source))
                .collect::<Option<Vec<_>>>()
            {
                Some(sources) => sources,
                None if fields.len() == sources.len() => sources.iter().collect(),
                None => return data_type,
            };
            DataType::Struct(
                fields
                    .iter()
                    .zip(sources)
                    .map(|(f, source)| field(f, source))
                    .collect(),
            )
        }
        (
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::ListView(f)
            | DataType::LargeListView(f),
            DataType::List(source)
            | DataType::LargeList(source)
            | DataType::FixedSizeList(source, _)
            | DataType::ListView(source)
            | DataType::LargeListView(source),
        ) => match &data_type {
            DataType::List(_) => DataType::List(field(f, source)),
            DataType::LargeList(_) => DataType::LargeList(field(f, source)),
            DataType::FixedSizeList(_, size) => DataType::FixedSizeList(field(f, source), *size),
            DataType::ListView(_) => DataType::ListView(field(f, source)),
            DataType::LargeListView(_) => DataType::LargeListView(field(f, source)),
            _ => data_type,
        },
        (DataType::Map(f, sorted), DataType::Map(source, _)) => {
            DataType::Map(field(f, source), *sorted)
        }
        _ => data_type,
    }
}

/// The common type of data types that differ only in the nullability of nested fields, which is
/// nullable wherever one of them is nullable, as Spark's
/// `TypeCoercion.findCommonTypeDifferentOnlyInNullFlags` finds it. Returns `None` if there are
/// no data types or if they differ otherwise, e.g. in field names, metadata or element types.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala#L298-L320>
pub fn widen_nested_nullability(data_types: &[DataType]) -> Option<DataType> {
    let (first, others) = data_types.split_first()?;
    let widened = others.iter().fold(first.clone(), |widened, data_type| {
        with_nested_field_metadata(widened, data_type)
    });
    data_types
        .iter()
        .all(|data_type| with_nested_field_metadata(data_type.clone(), &widened) == widened)
        .then_some(widened)
}

/// [`widen_nested_nullability`] where, in addition, a nested field of the NULL type (e.g. the
/// elements of `array(NULL)`, at any depth) fits a field of any type and becomes nullable, as in
/// Spark's `findTightestCommonType` and `findTypeForComplex` (`Cast.forceNullable` from
/// `NullType`). The names of list elements and map entries do not matter (e.g. `element` in
/// Parquet and `item` in Arrow), as in Spark, where they have no names; the first name is kept.
/// A list and a large list have the large list type in common.
/// Returns `None` if there are no data types or if they differ otherwise.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala#L76-L79>
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercionHelper.scala#L137-L179>
pub fn widen_list_nullability(data_types: &[DataType]) -> Option<DataType> {
    fn widen(left: &DataType, right: &DataType) -> Option<DataType> {
        let field = |left: &FieldRef, right: &FieldRef, named: bool| -> Option<FieldRef> {
            let (l, r) = (left.metadata(), right.metadata());
            if (named && left.name() != right.name()) || (!l.is_empty() && !r.is_empty() && l != r)
            {
                return None;
            }
            let data_type = match (left.data_type(), right.data_type()) {
                (DataType::Null, other) | (other, DataType::Null) => other.clone(),
                (l, r) => widen(l, r)?,
            };
            let nullable = left.is_nullable()
                || right.is_nullable()
                || left.data_type().is_null()
                || right.data_type().is_null();
            let metadata = if l.is_empty() { r } else { l };
            Some(Arc::new(
                left.as_ref()
                    .clone()
                    .with_data_type(data_type)
                    .with_nullable(nullable)
                    .with_metadata(metadata.clone()),
            ))
        };
        match (left, right) {
            (DataType::List(l), DataType::List(r)) => Some(DataType::List(field(l, r, false)?)),
            // A list and a large list have the large list type in common.
            (DataType::LargeList(l), DataType::LargeList(r))
            | (DataType::List(l), DataType::LargeList(r))
            | (DataType::LargeList(l), DataType::List(r)) => {
                Some(DataType::LargeList(field(l, r, false)?))
            }
            (DataType::ListView(l), DataType::ListView(r)) => {
                Some(DataType::ListView(field(l, r, false)?))
            }
            (DataType::LargeListView(l), DataType::LargeListView(r)) => {
                Some(DataType::LargeListView(field(l, r, false)?))
            }
            (DataType::FixedSizeList(l, ls), DataType::FixedSizeList(r, rs)) if ls == rs => {
                Some(DataType::FixedSizeList(field(l, r, false)?, *ls))
            }
            (DataType::Map(l, ls), DataType::Map(r, rs)) if ls == rs => {
                Some(DataType::Map(field(l, r, false)?, *ls))
            }
            (DataType::Struct(l), DataType::Struct(r)) if l.len() == r.len() => {
                let fields = l
                    .iter()
                    .zip(r.iter())
                    .map(|(l, r)| field(l, r, true))
                    .collect::<Option<Vec<_>>>()?;
                Some(DataType::Struct(fields.into()))
            }
            _ => widen_nested_nullability(&[left.clone(), right.clone()]),
        }
    }
    let (first, others) = data_types.split_first()?;
    others.iter().try_fold(first.clone(), |widened, data_type| {
        widen(&widened, data_type)
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;

    fn list(nullable: bool) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, nullable)))
    }

    #[test]
    fn test_widen_nested_nullability() {
        let nested =
            |nullable: bool| DataType::Struct(vec![Field::new("a", list(nullable), false)].into());
        assert_eq!(
            widen_nested_nullability(&[list(false), list(true)]),
            Some(list(true))
        );
        assert_eq!(
            widen_nested_nullability(&[nested(true), nested(false), nested(false)]),
            Some(nested(true))
        );
        assert_eq!(
            widen_nested_nullability(&[list(false), list(false)]),
            Some(list(false))
        );
        let large_list =
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
        assert_eq!(widen_nested_nullability(&[list(false), large_list]), None);
        let renamed = DataType::Struct(vec![Field::new("b", list(true), false)].into());
        assert_eq!(widen_nested_nullability(&[nested(false), renamed]), None);
        let int64_list = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        assert_eq!(widen_nested_nullability(&[list(false), int64_list]), None);
        assert_eq!(widen_nested_nullability(&[]), None);
    }

    #[test]
    fn test_widen_list_nullability() {
        let null_list = |nullable: bool| {
            DataType::List(Arc::new(Field::new_list_field(DataType::Null, nullable)))
        };
        assert_eq!(
            widen_list_nullability(&[list(false), null_list(true)]),
            Some(list(true))
        );
        assert_eq!(
            widen_list_nullability(&[null_list(false), list(false)]),
            Some(list(true))
        );
        assert_eq!(
            widen_list_nullability(&[list(false), list(true)]),
            Some(list(true))
        );
        assert_eq!(
            widen_list_nullability(&[null_list(true), null_list(true)]),
            Some(null_list(true))
        );
        let int64_list = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        assert_eq!(widen_list_nullability(&[list(false), int64_list]), None);
        // A list of NULL values nested in a list or a struct.
        let outer = |inner: DataType| DataType::List(Arc::new(Field::new_list_field(inner, false)));
        assert_eq!(
            widen_list_nullability(&[outer(list(false)), outer(null_list(true))]),
            Some(outer(list(true)))
        );
        let r#struct =
            |inner: DataType| DataType::Struct(vec![Field::new("a", inner, false)].into());
        assert_eq!(
            widen_list_nullability(&[r#struct(null_list(true)), r#struct(list(false))]),
            Some(r#struct(list(true)))
        );
        let int64_struct = DataType::Struct(vec![Field::new("a", DataType::Int64, false)].into());
        assert_eq!(
            widen_list_nullability(&[r#struct(DataType::Null), int64_struct]),
            Some(DataType::Struct(
                vec![Field::new("a", DataType::Int64, true)].into()
            ))
        );
        // The names of list elements do not matter, e.g. `element` in Parquet.
        let element_list = DataType::List(Arc::new(Field::new("element", DataType::Int32, true)));
        assert_eq!(
            widen_list_nullability(&[element_list.clone(), list(false)]),
            Some(element_list.clone())
        );
        assert_eq!(
            widen_list_nullability(&[list(false), element_list]),
            Some(list(true))
        );
        // A list and a large list have the large list type in common.
        let large_list =
            DataType::LargeList(Arc::new(Field::new("element", DataType::Int32, true)));
        assert_eq!(
            widen_list_nullability(&[list(false), large_list.clone()]),
            Some(DataType::LargeList(Arc::new(Field::new_list_field(
                DataType::Int32,
                true
            ))))
        );
        assert_eq!(
            widen_list_nullability(&[large_list.clone(), list(false)]),
            Some(large_list)
        );
        // The names of the elements of every list type and of map entries do not matter.
        let element =
            |name: &str, nullable: bool| Arc::new(Field::new(name, DataType::Int32, nullable));
        let kinds: [fn(FieldRef) -> DataType; 5] = [
            DataType::List,
            DataType::LargeList,
            DataType::ListView,
            DataType::LargeListView,
            |field| DataType::FixedSizeList(field, 2),
        ];
        for kind in kinds {
            assert_eq!(
                widen_list_nullability(&[
                    kind(element("element", true)),
                    kind(element("item", false))
                ]),
                Some(kind(element("element", true)))
            );
            assert_eq!(
                widen_list_nullability(&[
                    kind(element("item", false)),
                    kind(element("element", true))
                ]),
                Some(kind(element("item", true)))
            );
        }
        let map = |name: &str, nullable: bool| {
            let entries = vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, nullable),
            ];
            DataType::Map(
                Arc::new(Field::new(name, DataType::Struct(entries.into()), false)),
                false,
            )
        };
        assert_eq!(
            widen_list_nullability(&[map("key_value", false), map("entries", true)]),
            Some(map("key_value", true))
        );

        let b_struct = DataType::Struct(vec![Field::new("b", list(false), false)].into());
        assert_eq!(
            widen_list_nullability(&[r#struct(null_list(true)), b_struct]),
            None
        );
    }
}
