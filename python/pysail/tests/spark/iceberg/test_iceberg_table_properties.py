import pytest
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import BucketTransform, IdentityTransform, TruncateTransform
from pyiceberg.types import IntegerType, NestedField, StringType, StructType


@pytest.mark.parametrize("entry", ["path", "catalog"])
@pytest.mark.parametrize("evolved", [False, True], ids=["identity", "transformed-and-renamed"])
def test_show_properties_includes_sort_order_and_identifier_fields(spark, sql_catalog, entry, evolved):
    identifier = "default.show_derived_properties"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(
            field_id=2,
            name="payload",
            field_type=StructType(NestedField(3, "value", StringType(), required=True)),
            required=True,
        ),
        identifier_field_ids=[3] if evolved else [1],
    )
    sort_order = SortOrder(
        SortField(
            source_id=3 if evolved else 1,
            transform=TruncateTransform(8) if evolved else IdentityTransform(),
            direction=SortDirection.DESC,
            null_order=NullOrder.NULLS_LAST,
        )
    )
    if evolved:
        sort_order = SortOrder(
            *sort_order.fields,
            SortField(1, BucketTransform(16), SortDirection.ASC, NullOrder.NULLS_FIRST),
        )
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=schema,
        sort_order=sort_order,
        properties={"sort-order": "stale", "identifier-fields": "stale"},
    )
    name = "show_derived_properties"
    registered = False
    try:
        if evolved:
            with table.update_schema() as update:
                update.rename_column("payload.value", "code")
        if entry == "catalog":
            spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
            registered = True
            reference = name
        else:
            reference = f"iceberg.`{table.location()}`"
        # Spark 4.1.3 with Iceberg 1.11.0 derives these values from table metadata.
        expected = {"sort-order": "id DESC NULLS LAST", "identifier-fields": "[id]"}
        if evolved:
            expected = {
                "sort-order": "truncate(payload.code, 8) DESC NULLS LAST, bucket(16, id) ASC NULLS FIRST",
                "identifier-fields": "[payload.code]",
            }
        properties = dict(spark.sql(f"SHOW TBLPROPERTIES {reference}").collect())
        assert {key: properties.get(key) for key in expected} == expected
        for key, value in expected.items():
            assert [tuple(row) for row in spark.sql(f"SHOW TBLPROPERTIES {reference} ('{key}')").collect()] == [
                (key, value)
            ]
    finally:
        if registered:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)


def test_show_properties_ignores_derived_properties_without_schema_or_sort_metadata(spark, sql_catalog):
    identifier = "default.show_shadowed_properties"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(NestedField(1, "id", IntegerType(), required=True)),
        properties={"sort-order": "stale", "identifier-fields": "stale"},
    )
    try:
        properties = dict(spark.sql(f"SHOW TBLPROPERTIES iceberg.`{table.location()}`").collect())
        assert "sort-order" not in properties
        assert "identifier-fields" not in properties
    finally:
        sql_catalog.drop_table(identifier)
