from __future__ import annotations

import boto3
import pytest


@pytest.mark.skip(reason="moto does not support Glue CreateUserDefinedFunction API")
def test_glue_catalog_get_function(glue_spark, moto_endpoint):
    client = boto3.client(
        "glue",
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )

    function_name = "my_glue_function"
    client.create_user_defined_function(
        DatabaseName="test_db",
        FunctionInput={
            "FunctionName": function_name,
            "ClassName": "com.example.MyGlueFunction",
            "OwnerName": "owner",
            "OwnerType": "USER",
        },
    )

    assert glue_spark.catalog.functionExists(f"test_db.{function_name}") == True  # noqa: E712

    function = glue_spark.catalog.getFunction(f"test_db.{function_name}")
    assert function.name == function_name
    assert function.catalog == "sail"
    assert function.namespace == ["test_db"]
    assert function.className == "com.example.MyGlueFunction"
    assert function.isTemporary == False  # noqa: E712
