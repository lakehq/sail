# Python DataSource API for Sail
# 100% PySpark-compatible (PySpark 3.5+/4.0+)

from .base import (
    # Core classes
    DataSource,
    DataSourceReader,
    InputPartition,
    CaseInsensitiveDict,
    # Filters
    EqualTo,
    EqualNullSafe,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    IsNull,
    IsNotNull,
    Not,
    And,
    Or,
    StringStartsWith,
    StringEndsWith,
    StringContains,
    Filter,
    ColumnPath,
    # Registration
    register,
    get_registered_datasource,
    list_registered_datasources,
)

# Example datasources (auto-registered on import)
from .examples import (
    RangeDataSource,
    RangeDataSourceReader,
    RangeInputPartition,
    ConstantDataSource,
    ConstantDataSourceReader,
    FlappyBirdDataSource,
    FlappyBirdReader,
)

__all__ = [
    # Core classes
    "DataSource",
    "DataSourceReader",
    "InputPartition",
    "CaseInsensitiveDict",
    # Filters
    "EqualTo",
    "EqualNullSafe",
    "GreaterThan",
    "GreaterThanOrEqual",
    "LessThan",
    "LessThanOrEqual",
    "In",
    "IsNull",
    "IsNotNull",
    "Not",
    "And",
    "Or",
    "StringStartsWith",
    "StringEndsWith",
    "StringContains",
    "Filter",
    "ColumnPath",
    # Registration
    "register",
    "get_registered_datasource",
    "list_registered_datasources",
    # Example datasources
    "RangeDataSource",
    "RangeDataSourceReader",
    "RangeInputPartition",
    "ConstantDataSource",
    "ConstantDataSourceReader",
    "FlappyBirdDataSource",
    "FlappyBirdReader",
]
