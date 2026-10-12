"""Importable Python UDTs used by doctests.

Spark resolves Python UDTs by importing their declared module and no longer
unpickles UDT classes embedded in schema metadata. These test UDTs therefore
must be defined in a module instead of directly in the doctest namespace.

Reference: <https://issues.apache.org/jira/browse/SPARK-56463>
"""

from pyspark.sql.types import DoubleType, IntegerType, StringType, UserDefinedType


class UnnamedPythonUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):  # noqa: N802
        return StringType()

    @classmethod
    def module(cls):
        return __name__


class NamedPythonUDT(UnnamedPythonUDT):
    def simpleString(self):  # noqa: N802
        return "foo"


class IntegerStoragePythonUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):  # noqa: N802
        return IntegerType()

    @classmethod
    def module(cls):
        return __name__


class DoubleStoragePythonUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):  # noqa: N802
        return DoubleType()

    @classmethod
    def module(cls):
        return __name__


class Box:
    """A value with a UDT that serializes, so rows of it can be created and collected."""

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return type(self) is type(other) and self.value == other.value

    def __hash__(self):
        return hash(self.value)

    def __repr__(self):
        return f"Box({self.value!r})"


class BoxPythonUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):  # noqa: N802
        return StringType()

    @classmethod
    def module(cls):
        return __name__

    def serialize(self, obj):
        return obj.value

    def deserialize(self, datum):
        return Box(datum)


Box.__UDT__ = BoxPythonUDT()
