"""Importable Python UDTs used by doctests.

Spark resolves Python UDTs by importing their declared module and no longer
unpickles UDT classes embedded in schema metadata. These test UDTs therefore
must be defined in a module instead of directly in the doctest namespace.

Reference: <https://issues.apache.org/jira/browse/SPARK-56463>
"""

from pyspark.sql.types import ArrayType, DoubleType, StringType, UserDefinedType


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


class PythonPointUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):  # noqa: N802
        return ArrayType(DoubleType(), containsNull=False)

    @classmethod
    def module(cls):
        return __name__

    def serialize(self, obj):
        return [obj.x, obj.y]

    def deserialize(self, datum):
        return PythonPoint(*datum)


class PythonPoint:
    __UDT__ = PythonPointUDT()

    def __init__(self, x, y):
        self.x = x
        self.y = y
