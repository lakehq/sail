"""Importable Python UDTs used by doctests.

Spark resolves Python UDTs by importing their declared module and no longer
unpickles UDT classes embedded in schema metadata. These test UDTs therefore
must be defined in a module instead of directly in the doctest namespace.

Reference: <https://issues.apache.org/jira/browse/SPARK-56463>
"""

from pyspark.sql.types import StringType, UserDefinedType


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
