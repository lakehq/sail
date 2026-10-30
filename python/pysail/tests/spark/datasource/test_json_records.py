"""JSON reader options that change how records are found and parsed."""

import pytest
from pyspark.sql import Row

PEOPLE = '{"name":"Michael"}\n{"name":"Andy", "age":30}\n{"name":"Justin", "age":19}\n'


@pytest.fixture
def people(tmp_path):
    path = tmp_path / "people.json"
    path.write_text(PEOPLE)
    return str(path)


def test_primitives_as_string(spark, tmp_path):
    path = tmp_path / "primitives.json"
    path.write_text('{"a": 100, "b": true, "c": 1.5, "d": "x", "e": {"f": 2}, "g": [1, 2]}\n')
    df = spark.read.json(str(path), primitivesAsString=True)
    assert df.schema.simpleString() == "struct<a:string,b:string,c:string,d:string,e:struct<f:string>,g:array<string>>"
    assert df.collect() == [Row(a="100", b="true", c="1.5", d="x", e=Row(f="2"), g=["1", "2"])]


def test_multi_line_array(spark, people, tmp_path):
    path = tmp_path / "people_array.json"
    path.write_text('[\n  {"name": "Michael"},\n  {"name": "Andy", "age": 30},\n  {"name": "Justin", "age": 19}\n]\n')
    expected = spark.read.json(people).collect()
    assert spark.read.json(str(path), multiLine=True).collect() == expected


def test_multi_line_object(spark, tmp_path):
    path = tmp_path / "object.json"
    path.write_text('{\n  "name": "Andy",\n  "age": 30\n}\n')
    assert spark.read.json(str(path), multiLine=True).collect() == [Row(age=30, name="Andy")]


def test_line_sep_with_corrupt_records(spark, people):
    df = spark.read.json(people, lineSep=",")
    assert df.collect() == [
        Row(_corrupt_record=None, name="Michael"),
        Row(_corrupt_record=' "age":30}\n{"name":"Justin"', name=None),
        Row(_corrupt_record=' "age":19}\n', name=None),
    ]


def test_encoding(spark, tmp_path):
    path = tmp_path / "people_utf16le.json"
    path.write_bytes('[{"name": "Andy", "age": 30}, {"name": "Justin", "age": 19}]'.encode("utf-16-le"))
    df = spark.read.json(str(path), multiLine=True, encoding="UTF-16LE")
    assert df.collect() == [Row(age=30, name="Andy"), Row(age=19, name="Justin")]


def test_corrupt_record_modes(spark, tmp_path):
    path = tmp_path / "corrupt.json"
    path.write_text('{"a": 1}\n{"a": \n{"a": 3}\n')
    df = spark.read.json(str(path))
    assert df.collect() == [
        Row(_corrupt_record=None, a=1),
        Row(_corrupt_record='{"a": ', a=None),
        Row(_corrupt_record=None, a=3),
    ]
    df = spark.read.json(str(path), mode="DROPMALFORMED")
    assert df.collect() == [Row(a=1), Row(a=3)]
    with pytest.raises(Exception, match="Malformed records are detected"):
        spark.read.schema("a long").json(str(path), mode="FAILFAST").collect()


def test_corrupt_record_column_in_the_schema(spark, tmp_path):
    path = tmp_path / "corrupt.json"
    path.write_text('{"a": 1}\nnot json\n')
    df = spark.read.schema("a long, bad string").json(str(path), columnNameOfCorruptRecord="bad")
    assert df.collect() == [Row(a=1, bad=None), Row(a=None, bad="not json")]


def test_write_line_sep(spark, people, tmp_path):
    path = tmp_path / "people_line_sep"
    df = spark.read.json(people)
    df.coalesce(1).write.json(str(path), lineSep="!!")
    [file] = path.glob("*.json")
    assert file.read_text() == '{"name":"Michael"}!!{"age":30,"name":"Andy"}!!{"age":19,"name":"Justin"}!!'
    assert spark.read.json(str(path), lineSep="!!").collect() == df.collect()
