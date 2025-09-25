---
title: Compatibility
rank: 2
---

# Compatibility

All Spark data types are defined in the `pyspark.sql.types` package in PySpark.

The table below shows how Spark data types are mapped to Python types and Arrow data types.

<table tabindex="0">
  <thead>
    <tr>
      <th>Spark Data Type</th>
      <th>PySpark API</th>
      <th>Python Type</th>
      <th>Arrow Data Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>NullType</strong></td>
      <td><code>NullType()</code></td>
      <td>-</td>
      <td>Null</td>
    </tr>
    <tr>
      <td><strong>BooleanType</strong></td>
      <td><code>BooleanType()</code></td>
      <td><code>bool</code></td>
      <td>Boolean</td>
    </tr>
    <tr>
      <td><strong>ByteType</strong></td>
      <td><code>ByteType()</code></td>
      <td><code>int</code></td>
      <td>Int8</td>
    </tr>
    <tr>
      <td><strong>ShortType</strong></td>
      <td><code>ShortType()</code></td>
      <td><code>int</code></td>
      <td>Int16</td>
    </tr>
    <tr>
      <td><strong>IntegerType</strong></td>
      <td><code>IntegerType()</code></td>
      <td><code>int</code></td>
      <td>Int32</td>
    </tr>
    <tr>
      <td><strong>LongType</strong></td>
      <td><code>LongType()</code></td>
      <td><code>int</code></td>
      <td>Int64</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>UInt8<br />UInt16<br />UInt32<br />UInt64</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>Float16</td>
    </tr>
    <tr>
      <td><strong>FloatType</strong></td>
      <td><code>FloatType()</code></td>
      <td><code>float</code></td>
      <td>Float32</td>
    </tr>
    <tr>
      <td><strong>DoubleType</strong></td>
      <td><code>DoubleType()</code></td>
      <td><code>float</code></td>
      <td>Float64</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>Decimal32<br />Decimal64</td>
    </tr>
    <tr>
      <td><strong>DecimalType</strong></td>
      <td><code>DecimalType()</code></td>
      <td><code>decimal.Decimal</code></td>
      <td>Decimal128<br />Decimal256<br /></td>
    </tr>
    <tr>
      <td><strong>StringType</strong></td>
      <td><code>StringType()</code></td>
      <td><code>str</code></td>
      <td>Utf8<br />LargeUtf8</td>
    </tr>
    <tr>
      <td><strong>CharType(<em>n</em>)</strong></td>
      <td><code>CharType(<em>length</em>: int)</code></td>
      <td><code>str</code></td>
      <td>Utf8<br />LargeUtf8</td>
    </tr>
    <tr>
      <td><strong>VarcharType(<em>n</em>)</strong></td>
      <td><code>VarcharType(<em>length</em>: int)</code></td>
      <td><code>str</code></td>
      <td>Utf8<br />LargeUtf8</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>Utf8View</td>
    </tr>
    <tr>
      <td><strong>BinaryType</strong></td>
      <td><code>BinaryType()</code></td>
      <td><code>bytearray</code></td>
      <td>Binary<br />LargeBinary</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>FixedSizeBinary<br />BinaryView</td>
    </tr>
    <tr>
      <td><strong>TimestampType</strong></td>
      <td><code>TimestampType()</code></td>
      <td><code>datetime.datetime</code></td>
      <td>Timestamp(Microsecond, TimeZone(_))</td>
    </tr>
    <tr>
      <td><strong>TimestampNTZType</strong></td>
      <td><code>TimestampNTZType()</code></td>
      <td><code>datetime.datetime</code></td>
      <td>Timestamp(Microsecond, NoTimeZone)</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>
        Timestamp(Second, _)<br />Timestamp(Millisecond, _)<br />Timestamp(Nanosecond,
        _)
      </td>
    </tr>
    <tr>
      <td><strong>DateType</strong></td>
      <td><code>DateType()</code></td>
      <td><code>datetime.date</code></td>
      <td>Date32</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>Date64</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>
        Time32(Second)<br />Time32(Millisecond)<br />Time64(Microsecond)<br />Time64(Nanosecond)
      </td>
    </tr>
    <tr>
      <td><strong>YearMonthIntervalType</strong></td>
      <td><code>YearMonthIntervalType()</code></td>
      <td>-</td>
      <td>Interval(YearMonth)</td>
    </tr>
    <tr>
      <td><strong>DayTimeIntervalType</strong></td>
      <td><code>DayTimeIntervalType()</code></td>
      <td><code>datetime.timedelta</code></td>
      <td>Duration(Microsecond)</td>
    </tr>
    <tr>
      <td><strong>CalendarIntervalType</strong></td>
      <td><code>CalendarIntervalType()</code></td>
      <td>-</td>
      <td>Interval(MonthDayNano)</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>Interval(DayTime)</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>
        Duration(Second)<br />Duration(Millisecond)<br />Duration(Nanosecond)
      </td>
    </tr>
    <tr>
      <td><strong>ArrayType</strong></td>
      <td>
        <code
          >ArrayType(<em>elementType</em>, <em>containsNull</em>: bool =
          True)</code
        >
      </td>
      <td><code>list</code><br /><code>tuple</code></td>
      <td>List</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>LargeList<br />FixedSizeList<br />ListView<br />LargeListView</td>
    </tr>
    <tr>
      <td><strong>MapType</strong></td>
      <td>
        <code
          >MapType(<em>keyType</em>, <em>valueType</em>,
          <em>valueContainsNull</em>: bool = True)</code
        >
      </td>
      <td><code>dict</code></td>
      <td>Map</td>
    </tr>
    <tr>
      <td><strong>StructType</strong></td>
      <td><code>StructType(<em>fields</em>)</code></td>
      <td><code>list</code><br /><code>tuple</code></td>
      <td>Struct</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>Union</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>Dictionary</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>RunEndEncoded</td>
    </tr>
  </tbody>
</table>

## Notes

1. **DayTimeIntervalType** in Spark has microsecond precision, and it is mapped to the Duration(Microsecond) Arrow type. It is not mapped to the Interval(DayTime) Arrow type which only has millisecond precision.
2. **YearMonthIntervalType** and **CalendarIntervalType** in Spark are not supported in Python, so calling the `.collect()` method will raise an error for a DataFrame that contains these types.
3. **StringType**, **CharType(_n_)**, and **VarcharType(_n_)** in Spark are mapped to either the Utf8 or LargeUtf8 type in Arrow, depending on the `spark.sql.execution.arrow.useLargeVarTypes` configuration option.
4. **BinaryType** in Spark is mapped to either the Binary or LargeBinary type in Arrow, depending on the `spark.sql.execution.arrow.useLargeVarTypes` configuration option.
5. **CalendarIntervalType** in Spark has microsecond precision while the Interval(MonthDayNano) Arrow type has nanosecond precision. So the supported data range for calendar intervals is different between JVM Spark and Arrow.
