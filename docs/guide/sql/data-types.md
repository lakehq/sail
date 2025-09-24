---
title: Data Types
rank: 2
---

# Data Types

Sail supports Arrow data types that is a superset of data types available in Spark SQL.
For more background information, you can refer to the [Data Types](/guide/dataframe/data-types/) guide for the Spark DataFrame API.

The following table shows the SQL type syntax along with the corresponding Spark data types and Arrow data types.
Many data types have aliases not supported in JVM Spark. These are extensions in Sail.

<table tabindex="0">
  <thead>
    <tr>
      <th>SQL Type Syntax</th>
      <th>Spark Data Type</th>
      <th>Arrow Data Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>NULL</code><br /><code>VOID</code></td>
      <td><strong>NullType</strong></td>
      <td>Null</td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code><br /><code>BOOL</code></td>
      <td><strong>BooleanType</strong></td>
      <td>Boolean</td>
    </tr>
    <tr>
      <td>
        <code>BYTE</code><br /><code>TINYINT</code><br /><code>INT8</code>
      </td>
      <td><strong>ByteType</strong></td>
      <td>Int8</td>
    </tr>
    <tr>
      <td>
        <code>SHORT</code><br /><code>SMALLINT</code><br /><code>INT16</code>
      </td>
      <td><strong>ShortType</strong></td>
      <td>Int16</td>
    </tr>
    <tr>
      <td>
        <code>INTEGER</code><br /><code>INT</code><br /><code>INT32</code>
      </td>
      <td><strong>IntegerType</strong></td>
      <td>Int32</td>
    </tr>
    <tr>
      <td>
        <code>LONG</code><br /><code>BIGINT</code><br /><code>INT64</code>
      </td>
      <td><strong>LongType</strong></td>
      <td>Int64</td>
    </tr>
    <tr>
      <td>
        <code>UNSIGNED BYTE</code><br /><code>UNSIGNED TINYINT</code
        ><br /><code>UINT8</code>
      </td>
      <td>-</td>
      <td>UInt8</td>
    </tr>
    <tr>
      <td>
        <code>UNSIGNED SHORT</code><br /><code>UNSIGNED SMALLINT</code
        ><br /><code>UINT16</code>
      </td>
      <td>-</td>
      <td>UInt16</td>
    </tr>
    <tr>
      <td>
        <code>UNSIGNED INTEGER</code><br /><code>UNSIGNED INT</code
        ><br /><code>UINT32</code>
      </td>
      <td>-</td>
      <td>UInt32</td>
    </tr>
    <tr>
      <td>
        <code>UNSIGNED LONG</code><br /><code>UNSIGNED BIGINT</code
        ><br /><code>UINT64</code>
      </td>
      <td>-</td>
      <td>UInt64</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>Float16</td>
    </tr>
    <tr>
      <td>
        <code>FLOAT</code><br /><code>REAL</code><br /><code>FLOAT32</code>
      </td>
      <td><strong>FloatType</strong></td>
      <td>Float32</td>
    </tr>
    <tr>
      <td><code>DOUBLE</code><br /><code>FLOAT64</code></td>
      <td><strong>DoubleType</strong></td>
      <td>Float64</td>
    </tr>
    <tr>
      <td><code>DATE</code><br /><code>DATE32</code></td>
      <td><strong>DateType</strong></td>
      <td>Date32</td>
    </tr>
    <tr>
      <td><code>DATE64</code></td>
      <td>-</td>
      <td>Date64</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>
        Time32(Second)<br />Time32(Millisecond)<br />Time64(Microsecond)<br />Time64(Nanosecond)
      </td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'TIMESTAMP'['('<p>')']" /></code>
      </td>
      <td>
        <strong>TimestampType</strong><br /><strong>TimestampNTZType</strong>
      </td>
      <td>Timestamp(_, _)</td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'TIMESTAMP_LTZ'['('<p>')']" /></code><br />
        <code
          ><SyntaxText
            raw="'TIMESTAMP'['('<p>')']' WITH '['LOCAL ']'TIME ZONE'"
        /></code>
      </td>
      <td><strong>TimestampType</strong></td>
      <td>Timestamp(_, TimeZone(_))</td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'TIMESTAMP_NTZ'['('<p>')']" /></code><br />
        <code
          ><SyntaxText raw="'TIMESTAMP'['('<p>')']' WITHOUT TIME ZONE'"
        /></code>
      </td>
      <td><strong>TimestampNTZType</strong></td>
      <td>Timestamp(_, NoTimeZone)</td>
    </tr>
    <tr>
      <td><code>STRING</code></td>
      <td><strong>StringType</strong></td>
      <td>Utf8<br />LargeUtf8</td>
    </tr>
    <tr>
      <td><code>TEXT</code></td>
      <td>-</td>
      <td>LargeUtf8</td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'CHAR('<n>')'" /></code><br />
        <code><SyntaxText raw="'CHARACTER('<n>')'" /></code>
      </td>
      <td><strong>CharType(<em>n</em>)</strong></td>
      <td>Utf8<br />LargeUtf8</td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'VARCHAR('<n>')'" /></code>
      </td>
      <td><strong>VarcharType(<em>n</em>)</strong></td>
      <td>Utf8<br />LargeUtf8</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>Utf8View</td>
    </tr>
    <tr>
      <td><code>BINARY</code><br /><code>BYTEA</code></td>
      <td><strong>BinaryType</strong></td>
      <td>Binary<br />LargeBinary</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>FixedSizeBinary<br />BinaryView</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>Decimal32<br />Decimal64</td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'DECIMAL'['('<p>[', '<s>]')']" /></code
        ><br /><code><SyntaxText raw="'DEC'['('<p>[', '<s>]')']" /></code
        ><br /><code><SyntaxText raw="'NUMERIC'['('<p>[', '<s>]')']" /></code>
      </td>
      <td><strong>DecimalType</strong></td>
      <td>Decimal128<br />Decimal256</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>
        Duration(Second)<br />Duration(Millisecond)<br />Duration(Nanosecond)
      </td>
    </tr>
    <tr>
      <td>
        <code>INTERVAL YEAR</code><br /><code>INTERVAL YEAR TO MONTH</code
        ><br /><code>INTERVAL MONTH</code>
      </td>
      <td><strong>YearMonthIntervalType</strong></td>
      <td>Interval(YearMonth)</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>Interval(DayTime)</td>
    </tr>
    <tr>
      <td>
        <code>INTERVAL DAY</code><br /><code>INTERVAL DAY TO HOUR</code
        ><br /><code>INTERVAL DAY TO MINUTE</code><br /><code
          >INTERVAL DAY TO SECOND</code
        ><br /><code>INTERVAL HOUR</code><br /><code
          >INTERVAL HOUR TO MINUTE</code
        ><br /><code>INTERVAL HOUR TO SECOND</code><br /><code
          >INTERVAL MINUTE</code
        ><br /><code>INTERVAL MINUTE TO SECOND</code><br /><code
          >INTERVAL SECOND</code
        >
      </td>
      <td><strong>DayTimeIntervalType</strong></td>
      <td>Duration(Microsecond)</td>
    </tr>
    <tr>
      <td><code>INTERVAL</code></td>
      <td><strong>CalendarIntervalType</strong></td>
      <td>Interval(MonthDayNano)</td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'ARRAY<'<type>'>'" /></code>
      </td>
      <td><strong>ArrayType</strong></td>
      <td>List</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>LargeList<br />FixedSizeList<br />ListView<br />LargeListView</td>
    </tr>
    <tr>
      <td>
        <code><SyntaxText raw="'MAP<'<key-type>', '<value-type>'>'" /></code>
      </td>
      <td><strong>MapType</strong></td>
      <td>Map</td>
    </tr>
    <tr>
      <td>
        <code
          ><SyntaxText
            raw="'STRUCT<'<name>[':']' '<type>(', '<name>[':']' '<type>)*'>'"
        /></code>
      </td>
      <td><strong>StructType</strong></td>
      <td>Struct</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>Union</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>Dictionary</td>
    </tr>
    <tr>
      <td>-</td>
      <td>-</td>
      <td>RunEndEncoded</td>
    </tr>
  </tbody>
</table>

## Notes

1. The SQL string types (except `TEXT`) are mapped to either the Utf8 or LargeUtf8 type in Arrow, depending on the `spark.sql.execution.arrow.useLargeVarTypes` configuration option.
2. The SQL binary types are mapped to either the Binary or LargeBinary type in Arrow, depending on the `spark.sql.execution.arrow.useLargeVarTypes` configuration option.
3. The SQL `TIMESTAMP` type can either represent timestamps with local time zone (`TIMESTAMP_LTZ`, the default) or timestamps without time zone (`TIMESTAMP_NTZ`), depending on the `spark.sql.timestampType` configuration option.
4. For the SQL timestamp types, the optional <code><SyntaxText raw="<p>" /></code> parameter specifies the precision of the timestamp. A number of `0`, `3`, `6`, or `9` represents second, millisecond, microsecond, or nanosecond precision respectively. The default value is `6` (microsecond precision). Note that only the microsecond precision timestamp is compatible with Spark.
5. For the SQL decimal types, the optional <code><SyntaxText raw="<p>" /></code> and <code><SyntaxText raw="<s>" /></code> parameters specify the precision and scale of the decimal number respectively. The default precision is `10` and the default scale is `0`. The decimal type maps to either Decimal128 or Decimal256 type in Arrow depending on the specified precision.

<script setup>
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
