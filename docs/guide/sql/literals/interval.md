---
title: Interval Literals
rank: 7
---

# Interval Literals

Interval literals denote fixed spans of time.

Interval literals can be written in the **ANSI** or **multi-unit** syntax.

## ANSI Interval Syntax

The ANSI SQL standard defines interval literals in the following form:

<SyntaxBlock>
  <SyntaxText
    raw="'INTERVAL '['+'|'-']<interval-string>' '<interval-qualifier>"
  />
</SyntaxBlock>

<code><SyntaxText raw="<interval-qualifier>" /></code> can be a single field
or in the field-to-field form:

<SyntaxBlock>
  <SyntaxText raw="<field>" />
  <SyntaxText raw="<field>' TO '<field>" />
</SyntaxBlock>

<code><SyntaxText raw="<field>" /></code> is case-insensitive, and can be one
of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `SECOND`. Note that in the
field-to-field form, the first field must be of a coarser granularity than the
second field. The valid field pairs are listed in the interval format tables
later in this section.

<code><SyntaxText raw="<interval-string>" /></code> is an interval literal
string that corresponds to either the year-month or day-time interval type:

<SyntaxBlock>
  <SyntaxText
    raw="''''['+'|'-'](<year-month-literal>|<day-time-literal>)''''"
  />
</SyntaxBlock>

<code><SyntaxText raw="<year-month-literal>" /></code> takes the following
forms:

<SyntaxBlock>
  <SyntaxText raw="<y>['-'<m>]" />
  <SyntaxText raw="<m>" />
</SyntaxBlock>

<code><SyntaxText raw="<y>" /></code> and
<code><SyntaxText raw="<m>" /></code> are the number of years and months,
respectively.

<code><SyntaxText raw="<day-time-literal>" /></code> takes the following
forms:

<SyntaxBlock>
  <SyntaxText raw="<d>[' '<H>[':'<M>[':'<S>['.'<f>]]]]" />
  <SyntaxText raw="<H>[':'<M>[':'<S>['.'<f>]]]" />
  <SyntaxText raw="<M>[':'<S>['.'<f>]]" />
  <SyntaxText raw="<S>['.'<f>]" />
</SyntaxBlock>

<code><SyntaxText raw="<d>" /></code>, <code><SyntaxText raw="<H>" /></code>,
<code><SyntaxText raw="<M>" /></code>, <code><SyntaxText raw="<S>" /></code>,
and <code><SyntaxText raw="<f>" /></code> are the number of days, hours,
minutes, seconds, and fractional seconds, respectively.

Here are the supported formats for year-month interval literals.

<table tabindex="0">
  <thead>
    <tr>
      <th>Qualifier</th>
      <th>Signed String</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>YEAR</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<y>''''" /></code>
      </td>
      <td><code>INTERVAL -'1999' YEAR</code></td>
    </tr>
    <tr>
      <td><code>YEAR TO MONTH</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<y>'-'<m>''''" /></code>
      </td>
      <td><code>INTERVAL '-1999-11' YEAR TO MONTH</code></td>
    </tr>
    <tr>
      <td><code>MONTH</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<m>''''" /></code>
      </td>
      <td><code>INTERVAL '24' MONTH</code></td>
    </tr>
  </tbody>
</table>

Here are the supported formats for day-time interval literals.

<table tabindex="0">
  <thead>
    <tr>
      <th>Qualifier</th>
      <th>Signed String</th>
      <th>Example</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>DAY</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<d>''''" /></code>
      </td>
      <td><code>INTERVAL -'365' DAY</code></td>
    </tr>
    <tr>
      <td><code>DAY TO HOUR</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<d>' '<H>''''" /></code>
      </td>
      <td><code>INTERVAL '-10 05' DAY TO HOUR</code></td>
    </tr>
    <tr>
      <td><code>DAY TO MINUTE</code></td>
      <td>
        <code
          ><SyntaxText raw="['+'|'-']''''['+'|'-']<d>' '<H>':'<M>''''"
        /></code>
      </td>
      <td><code>INTERVAL '100 10:30' DAY TO MINUTE</code></td>
    </tr>
    <tr>
      <td><code>DAY TO SECOND</code></td>
      <td>
        <code
          ><SyntaxText
            raw="['+'|'-']''''['+'|'-']<d>' '<H>':'<M>':'<S>['.'<f>]''''"
        /></code>
      </td>
      <td><code>INTERVAL '100 10:30:40.999999' DAY TO SECOND</code></td>
    </tr>
    <tr>
      <td><code>HOUR</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<H>''''" /></code>
      </td>
      <td><code>INTERVAL '123' HOUR</code></td>
    </tr>
    <tr>
      <td><code>HOUR TO MINUTE</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<H>':'<M>''''" /></code>
      </td>
      <td><code>INTERVAL -'-15:45' HOUR TO MINUTE</code></td>
    </tr>
    <tr>
      <td><code>HOUR TO SECOND</code></td>
      <td>
        <code
          ><SyntaxText
            raw="['+'|'-']''''['+'|'-']<H>':'<M>':'<S>['.'<f>]''''"
        /></code>
      </td>
      <td><code>INTERVAL '123:10:59' HOUR TO SECOND</code></td>
    </tr>
    <tr>
      <td><code>MINUTE</code></td>
      <td>
        <code><SyntaxText raw="['+'|'-']''''['+'|'-']<M>''''" /></code>
      </td>
      <td><code>INTERVAL '5000' MINUTE</code></td>
    </tr>
    <tr>
      <td><code>MINUTE TO SECOND</code></td>
      <td>
        <code
          ><SyntaxText raw="['+'|'-']''''['+'|'-']<M>':'<S>['.'<f>]''''"
        /></code>
      </td>
      <td><code>INTERVAL '2000:02.002' MINUTE TO SECOND</code></td>
    </tr>
    <tr>
      <td><code>SECOND</code></td>
      <td>
        <code
          ><SyntaxText raw="['+'|'-']''''['+'|'-']<S>['.'<f>]''''"
        /></code>
      </td>
      <td><code>INTERVAL '2000.000002' SECOND</code></td>
    </tr>
  </tbody>
</table>

## ANSI Interval Examples

```sql
SELECT INTERVAL '5-6' YEAR TO MONTH AS col;
-- +----------------------------+
-- |col                         |
-- +----------------------------+
-- |INTERVAL '5-6' YEAR TO MONTH|
-- +----------------------------+

SELECT INTERVAL -'12 23:45:59.888888' DAY TO SECOND AS col;
-- +--------------------------------------------+
-- |col                                         |
-- +--------------------------------------------+
-- |INTERVAL '-12 23:45:59.888888' DAY TO SECOND|
-- +--------------------------------------------+
```

## Multi-unit Interval Syntax

<SyntaxBlock>
  <SyntaxText
    raw="'INTERVAL '<interval-value>' '<interval-unit>(' '<interval-value>' '<interval-unit>)*"
  />
  <SyntaxText
    raw="'INTERVAL '''<interval-value>' '<interval-unit>(' '<interval-value>' '<interval-unit>)*''''"
  />
</SyntaxBlock>

<code><SyntaxText raw="<interval-value>" /></code> is a signed integer or its
string representation.

<SyntaxBlock>
  <SyntaxText raw="['+'|'-']<number>" />
  <SyntaxText raw="''''['+'|'-']<number>''''" />
</SyntaxBlock>

<code><SyntaxText raw="<interval-unit>" /></code> is a case-insensitive
interval unit:

<SyntaxBlock>
  <SyntaxText raw="'YEAR'['S']" />
  <SyntaxText raw="'MONTH'['S']" />
  <SyntaxText raw="'WEEK'['S']" />
  <SyntaxText raw="'DAY'['S']" />
  <SyntaxText raw="'HOUR'['S']" />
  <SyntaxText raw="'MINUTE'['S']" />
  <SyntaxText raw="'SECOND'['S']" />
  <SyntaxText raw="'MILLISECOND'['S']" />
  <SyntaxText raw="'MICROSECOND'['S']" />
</SyntaxBlock>

Mix of the <code><SyntaxText raw="'YEAR'['S']" /></code> or <code><SyntaxText raw="'MONTH'['S']" /></code> interval units with other units is not allowed.

::: info
Although Sail supports the <code><SyntaxText raw="'YEAR'['S']" /></code> and <code><SyntaxText raw="'MONTH'['S']" /></code> interval units in the multi-unit syntax, the Spark client is unable to convert these from Arrow when invoking `.collect()` or `.toPandas()`.
:::

## Multi-unit Interval Examples

```sql
SELECT INTERVAL 3 WEEK AS col;
-- +-----------------+
-- |col              |
-- +-----------------+
-- |INTERVAL '21' DAY|
-- +-----------------+

SELECT INTERVAL -2 WEEKS '3' DAYS AS col;
-- +------------------+
-- |col               |
-- +------------------+
-- |INTERVAL '-11' DAY|
-- +------------------+

SELECT INTERVAL '3 DAYS 50 SECONDS' AS col;
-- +-----------------------------------+
-- |col                                |
-- +-----------------------------------+
-- |INTERVAL '3 00:00:50' DAY TO SECOND|
-- +-----------------------------------+

SELECT INTERVAL 3 WEEK 4 DAYS 5 HOUR 6 MINUTES 7 SECOND 8 MILLISECOND 9 MICROSECONDS AS col;
-- +-------------------------------------------+
-- |col                                        |
-- +-------------------------------------------+
-- |INTERVAL '25 05:06:07.008009' DAY TO SECOND|
-- +-------------------------------------------+
```

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
