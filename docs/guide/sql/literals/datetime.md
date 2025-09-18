---
title: Datetime Literals
rank: 6
---

# Datetime Literals

Datetime literals capture specific dates or timestamps.

## Date Syntax

<SyntaxBlock>
  <SyntaxText raw="'DATE '''<y><y><y><y>''''" />
  <SyntaxText raw="'DATE '''<y><y><y><y>'-'<m>[<m>]''''" />
  <SyntaxText raw="'DATE '''<y><y><y><y>'-'<m>[<m>]'-'<d>[<d>]''''" />
  <SyntaxText raw="'DATE '''<y><y><y><y>'-'<m>[<m>]'-'<d>[<d>]'T'''" />
</SyntaxBlock>

<code><SyntaxText raw="<y>" /></code>, <code><SyntaxText raw="<m>" /></code>,
and <code><SyntaxText raw="<d>" /></code> represent a digit (`0`-`9`) for the
year, month, and day, respectively. The month or day defaults to `01` if not
specified.

## Date Examples

```sql
SELECT DATE '2001' AS col;
-- +----------+
-- |       col|
-- +----------+
-- |2001-01-01|
-- +----------+

SELECT DATE '2005-07' AS col;
-- +----------+
-- |       col|
-- +----------+
-- |2005-07-01|
-- +----------+

SELECT DATE '2019-12-25' AS col;
-- +----------+
-- |       col|
-- +----------+
-- |2019-12-25|
-- +----------+
```

## Timestamp Syntax

<SyntaxBlock>
  <SyntaxText raw="'TIMESTAMP '''<y><y><y><y>''''" />
  <SyntaxText raw="'TIMESTAMP '''<y><y><y><y>'-'<m>[<m>]''''" />
  <SyntaxText raw="'TIMESTAMP '''<y><y><y><y>'-'<m>[<m>]'-'<d>[<d>]''''" />
  <SyntaxText
    raw="'TIMESTAMP '''<y><y><y><y>'-'<m>[<m>]'-'<d>[<d>]('T'|' ')<H>[<H>]''''"
  />
  <SyntaxText
    raw="'TIMESTAMP '''<y><y><y><y>'-'<m>[<m>]'-'<d>[<d>]('T'|' ')<H>[<H>]':'<M>[<M>]''''"
  />
  <SyntaxText
    raw="'TIMESTAMP '''<y><y><y><y>'-'<m>[<m>]'-'<d>[<d>]('T'|' ')<H>[<H>]':'<M>[<M>]':'<S>[<S>]['.'<fraction>][<zone>]''''"
  />
</SyntaxBlock>

<code><SyntaxText raw="<y>" /></code>, <code><SyntaxText raw="<m>" /></code>,
<code><SyntaxText raw="<d>" /></code>, <code><SyntaxText raw="<H>" /></code>,
<code><SyntaxText raw="<M>" /></code>, and
<code><SyntaxText raw="<S>" /></code> represent a digit (`0`-`9`) for the
year, month, day, hour, minute, and second, respectively. The month and day
default to `01` if not specified. The hour, minute, and second default to `00`
if not specified.

<code><SyntaxText raw="<fraction>" /></code>, if specified, is the fractional
seconds part that can have from 1 to 6 decimal digits (`0`-`9`), representing
microsecond precision. If fewer than 6 digits are provided, the value is
padded on the right with zeros to reach microsecond precision.

<code><SyntaxText raw="<zone>" /></code> represents the time zone and should
have one of the forms:

- `Z` (Zulu time zone UTC+0)
- <code><SyntaxText raw="('+'|'-')<H>[<H>]':'<M>[<M>]" /></code>
- A string with one of the prefixes `UTC+`, `UTC-`, `GMT+`, `GMT-`, `UT+` or `UT-`, and a suffix in the following forms:
  - <code><SyntaxText raw="('+'|'-')<H>[<H>]" /></code>
  - <code><SyntaxText raw="('+'|'-')<H><H>[':']<M><M>" /></code>
  - <code><SyntaxText raw="('+'|'-')<H><H>':'<M><M>':'<S><S>" /></code>
  - <code><SyntaxText raw="('+'|'-')<H><H><M><M><S><S>" /></code>
- A time zone name in the form <code><SyntaxText raw="<area>'/'<city>" /></code>, such as `Europe/Paris`

The time zone defaults to the session local timezone (set via `spark.sql.session.timeZone`) if not specified.

## Timestamp Examples

```sql
SELECT TIMESTAMP '2000-02-29 23:59:59.123' AS col;
-- +-----------------------+
-- |                    col|
-- +-----------------------+
-- |2000-02-29 23:59:59.123|
-- +-----------------------+

SELECT TIMESTAMP '2015-06-30 12:00:00.999999UTC-05:00' AS col;
-- +--------------------------+
-- |                      col |
-- +--------------------------+
-- |2015-06-30 17:00:00.999999|
-- +--------------------------+

SELECT TIMESTAMP '2010-08' AS col;
-- +-------------------+
-- |                col|
-- +-------------------+
-- |2010-08-01 00:00:00|
-- +-------------------+
```

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
