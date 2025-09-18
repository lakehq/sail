---
title: Numeric Literals
rank: 5
---

# Numeric Literals

Numeric literals represent integers, fixed-point numbers, or floating-point numbers.

## Integral Number Syntax

<SyntaxBlock>
  <SyntaxText raw="['+'|'-']<digit>+['L'|'S'|'Y']" />
</SyntaxBlock>

<code><SyntaxText raw="<digit>" /></code> is any decimal digit (`0`-`9`).

The data type of the integral number is determined by the optional postfix:

- `L` (case-insensitive) indicates `BIGINT` or `LONG`, an 8-byte signed integer.
- `S` (case-insensitive) indicates `SMALLINT` or `SHORT`, a 2-byte signed integer.
- `Y` (case-insensitive) indicates `TINYINT` or `BYTE`, a 1-byte signed integer.
- If there is no postfix, the number is interpreted as an `INT` or `INTEGER`, a 4-byte signed integer.

## Integral Number Examples

```sql
SELECT -2147483600 AS col;
-- +-----------+
-- |        col|
-- +-----------+
-- |-2147483600|
-- +-----------+

SELECT 9223372036854775807L AS col;
-- +-------------------+
-- |                col|
-- +-------------------+
-- |9223372036854775807|
-- +-------------------+

SELECT -64Y AS col;
-- +----+
-- | col|
-- +----+
-- |-64 |
-- +----+

SELECT 512S AS col;
-- +----+
-- |col |
-- +----+
-- |512 |
-- +----+
```

## Fractional Number Syntax

A decimal literals can be written in the following forms.

<SyntaxBlock>
  <SyntaxText raw="<decimal>" />
  <SyntaxText raw="<decimal>[<exponent>]'BD'" />
  <SyntaxText raw="<whole>[<exponent>]'BD'" />
</SyntaxBlock>

The `BD` postfix is required if the number is a whole number or has an exponent. Otherwise, the number would be interpreted as an integer or a double.

A double literal can be written in the following forms.
The `D` postfix is optional when there is an exponent.

<SyntaxBlock>
  <SyntaxText raw="(<decimal>|<whole>)'D'" />
  <SyntaxText raw="(<decimal>|<whole>)<exponent>['D']" />
</SyntaxBlock>

A float literal can be written in the following forms.
The `F` postfix is required in all forms.

<SyntaxBlock>
  <SyntaxText raw="(<decimal>|<whole>)'F'" />
  <SyntaxText raw="(<decimal>|<whole>)<exponent>'F'" />
</SyntaxBlock>

<code><SyntaxText raw="<decimal>" /></code> is a number that contains a
decimal point.

<SyntaxBlock>
  <SyntaxText raw="['+'|'-']<digit>+'.'" />
  <SyntaxText raw="['+'|'-']<digit>+'.'<digit>+" />
  <SyntaxText raw="['+'|'-']'.'<digit>+" />
</SyntaxBlock>

<code><SyntaxText raw="<whole>" /></code> is a whole number without a decimal
point.

<SyntaxBlock>
  <SyntaxText raw="['+'|'-']<digit>+" />
</SyntaxBlock>

<code><SyntaxText raw="<exponent>" /></code> is the exponent in scientific
notation.

<SyntaxBlock>
  <SyntaxText raw="('E'|'e')['+'|'-']<digit>+" />
</SyntaxBlock>

<code><SyntaxText raw="<digit>" /></code> is any decimal digit (`0`-`9`).

In summary, fractional numbers are represented by decimal numbers or whole numbers, with optional exponent the following postfixes to indicate the data type:

- `D` (case-insensitive) indicates `DOUBLE`, an 8-byte double-precision floating point number.
- `F` (case-insensitive) indicates `FLOAT`, a 4-byte single-precision floating point number.
- `BD` (case-insensitive) indicates `DECIMAL`, with the total number of digits as precision and the number of digits to right of
  decimal point as scale.

## Fractional Number Examples

```sql
SELECT 76.543 AS col, TYPEOF(76.543) AS type;
-- +-------+------------+
-- |    col|        type|
-- +-------+------------+
-- | 76.543|decimal(5,3)|
-- +-------+------------+

SELECT 8.21E1 AS col, TYPEOF(8.21E1) AS type;
-- +------+------+
-- |   col|  type|
-- +------+------+
-- | 82.1 |double|
-- +------+------+

SELECT -0.4321 AS col;
-- +--------+
-- |     col|
-- +--------+
-- |-0.4321 |
-- +--------+

SELECT 250.BD AS col;
-- +-----+
-- | col |
-- +-----+
-- |250  |
-- +-----+

SELECT 6.9D AS col;
-- +-----+
-- | col |
-- +-----+
-- | 6.9 |
-- +-----+

SELECT -18BD AS col;
-- +----+
-- |col |
-- +----+
-- |-18 |
-- +----+

SELECT .789E3F AS col;
-- +------+
-- | col  |
-- +------+
-- |789.0 |
-- +------+
```

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
