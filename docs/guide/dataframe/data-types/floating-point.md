---
title: Floating Point Semantics
rank: 4
---

# Floating Point Semantics

## Special Values

Floating point types in Spark have a few special values that can be cast from string literals.
For example, `double('NaN')` produces a **DoubleType** NaN value,
and `'inf'::float` produces a **FloatType** positive infinity value.

- **Positive Infinity**
  - String literals: `'Inf'`, `'+Inf'`, `'Infinity'`, or `'+Infinity'` (case-insensitive)
- **Negative Infinity**
  - String literals: `'Inf'` or `'Infinity'` (case-insensitive)
- **NaN (Not a Number)**
  - String literal: `'NaN'` (case-insensitive)

The special values are stored as their corresponding IEEE-754 representations in Arrow.

## Infinity Semantics

- Positive infinity multiplied by any positive value returns positive infinity.
- Negative infinity multiplied by any positive value returns negative infinity.
- Positive infinity multiplied by any negative value returns negative infinity.
- Negative infinity multiplied by any negative value returns positive infinity.
- Positive/negative infinity multiplied by 0 returns NaN.
- Positive/negative infinity is equal to itself.
- Positive infinity values are grouped together in aggregations.
- Negative infinity values are grouped together in aggregations.
- Positive infinity and negative infinity are treated as normal values in join keys.
- Positive infinity are considered less than NaN and greater than any other values.
- Negative infinity are considered less than any other values.

## NaN Semantics

The handling of NaN for **FloatType** or **DoubleType** differs from standard IEEE-754 semantics in specific ways:

- NaN values are considered equal to each other.
- NaN values are grouped together in aggregations.
- NaN values are treated as normal values in join keys.
- NaN values are considered greater than any other values.

## Examples

```sql
SELECT double('infinity') AS col;
-- +--------+
-- |     col|
-- +--------+
-- |Infinity|
-- +--------+

SELECT float('-inf') AS col;
-- +---------+
-- |      col|
-- +---------+
-- |-Infinity|
-- +---------+

SELECT float('NaN') AS col;
-- +---+
-- |col|
-- +---+
-- |NaN|
-- +---+

SELECT double('infinity') * 0 AS col;
-- +---+
-- |col|
-- +---+
-- |NaN|
-- +---+

SELECT double('-infinity') * (-1234567) AS col;
-- +--------+
-- |     col|
-- +--------+
-- |Infinity|
-- +--------+

SELECT double('infinity') < double('NaN') AS col;
-- +----+
-- | col|
-- +----+
-- |true|
-- +----+

SELECT double('NaN') = double('NaN') AS col;
-- +----+
-- | col|
-- +----+
-- |true|
-- +----+

SELECT double('inf') = double('infinity') AS col;
-- +----+
-- | col|
-- +----+
-- |true|
-- +----+

SELECT COUNT(*), c2
FROM VALUES
    (1, double('infinity')),
    (2, double('infinity')),
    (3, double('inf')),
    (4, double('-inf')),
    (5, double('NaN')),
    (6, double('NaN')),
    (7, double('-infinity')) AS t(c1, c2)
GROUP BY c2
ORDER BY c2;
-- +---------+---------+
-- | count(1)|       c2|
-- +---------+---------+
-- |        2|-Infinity|
-- |        3| Infinity|
-- |        2|      NaN|
-- +---------+---------+
```
