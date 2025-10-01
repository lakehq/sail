---
title: Identifiers
rank: 3
---

# Identifiers

A SQL identifier is a name used to identify a database object, such as a catalog, database, table, view, function, or column.

Identifiers are case-insensitive.

## Regular Identifiers

<SyntaxBlock>
  <SyntaxText raw="(<letter>|<digit>|'_')+" />
</SyntaxBlock>

<code><SyntaxText raw="<letter>" /></code> is an ASCII letter (`a`-`z` or
`A`-`Z`). <code><SyntaxText raw="<digit>" /></code> is an ASCII digit
(`0`-`9`).

Note that a regular identifier can start with a digit.

## Delimited Identifiers

<SyntaxBlock>
  <SyntaxText raw="'`'<char>+'`'" />
</SyntaxBlock>

<code><SyntaxText raw="<char>" /></code> is a character drawn from the
supported character set. The backtick character <code>&#96;</code> can be
escaped by repeating it twice (i.e. <code>&#96;&#96;</code>).

Delimited identifiers are required when the identifier contains special characters. For example, `SELECT * FROM a.b` represents a query that selects all columns from table `b` in database `a`, while <code>SELECT \* FROM &#96;a.b&#96;</code> represents a query that selects all columns from a table named `a.b`.

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
