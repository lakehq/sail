---
title: String Literals
rank: 1
---

# String Literals

A string literal specifies a character string value.

## Syntax

<SyntaxBlock>
  <SyntaxText raw="['r'](''''<char>*''''|'&quot;'<char>*'&quot;')" />
</SyntaxBlock>

<code><SyntaxText raw="<char>" /></code> is a character drawn from the
supported character set.

- Use `\` to escape special characters.
- Use <code><SyntaxText raw="'\u'<XXXX>" /></code> or <code><SyntaxText raw="'\U'<xxxxxxxx>" /></code> to represent Unicode characters, where <code><SyntaxText raw="<XXXX>" /></code> and <code><SyntaxText raw="<xxxxxxxx>" /></code> are 16-bit and 32-bit code
  points in hexadecimal respectively. For example, `\u03C0` → `π`, `\U0001F44B` → `👋`.
- Use an octal number preceded by `\` to represent an ASCII character. For example, `\142` → `b`.

`r` (case-insensitive) is an optional prefix that indicates a raw string literal.
If a string literal starts with the `r` prefix, neither special characters nor Unicode characters are escaped by `\`.

The following escape sequences are recognized in regular string literals without the `r` prefix, and replaced according
to the following rules:

- `\0`->`\u0000` (null)
- `\b`->`\u0008` (backspace)
- `\n`->`\u000a` (line feed)
- `\r`->`\u000d` (carriage return)
- `\t`->`\u0009` (horizontal tab)
- `\Z`->`\u001A` (substitute)
- `\%`->`\%` (percent sign)
- `\_`->`\_` (underscore)

For <code><SyntaxText raw="'\'<c>" /></code> where <code><SyntaxText raw="<c>" /></code> is any other character,
the backslash `\` is simply removed and the character is left as is.

## Examples

<!-- FIXME: broken syntax highlighting -->

```
SELECT 'Hello, World!' AS col;
-- +-------------+
-- |          col|
-- +-------------+
-- |Hello, World!|
-- +-------------+

SELECT "Sail" AS col;
-- +----+
-- | col|
-- +----+
-- |Sail|
-- +----+

SELECT 'It\'s about $25.' AS col;
-- +---------------+
-- |            col|
-- +---------------+
-- |It's about $25.|
-- +---------------+

SELECT r"'\n' represents a newline character." AS col;
-- +------------------------------------+
-- |                                 col|
-- +------------------------------------+
-- |'\n' represents a newline character.|
-- +------------------------------------+
```

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
