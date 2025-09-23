---
title: Binary Literals
rank: 2
---

# Binary Literals

A binary literal is used to specify a byte sequence value.

## Syntax

<SyntaxBlock>
  <SyntaxText raw="'X'(''''<hexdigit>*''''|'&quot;'<hexdigit>*'&quot;')" />
</SyntaxBlock>

<code><SyntaxText raw="<hexdigit>" /></code> is a case-insensitive hexadecimal
number (`0`-`9`, `a`-`f`, or `A`-`F`).

## Examples

```sql
SELECT X'123456' AS col;
-- +----------+
-- |       col|
-- +----------+
-- |[12 34 56]|
-- +----------+
```

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
