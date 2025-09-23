---
title: Boolean Literals
rank: 4
---

# Boolean Literals

A boolean literal specifies a boolean truth value.

## Syntax

<SyntaxBlock>
  <SyntaxText raw="'TRUE'|'FALSE'" />
</SyntaxBlock>

The boolean literals are case-insensitive.

## Examples

```sql
SELECT TRUE AS col;
-- +----+
-- | col|
-- +----+
-- |true|
-- +----+

SELECT FALSE AS col;
-- +-----+
-- |  col|
-- +-----+
-- |false|
-- +-----+
```

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
