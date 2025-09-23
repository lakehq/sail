---
title: Null Literals
rank: 3
---

# Null Literals

A null literal represents the SQL null value.

## Syntax

<SyntaxBlock>
  <SyntaxText raw="'NULL'" />
</SyntaxBlock>

The null literal is case-insensitive.

## Example

```sql
SELECT NULL AS col;
-- +----+
-- | col|
-- +----+
-- |NULL|
-- +----+
```

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
