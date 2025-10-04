---
title: Operators
rank: 5
---

# Operators

Sail supports all Spark SQL operators in SQL expressions.

## Operator Precedence

The precedence of an operator determines the order in which expressions are evaluated.
For example, in the expression `3 + 4 * 5`, the multiplication operator (`*`) has a higher precedence than the addition operator (`+`), so the multiplication is performed first, resulting in `3 + (4 * 5) = 23`.

Operators that have higher precedence are evaluated before operators with lower precedence. Operators with the same precedence are evaluated based on their associativity.

The following table lists the precedence and associativity of all the SQL operators in Sail.
The operators are listed in order of decreasing precedence, where 1 is the highest precedence.

<table tabindex="0">
  <thead>
    <tr>
      <th>Precedence</th>
      <th>Operator</th>
      <th>Operation</th>
      <th>Associativity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td><code>.</code><br /><code>[]</code><br /><code>::</code></td>
      <td>Member access<br />Element access<br />Cast</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>2</td>
      <td><code>+</code><br /><code>-</code><br /><code>~</code></td>
      <td>Unary plus<br />Unary minus<br />Bitwise not</td>
      <td>Right to left</td>
    </tr>
    <tr>
      <td>3</td>
      <td>
        <code>*</code><br /><code>/</code><br /><code>%</code><br /><code
          >DIV</code
        >
      </td>
      <td>Multiplication<br />Division<br />Modulo<br />Integral division</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>4</td>
      <td><code>+</code><br /><code>-</code><br /><code>||</code></td>
      <td>Addition<br />Subtraction<br />Concatenation</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>5</td>
      <td>
        <code>&lt;&lt;</code><br /><code>&gt;&gt;</code><br /><code
          >&gt;&gt;&gt;</code
        >
      </td>
      <td>
        Bitwise shift left<br />Bitwise shift right<br />Bitwise shift right
        unsigned
      </td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>6</td>
      <td><code>&amp;</code></td>
      <td>Bitwise and</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>7</td>
      <td><code>^</code></td>
      <td>Bitwise xor (exclusive or)</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>8</td>
      <td><code>|</code></td>
      <td>Bitwise or (inclusive or)</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>9</td>
      <td>
        <code>=</code>, <code>==</code><br /><code>&lt;&gt;</code>,
        <code>!=</code><br /><code>&lt;</code><br /><code>&lt;=</code
        ><br /><code>&gt;</code><br /><code>&gt;=</code>
      </td>
      <td>Comparison operators</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>10</td>
      <td>
        <code>BETWEEN</code><br /><code>IN</code><br /><code>RLIKE</code>,
        <code>REGEXP</code><br /><code>ILIKE</code><br /><code>LIKE</code
        ><br /><code><SyntaxText raw="'IS '['NOT ']'NULL'" /></code><br />
        <code><SyntaxText raw="'IS '['NOT ']'TRUE'" /></code><br /><code
          ><SyntaxText raw="'IS '['NOT ']'FALSE'" /></code
        ><br /><code><SyntaxText raw="'IS '['NOT ']'DISTINCT FROM'" /></code>
      </td>
      <td>Other predicates</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>11</td>
      <td><code>NOT</code>, <code>!</code><br /><code>EXISTS</code></td>
      <td>Logical not<br />Existence</td>
      <td>Right to left</td>
    </tr>
    <tr>
      <td>12</td>
      <td><code>AND</code></td>
      <td>Logical and (conjunction)</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>13</td>
      <td><code>OR</code></td>
      <td>Logical or (disjunction)</td>
      <td>Left to right</td>
    </tr>
  </tbody>
</table>

::: info

In the original Spark documentation, the logical not operator (`NOT` or `!`) has a
higher precedence than predicates such as `LIKE`,
but this does not seem to be consistent with the actual implementation.

In both the Sail documentation here and the Sail implementation, the logical not operator has a lower precedence than other predicates.
This provides a more intuitive behavior, where `NOT a LIKE b` is interpreted as `NOT (a LIKE b)` instead of `(NOT a) LIKE b`.

:::

<script setup>
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
