---
title: Query Planning
rank: 2
---

# Query Planning

<SvgDiagram :svg="data['planning.dot']" />

Query planning in Sail involves multiple stages of syntactic and semantic analysis.

Sail accepts SQL strings as well as Spark relations defined in the Spark Connect protocol.
It defines an unresolved specification (the "Sail spec") capable of representing both SQL and Spark relations.

The SQL string is parsed into an abstract syntax tree (AST) and then converted into the Sail spec.
Sail implements its own in-house SQL parser to support all Spark SQL features.
The Spark relation is converted into the Sail spec directly.

Sail uses [Apache DataFusion](https://datafusion.apache.org/) as the underlying query engine. DataFusion provides abstractions and implementations for logical plans, physical plans, catalogs, and file formats. Sail combines these capabilities with its own extensions.

The Sail spec is analyzed and resolved into a DataFusion logical plan using contextual information from the catalog and function registry.
The logical plan is optimized and converted into a DataFusion physical plan, which undergoes another round of optimization.
Sail implements various custom logical and physical plan extension nodes to support Spark features such as PySpark UDFs.

In local mode, the optimized physical plan is executed directly.
In cluster mode, the optimized physical plan is further split into stages at data shuffle boundaries. The tasks, each representing one partition within a stage, are sent to the workers for execution.

<script setup lang="ts">
import SvgDiagram from "@theme/components/SvgDiagram.vue";
import { data } from "./index.data.ts";
</script>
