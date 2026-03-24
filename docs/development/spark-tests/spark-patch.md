---
title: Working with the Spark Patch
rank: 90
---

# Working with the Spark Patch

Occasionally, you may need to patch the Spark source code further.
Here are the commands that can be helpful for this purpose.

```bash
# Apply the patch.
# You can now modify the Spark source code.
git -C opt/spark apply ../../scripts/spark-tests/spark-4.1.1.patch

# Update the Spark patch file with your local modification.
git -C opt/spark add .
git -C opt/spark diff --staged -p > scripts/spark-tests/spark-4.1.1.patch

# Revert the patch.
git -C opt/spark reset
git -C opt/spark apply -R ../../scripts/spark-tests/spark-4.1.1.patch
```

However, note that we should keep the patch minimal.
It is possible to alter many Spark test behaviors at runtime via monkey-patching using pytest fixtures.
