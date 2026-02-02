#!/bin/bash
#
# Patch Ibis to fix TRY_CAST generation for Spark dialect.
#
# Issue: Ibis 11.0.0 generates CAST instead of TRY_CAST because the
# sqlglot TryCast object is created without safe=True flag.
#
# Fix upstream: https://github.com/ibis-project/ibis/commit/fc494985a39d88be9c9e35b342243face78dbfed
#
# This patch can be removed once Ibis releases a version >= 11.1.0 that includes the fix.
#

set -euo pipefail

# Find the ibis base.py file in the current Python environment
IBIS_BASE_PY=$(python -c "import ibis.backends.sql.compilers.base as b; print(b.__file__)")

if [[ ! -f "$IBIS_BASE_PY" ]]; then
    echo "Error: Could not find ibis base.py at $IBIS_BASE_PY"
    exit 1
fi

echo "Patching Ibis TRY_CAST fix in: $IBIS_BASE_PY"

# Check if already patched
if grep -q "sge.TryCast.*safe=True" "$IBIS_BASE_PY"; then
    echo "Ibis is already patched, skipping."
    exit 0
fi

# Apply the patch: add safe=True to the TryCast call
# Before: return sge.TryCast(this=arg, to=self.type_mapper.from_ibis(to))
# After:  return sge.TryCast(this=arg, to=self.type_mapper.from_ibis(to), safe=True)
sed -i.bak 's/return sge\.TryCast(this=arg, to=self\.type_mapper\.from_ibis(to))/return sge.TryCast(this=arg, to=self.type_mapper.from_ibis(to), safe=True)/' "$IBIS_BASE_PY"

# Verify the patch was applied
if grep -q "sge.TryCast.*safe=True" "$IBIS_BASE_PY"; then
    echo "Patch applied successfully."
    rm -f "${IBIS_BASE_PY}.bak"
else
    echo "Error: Patch failed to apply."
    # Restore backup
    mv "${IBIS_BASE_PY}.bak" "$IBIS_BASE_PY"
    exit 1
fi
