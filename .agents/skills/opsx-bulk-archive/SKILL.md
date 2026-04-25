---
name: opsx:bulk-archive
description: Archive multiple changes simultaneously while detecting and resolving spec conflicts through codebase inspection.
user-invocable: true
disable-model-invocation: false
---

# /opsx:bulk-archive - Archive Multiple Changes

Archive multiple completed changes in one operation. Automatically detects spec conflicts and helps resolve them.

## What This Does

- **Archives in batch**: Process multiple changes at once
- **Detects conflicts**: Finds conflicting spec changes automatically
- **Resolves intelligently**: Uses codebase inspection to determine truth
- **Preserves history**: Each change archived with metadata
- **Syncs all specs**: Merges all delta specs to main
- **Reports results**: Summary of what was archived

## When to Use Bulk Archive

### Good Fit for Bulk Archive
- Multiple related changes completed (e.g., payment system features)
- Team finishing sprint or milestone
- Multiple changes with interdependent specs
- Want to resolve all conflicts at once

### Use Single Archive Instead
- Just one or two changes
- Changes are independent
- No spec conflicts expected
- Want individual control

## How to Use

Archive multiple changes:
```
/opsx:bulk-archive
```

Specify which changes:
```
/opsx:bulk-archive email-verification payment-refunds api-rate-limiting
```

Preview before archiving:
```
/opsx:bulk-archive --preview
```

## Bulk Archive Process

```
1. /opsx:bulk-archive

2. Codex finds changes in active/
   Found: 3 changes
   - email-verification
   - payment-refunds
   - api-rate-limiting

3. Codex checks each:
   ✓ email-verification: All tasks complete
   ✓ payment-refunds: All tasks complete
   ✓ api-rate-limiting: All tasks complete

4. Codex detects conflicts:
   CONFLICT: Both email-verification and
   payment-refunds modify "Error Handling" requirement

5. Codex resolves:
   Inspects codebase to determine intent
   Proposes resolution
   You approve or adjust

6. Codex syncs specs:
   Merges all delta specs to main
   Combines changes intelligently
   Resolves conflicts

7. Codex archives:
   Moves all changes to archive/
   Records metadata for each
   Creates archive summary

8. Results:
   3 changes archived
   1 conflict resolved
   Main spec updated
```

## Conflict Detection

When multiple changes modify the same spec:

```
CONFLICT DETECTED
═════════════════

Spec: "Error Handling"

Change 1: email-verification
- Modified: Added "Verification timeout" scenario

Change 2: payment-refunds
- Modified: Added "Refund failed" scenario

Conflicting Section:
- Both add to same requirement
- No direct conflict in logic
- Proposed merge: Combine both scenarios

Codebase Inspection:
- Email service uses: verifyEmailWithTimeout()
- Payment service uses: processRefundWithRetry()
- No actual code conflict

Resolution:
- Merge both scenarios into single requirement
- Both changes are correct and compatible
- Result: Richer error handling spec

Approve merge? (yes/no/manual)
```

## Conflict Resolution Strategies

### Strategy 1: Combine (Default)
When changes enhance different parts:
```
Change A: Adds scenario for success case
Change B: Adds scenario for timeout case
→ Combine both scenarios in final spec
```

### Strategy 2: Choose Winner
When changes are mutually exclusive:
```
Change A: Password must be 8+ chars
Change B: Password must be 12+ chars
→ Inspect code to see which is implemented
→ Use the one actually in code
```

### Strategy 3: Manual Resolution
When conflict needs discussion:
```
Change A: Pagination limit 10 per page
Change B: Pagination limit 25 per page
→ You decide which to use
→ Codex documents your choice
```

## Preview Mode

Check what will be archived before doing it:

```
/opsx:bulk-archive --preview

BULK ARCHIVE PREVIEW
════════════════════

Changes to Archive: 3

1. email-verification (3 days)
   Tasks: 6/6 ✓
   Specs Modified: 1
   Conflicts: 0

2. payment-refunds (2 days)
   Tasks: 4/4 ✓
   Specs Modified: 2
   Conflicts: 1 (with email-verification)

3. api-rate-limiting (1 day)
   Tasks: 3/3 ✓
   Specs Modified: 1
   Conflicts: 0

TOTAL CONFLICTS: 1
Resolution: email-verification + payment-refunds
Strategy: Combine scenarios

Proceed? (yes/no)
```

## Result Report

After archiving:

```
BULK ARCHIVE COMPLETE
═════════════════════

Changes Archived: 3
- email-verification-2024-02-13/
- payment-refunds-2024-02-13/
- api-rate-limiting-2024-02-13/

Specs Synced: 4 requirements modified
Conflicts Resolved: 1

Final Main Spec:
- 45 requirements (was 42)
- 180 scenarios (was 175)
- New features documented

Time Saved: ~20 minutes vs. archiving individually

Next Steps:
1. Review main spec changes (git diff spec.md)
2. Merge code changes to develop/main
3. Deploy when ready
```

## Archive Organization After Bulk

```
archive/
├── 2024-02-13/
│   ├── email-verification-2024-02-13/
│   ├── payment-refunds-2024-02-13/
│   └── api-rate-limiting-2024-02-13/
├── 2024-02-12/
│   └── user-authentication-2024-02-12/
└── ...
```

## Bulk Archive Metadata

Includes cross-change information:

```
BULK_ARCHIVE_INFO.md:
- Dates: When each change was completed
- Dependencies: Which changes were related
- Conflicts Resolved: How conflicts were handled
- Changes to Main Spec: Summary of all modifications
- Related Commits: All commits for all changes
```

## Tips for Bulk Archiving

1. **Verify each first**: Ensure all changes pass `/opsx:verify`
2. **Review conflicts**: Understand how conflicts will be resolved
3. **Preview before**: Use `--preview` to see what happens
4. **Archive together**: Group related changes (same feature area)
5. **Communicate**: Tell team about major spec changes
6. **Test after**: Verify main spec still makes sense

## Common Scenarios

### Scenario 1: Feature Release
```
Multiple related changes for a feature:
- api-endpoints
- database-migration
- cache-strategy
- error-handling

/opsx:bulk-archive api-endpoints database-migration cache-strategy error-handling
[Resolves all interdependencies at once]
```

### Scenario 2: Sprint Completion
```
Team finished sprint with 5 changes:
- feature-1
- feature-2
- bug-fix-1
- optimization
- documentation

/opsx:bulk-archive
[Archives all at once]
[Merges all spec changes]
[Creates sprint summary]
```

### Scenario 3: Parallel Work
```
Team worked on same feature area:
- email-auth (team A)
- email-notifications (team B)
- email-templates (team C)

/opsx:bulk-archive email-auth email-notifications email-templates
[Combines all email-related work]
[Resolves overlapping specs]
[Single coherent email spec]
```

## After Bulk Archive

1. **All changes archived**: History preserved
2. **Specs consolidated**: Main spec comprehensive
3. **Code ready**: All implementations complete
4. **Team aligned**: Common spec understanding
5. **Next phase**: Deploy or start new features

## Tips for Minimizing Conflicts

1. **Communicate spec changes**: Tell team during implementation
2. **Keep specs updated**: Sync early and often
3. **Coordinate modifications**: Discuss overlapping changes
4. **Review main spec regularly**: Stay in sync

## Related Commands

- `/opsx:archive` - Archive single change
- `/opsx:verify` - Verify before bulk archiving
- `/opsx:sync` - Sync specs (done automatically)
