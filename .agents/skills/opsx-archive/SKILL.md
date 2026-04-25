---
name: opsx:archive
description: Finalize and archive completed changes. Checks artifact/task completion, optionally syncs delta specs to main specs, then moves change directory to archive with timestamp.
user-invocable: true
disable-model-invocation: false
---

# /opsx:archive - Archive Completed Change

Finalize a completed change by archiving it. Validates completeness, optionally syncs specs, and moves the change to an archive directory.

## What This Does

- **Verifies completion**: Checks all tasks are done
- **Syncs specs**: Optionally merges delta specs to main
- **Creates archive**: Moves change to timestamped archive
- **Cleans up**: Removes from active changes
- **Documents closure**: Records when completed
- **Maintains history**: Keeps for reference and learning

## When to Archive

Archive when:
- ✅ All tasks are complete
- ✅ Code is implemented and tested
- ✅ Verification passed (all 3 dimensions)
- ✅ Code review is done
- ✅ Specs are synced (if needed)
- ✅ Ready to merge/deploy

Don't archive if:
- ❌ Tasks still pending
- ❌ Tests failing
- ❌ Code not reviewed
- ❌ Verification issues remain
- ❌ Specs need syncing

## How to Use

Archive a change:
```
/opsx:archive
```

Or specify which change:
```
/opsx:archive email-verification
```

Archive without syncing specs:
```
/opsx:archive email-verification --no-sync
```

Archive with a note:
```
/opsx:archive email-verification --note "Ready for production deployment"
```

## Archive Process

```
1. /opsx:archive email-verification

2. Codex checks:
   ✓ All tasks complete?
   ✓ Code implemented?
   ✓ Tests passing?
   ✓ Verification passed?

3. Codex offers to sync:
   "Sync spec changes to main? (yes/no)"

4. Codex archives:
   moves changes/ → archive/email-verification-2024-02-13/

5. Documented:
   Archive entry created
   Completion date recorded
   Notes added (if any)
```

## Pre-Archive Checklist

Before archiving, ensure:

- [ ] All tasks in TASKS.md completed
- [ ] Code written and tested
- [ ] Tests all passing
- [ ] Verification passed
- [ ] Code reviewed
- [ ] Specs synced (if delta changes exist)
- [ ] Documentation updated
- [ ] Commit message written
- [ ] Ready to merge/deploy

## Archive Directory Structure

```
Before archiving:
changes/
└── email-verification-2024-02-13/
    ├── PROPOSAL.md
    ├── SPEC.md
    ├── DESIGN.md
    ├── TASKS.md
    └── status.md

After archiving:
archive/
└── email-verification-2024-02-13/
    ├── PROPOSAL.md
    ├── SPEC.md
    ├── DESIGN.md
    ├── TASKS.md
    ├── status.md
    └── ARCHIVE_INFO.md (metadata)
```

## Archive Metadata

Each archived change includes `ARCHIVE_INFO.md`:

```markdown
# Archive Information

Change: email-verification
Archived: 2024-02-13 14:30:00
Duration: 3 days, 4 hours

Commits:
- aec3f2a feat: email service implementation
- b2d4e5f feat: email verification endpoint
- c3e5f6g test: email verification tests

Specs Synced:
- Added: Rate Limiting requirement (3 scenarios)
- Modified: Email Validation (1 scenario updated)

Artifacts:
- PROPOSAL.md: 8 KB, 120 lines
- SPEC.md: 15 KB, 240 lines
- DESIGN.md: 12 KB, 180 lines
- TASKS.md: 10 KB, 150 lines

Final Status: ✓ COMPLETE
Verification: ✓ ALL PASS
Code Review: ✓ APPROVED

Notes:
Ready for production deployment on Feb 15.
```

## Examples

### Example 1: Archive Successful Feature

```
/opsx:apply email-verification
[Complete all 6 tasks]

/opsx:verify email-verification
[All dimensions verified ✓]

/opsx:archive email-verification

Archive email-verification? (yes/no)
> yes

Sync specs? (yes/no)
> yes

[Syncing delta specs...]
[Creating archive directory...]
[Recording metadata...]

✓ Change archived: archive/email-verification-2024-02-13/

Status:
- Moved from changes/ to archive/
- All artifacts preserved
- Specs synced to main
- Metadata recorded
```

### Example 2: Archive with Issues Resolved

```
/opsx:verify email-verification
[Found 2 issues]

[Fix issues in implementation]

/opsx:verify email-verification
[All verified ✓]

/opsx:archive email-verification --note "Issues from initial verification fixed and re-verified"

[Change archived with completion note]
```

## After Archiving

Once archived:

1. **Code is ready**: All implementation complete
2. **Specs are current**: Main spec updated with discoveries
3. **History preserved**: Can review archived change anytime
4. **Team informed**: Change completed and documented
5. **Next steps**: Merge, deploy, or start new change

## Accessing Archived Changes

View archived change:
```
archive/email-verification-2024-02-13/ARCHIVE_INFO.md
[See what was completed]

archive/email-verification-2024-02-13/SPEC.md
[Review final specifications]

archive/email-verification-2024-02-13/TASKS.md
[See all completed tasks]
```

## Archive Organization

Archive is organized by date:

```
archive/
├── email-verification-2024-02-13/
├── payment-refunds-2024-02-12/
├── user-roles-2024-02-11/
├── api-rate-limiting-2024-02-10/
└── ...
```

You can also organize by feature:

```
archive/
├── user-management/
│   ├── email-verification-2024-02-13/
│   ├── password-reset-2024-02-12/
│   └── user-roles-2024-02-11/
├── payments/
│   ├── payment-refunds-2024-02-12/
│   └── payment-webhooks-2024-02-10/
└── ...
```

## Tips for Successful Archiving

1. **Verify first**: Run `/opsx:verify` before archiving
2. **Sync specs**: Keep specs current with `/opsx:sync`
3. **Write good notes**: Explain the change for future reference
4. **Keep commits clean**: Each task should be a focused commit
5. **Document decisions**: In DESIGN.md why you chose certain approaches
6. **Archive regularly**: Don't let changes pile up
7. **Review archives**: Learn from completed changes

## Learning from Archives

Archive is a knowledge base:

```
Looking to understand email verification?
→ archive/email-verification-2024-02-13/SPEC.md

How did we design the payment system?
→ archive/payment-system-2024-01-20/DESIGN.md

What were the original payment requirements?
→ archive/payment-system-2024-01-20/PROPOSAL.md
```

## Common Mistakes to Avoid

❌ **Archiving incomplete work**
→ Verify all tasks complete first

❌ **Forgetting to sync specs**
→ Use `/opsx:sync` before archiving

❌ **Archiving without review**
→ Have code reviewed first

❌ **Not documenting discoveries**
→ Update DELTA_SPEC.md during implementation

❌ **Losing context**
→ Keep detailed PROPOSAL.md and DESIGN.md

## Related Commands

- `/opsx:verify` - Verify before archiving
- `/opsx:sync` - Sync specs before archiving
- `/opsx:bulk-archive` - Archive multiple changes at once
