---
name: opsx:sync
description: Intelligently merge delta spec changes into main specifications. Adds scenarios without duplication, modifies selectively rather than replacing wholesale, and resolves conflicts.
user-invocable: true
disable-model-invocation: false
---

# /opsx:sync - Sync Delta Specs

Merge spec changes from a change back into main specifications. Uses intelligent merging to avoid duplication and preserve existing specs.

## What This Does

- **Reads delta specs**: Changes made during implementation (in DELTA_SPEC.md)
- **Merges intelligently**: Adds new scenarios without duplicating existing ones
- **Modifies selectively**: Updates only changed requirements, preserves the rest
- **Resolves conflicts**: If spec was modified elsewhere, helps resolve conflicts
- **Maintains history**: Keeps track of what changed and when
- **Main specs updated**: Changes are integrated back to main spec.md

## When to Sync

### Scenarios Where You Need to Sync

1. **Implementation revealed gaps**: Found edge cases during coding
2. **Spec was incomplete**: Realized requirements after starting
3. **Behavior changed**: Implementation differs from original spec
4. **New requirements emerged**: Discovered during testing

### You DON'T Need to Sync

- Spec was perfect from the start (rare!)
- No changes were discovered during implementation
- Changes were already in original spec
- Only code changed, specs stayed the same

## How to Use

Sync a change's specs:
```
/opsx:sync
```

Or specify which change:
```
/opsx:sync email-verification
```

Review before syncing:
```
/opsx:sync email-verification --review
```

## Delta Spec Format

During implementation, you track changes in `DELTA_SPEC.md`:

```markdown
# Email Verification - Delta Spec

## New Requirements Added

### Requirement: Rate Limiting
Prevent email verification link from being requested too frequently.

#### Scenario: Rate Limit Exceeded
- GIVEN user has requested verification email 5 times in 10 minutes
- WHEN user requests another verification email
- THEN request is denied with "Too many requests" error
- AND user must wait 10 minutes before trying again

## Modified Requirements

### Requirement: Email Validation (MODIFIED)
- OLD: "Email format must be valid"
- NEW: "Email format must be valid AND domain must not be disposable"
- Reason: Prevent registration with throwaway email services

New Scenario:
#### Scenario: Disposable Email Rejected
- GIVEN a registration with email "user@temp-mail.com"
- WHEN the system checks the email domain
- THEN registration is rejected with "Disposable emails not allowed"
```

## The Sync Process

```
1. /opsx:sync email-verification

2. Codex checks:
   - Does DELTA_SPEC.md exist?
   - What changed during implementation?
   - Conflicts with main spec.md?

3. Codex merges:
   NEW scenarios → Add to main spec
   MODIFIED scenarios → Update in place
   REMOVED → Mark as deprecated

4. Codex shows:
   - What will be merged
   - Any conflicts found
   - Resulting main spec.md

5. You confirm or adjust
```

## Merge Strategies

### Adding New Requirements

```
DELTA_SPEC.md has:
- Rate Limiting requirement
- 3 new scenarios

Merge result:
- Adds full "Rate Limiting" requirement to main spec
- All 3 scenarios included
- No duplication (checks for existing similar scenarios)
```

### Modifying Existing Requirements

```
DELTA_SPEC.md modified:
- Email Validation: added "disposable domain check"

Merge result:
- Original Email Validation requirement updated
- New scenario added
- Old scenarios preserved
- Modification reason documented
```

### Removing Scenarios

```
DELTA_SPEC.md marked:
- "Scenario: Very long email addresses" - REMOVED

Merge result:
- Scenario marked as deprecated
- Reason documented
- Not deleted (for history)
```

## Conflict Resolution

If main spec was modified elsewhere:

```
CONFLICT DETECTED:
Spec "Email Validation" was changed in:
- your DELTA_SPEC.md (added disposable check)
- main spec.md (was updated separately)

Conflicting changes:
- DELTA: Add disposable domain validation
- MAIN: Changed max email length requirement

Actions:
1. Review both changes
2. Decide: keep delta, keep main, or merge both
3. Codex helps resolve
```

## Reviewing Before Sync

Use --review to see what will change:

```
/opsx:sync email-verification --review

DELTA CHANGES TO MERGE
═════════════════════

ADDITIONS (will be added to main spec):
+ Rate Limiting (Requirement)
  + Scenario: Rate Limit Exceeded
  + Scenario: Burst Request Protection

MODIFICATIONS (will update main spec):
~ Email Validation (Requirement)
  + Scenario: Disposable Email Rejected (new)
  ~ Scenario: Valid Email Accepted (clarified)

DEPRECATIONS (marked as old):
- Scenario: Email Format Validation (replaced by more specific scenarios)

Proceed with sync? (yes/no/resolve-conflicts)
```

## Examples

### Example 1: Syncing New Scenarios

```
During implementation, you discovered:
"We need to handle expired verification tokens"

Added to DELTA_SPEC.md:
Scenario: Expired Token Rejected
- GIVEN a verification token created 48 hours ago
- WHEN user clicks the verification link
- THEN verification fails with "Link expired"

/opsx:sync email-verification

Result:
- New scenario added to main spec
- Marked as discovered during implementation
- Linked to the change that added it
```

### Example 2: Syncing Behavior Changes

```
Original spec said:
"Password must be 8+ characters"

During implementation, discovered:
"8 characters insufficient for security, need 12+"

Added to DELTA_SPEC.md:
MODIFIED: Password Requirements
- OLD: 8+ characters
- NEW: 12+ characters and complexity rules
- Reason: Security audit recommended strengthening

/opsx:sync email-verification

Result:
- Main spec updated to 12+ characters
- Old requirement marked as superseded
- Change reason documented
```

## After Syncing

Once synced:
1. **Main spec is updated**: spec.md reflects implementation reality
2. **History is preserved**: DELTA_SPEC.md kept for reference
3. **Changes are documented**: Why things changed and when
4. **Team is informed**: Everyone sees spec evolved

## Tips for Effective Syncing

1. **Keep DELTA_SPEC current**: Update as you discover changes
2. **Document reasons**: Explain why spec changed
3. **Review before syncing**: See what will change first
4. **Resolve conflicts**: Address conflicts immediately
5. **Communicate changes**: Tell team about spec evolution
6. **Update tasks if needed**: TASKS.md may need updating too

## Before Archiving

Sync before archiving to keep specs current:

```
/opsx:apply email-verification
[Complete implementation]

/opsx:sync email-verification
[Merge any spec changes discovered]

/opsx:verify email-verification
[Verify against updated specs]

/opsx:archive email-verification
[Archive with current specs]
```

## Related Commands

- `/opsx:apply` - Implement (may discover spec changes)
- `/opsx:verify` - Validate after syncing
- `/opsx:archive` - Usually sync before archiving
