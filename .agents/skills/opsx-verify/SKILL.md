---
name: opsx:verify
description: Validate implementation against artifacts across three dimensions - Completeness (requirements met), Correctness (spec adherence), and Coherence (design consistency).
user-invocable: true
disable-model-invocation: false
---

# /opsx:verify - Validate Implementation

Verify that your implementation matches the proposal, specs, design, and tasks. Checks three dimensions: Completeness, Correctness, and Coherence.

## What This Does

- **Completeness check**: Did you implement all requirements?
- **Correctness check**: Does implementation match the spec?
- **Coherence check**: Does design stay consistent throughout?
- **Reports findings**: Clear list of what's verified vs. missing
- **Suggests fixes**: Identifies gaps and how to fix them

## The Three Verification Dimensions

### 1. Completeness ✓
**Question**: Did we build everything that was required?

Checks:
- All PROPOSAL goals met
- All SPEC requirements implemented
- All TASKS completed
- Nothing critical was skipped

### 2. Correctness ✓
**Question**: Does the implementation match the specification?

Checks:
- Given/When/Then scenarios pass
- Edge cases are handled
- Error messages match spec
- Behavior matches spec exactly

### 3. Coherence ✓
**Question**: Is the implementation consistent with the design?

Checks:
- Code follows DESIGN architecture
- Component relationships match design
- Data flows as designed
- No architectural violations

## How to Use

Verify a change:
```
/opsx:verify
```

Or specify which change:
```
/opsx:verify email-verification
```

Get a detailed report:
```
/opsx:verify email-verification --detailed
```

## Verification Process

```
1. /opsx:verify email-verification

2. Codex reads:
   - PROPOSAL.md
   - SPEC.md
   - DESIGN.md
   - TASKS.md
   - Your actual code

3. Codex checks each dimension:

   COMPLETENESS
   ✓ Goal 1: Users can register
   ✓ Goal 2: Secure password storage
   ✓ Requirement 1: Email verification
   ✓ Requirement 2: Password strength
   ✓ All tasks completed

   CORRECTNESS
   ✓ Scenario 1: Successful registration
   ✗ Scenario 2: Invalid email format
   ✓ Error handling for duplicate email

   COHERENCE
   ✓ Service architecture matches design
   ✓ Data flow follows design
   ✗ Email component not following design pattern

4. Codex provides:
   - Summary of verification
   - List of issues found
   - Recommendations for fixes
```

## Verification Report

A typical report includes:

```
VERIFICATION REPORT: email-verification
═══════════════════════════════════════

COMPLETENESS: ✓ PASS (9/9)
─────────────────────────
All proposal goals met
All spec requirements implemented
All tasks completed
No critical gaps

CORRECTNESS: ⚠ PARTIAL (6/8)
──────────────────────────
✓ Scenario: Successful registration
✗ ISSUE: Invalid email format not validated
  Spec says: "Email must be valid format"
  Code: Email validation missing in controller

✓ Scenario: Duplicate email error
✗ ISSUE: Error message doesn't match spec
  Spec says: "Email already in use"
  Code shows: "Duplicate email"

COHERENCE: ✓ PASS (7/7)
───────────────────────
✓ Service architecture matches design
✓ Database schema matches design
✓ Component relationships correct

SUMMARY
───────
Status: NEEDS FIXES (2 issues found)
Severity: LOW (non-breaking issues)
Estimate: 30 minutes to fix

Recommended Actions:
1. Add email format validation in registration controller
2. Update error messages to match spec exactly
3. Re-verify after fixes
```

## Issue Categories

### Critical Issues
- Missing major feature
- Spec violation affecting behavior
- Design not followed
- Security issue
- Performance problem

### Major Issues
- Missing edge case handling
- Incomplete implementation
- Design deviation
- Missing test coverage

### Minor Issues
- Error message mismatch
- Missing documentation
- Code style inconsistency
- Performance optimization

## Handling Issues

If verification finds issues:

```
/opsx:verify email-verification

[Issues found]

Fix issues:
1. Read the issue description
2. Locate the code
3. Make the fix
4. Test the fix
5. Re-verify

/opsx:verify email-verification
[All verified ✓]
```

## Re-verification

After making fixes:

```
Updated: email-verification/src/controller.ts
Updated: email-verification/tests/registration.test.ts

/opsx:verify email-verification

[Codex re-checks all dimensions]
[Issues fixed, all passing]
```

## What Gets Verified

### Against PROPOSAL
- Goals and success criteria
- Problem statement addressed
- Intended audience needs met

### Against SPEC
- All requirements implemented
- All scenarios working
- Edge cases handled
- Error cases covered
- Performance specs met
- Data integrity maintained

### Against DESIGN
- Architecture followed
- Component structure correct
- Data models match
- Integration points correct
- Code organization matches design

### Against TASKS
- All tasks marked complete
- Task dependencies satisfied
- Acceptance criteria met

## Before Archive

Always verify before archiving:

```
/opsx:apply email-verification
[Finish all tasks]

/opsx:verify email-verification
[Confirm all dimensions pass]

/opsx:archive email-verification
[Archive with confidence]
```

## Continuous Verification

You can verify at any time:
- After each task completes
- Before committing
- Before code review
- Before deployment
- After design changes

```
Completed Task 2
/opsx:verify --quick
[Quick check while continuing]

Finished implementation
/opsx:verify --detailed
[Full verification before archive]
```

## Tips for Passing Verification

1. **Reference specs**: Check your code against Given/When/Then scenarios
2. **Follow design**: Build exactly what DESIGN.md describes
3. **Complete tasks**: Don't skip TASKS.md items
4. **Test thoroughly**: Write tests that validate scenarios
5. **Handle errors**: Implement all error cases from spec
6. **Verify early**: Check progress while still implementing
7. **Review artifacts**: Make sure artifacts are current

## Related Commands

- `/opsx:apply` - Implement the change
- `/opsx:sync` - Sync any spec changes made during implementation
- `/opsx:archive` - Finalize once verified
