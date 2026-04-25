---
name: opsx:apply
description: Implement tasks from a completed change. Reads context artifacts, works through checklist items in sequence, and marks completion as code changes are made.
user-invocable: true
disable-model-invocation: false
---

# /opsx:apply - Implement Change

Implement a change by working through tasks in sequence. Uses PROPOSAL, SPEC, and DESIGN as context while executing items from TASKS.md.

## What This Does

- **Reads all context**: Understands proposal, specs, and design
- **Works through tasks**: Implements TASKS.md items one by one
- **Guided implementation**: Codex guides you through each step
- **Maintains checklist**: Tracks which tasks are complete
- **References artifacts**: Links back to specs and design for guidance
- **Writes code**: Actually creates or modifies files

## How to Use

Apply a change:
```
/opsx:apply
```

Or specify which change:
```
/opsx:apply email-verification
```

Resume an in-progress implementation:
```
/opsx:apply email-verification --resume
```

## Implementation Workflow

```
1. /opsx:apply email-verification

2. Codex reads:
   - PROPOSAL.md (why?)
   - SPEC.md (what exactly?)
   - DESIGN.md (how to build it?)
   - TASKS.md (step-by-step)

3. Codex asks:
   "Ready to start with Task 1: Create email service?"

4. You work together:
   - Codex explains the task
   - You write/review the code
   - Mark complete when done

5. Move to Task 2, 3, etc.

6. All tasks complete → change ready
```

## Task Execution

### Each Task Includes

- **Name**: What needs to be done
- **Description**: Why and what we're building
- **Acceptance Criteria**: How to know it's done
- **Related Spec Sections**: Which requirements this implements
- **Related Design Sections**: Architecture this follows

### During Each Task

1. **Understanding**: Codex explains the task
2. **Planning**: Discuss approach together
3. **Implementation**: Write the code
4. **Verification**: Check acceptance criteria
5. **Mark complete**: Move to next task

## Example: Implementing Email Verification

```
/opsx:apply email-verification

[Codex reads all artifacts]

Task 1 of 6: Create Email Service
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Spec Reference:
- Requirement: "Send verification email"
- Scenario: "Email sent with verification link"

Design Reference:
- Component: EmailService
- Method: sendVerificationEmail(email, token)

Task Description:
Create an email service that sends verification emails.
Should handle templates, error cases, and logging.

Acceptance Criteria:
✓ Service accepts email and token
✓ Renders HTML email template
✓ Sends via configured mail provider
✓ Logs success/failure
✓ Throws on misconfiguration

Ready? (yes/next/explain)
> yes

[You implement EmailService]
[Show the code when ready]

✓ Task 1 Complete
→ Task 2: Create Database Schema
```

## Task Ordering

Tasks are ordered by:
- **Dependencies**: Earlier tasks are prerequisites
- **Logical flow**: Build foundation before features
- **Testing**: Unit tests before integration
- **Deployment**: Code before configuration

Example order:
1. Database schema (foundation)
2. Model/entity classes (data layer)
3. API endpoints (business logic)
4. Authentication (security)
5. Error handling (robustness)
6. Tests (validation)
7. Documentation (communication)

## Resuming Implementation

If you stop mid-way:

```
/opsx:apply email-verification --resume

[Codex reads all artifacts and current progress]

Completed Tasks:
✓ Task 1: Create Email Service
✓ Task 2: Create Database Schema

Current Task:
⏳ Task 3: Build Verification Controller

Where you left off:
[Shows last few lines of code/changes]

Ready to continue? (yes/explain/review)
> yes
```

## Handling Challenges

If you get stuck:

```
Ready? (yes/next/explain/help)
> help

[Codex provides additional explanation]
[Shows code examples]
[Discusses design rationale]
```

If task is unclear:

```
> explain

[Codex re-explains the task]
[Shows similar examples from codebase]
[Clarifies acceptance criteria]
```

If you disagree with task:

```
> This task seems wrong

[Discuss the task]
[Update TASKS.md if needed]
[Continue or go back to design]
```

## Tracking Progress

Status file tracks your progress:

```
Task Status:
✓ Task 1: Create Email Service - COMPLETE
✓ Task 2: Create Database Schema - COMPLETE
⏳ Task 3: Build Verification Controller - IN PROGRESS
○ Task 4: Write Integration Tests - PENDING
○ Task 5: Update API Documentation - PENDING
○ Task 6: Deploy Verification System - PENDING

Completion: 2/6 (33%)
```

## Code Quality

While implementing:
- **Follow specs exactly**: Don't improvise beyond SPEC.md
- **Respect design**: Stick to DESIGN.md architecture
- **Write tests**: For each task as requested
- **Handle errors**: All edge cases from spec
- **Comment code**: Link to spec sections

## When You're Done

After all tasks complete:

```
✓ All 6 tasks complete!

Next steps:
1. Run tests: npm test
2. Review changes: git diff
3. Verify: /opsx:verify
4. Archive: /opsx:archive
```

## Tips for Successful Implementation

1. **Read the artifacts first**: Understand proposal, specs, design
2. **Take one task at a time**: Don't skip ahead
3. **Ask for clarification**: If task is unclear
4. **Reference specs**: When unsure about behavior
5. **Test as you go**: Don't wait until the end
6. **Keep commits atomic**: One commit per task
7. **Update docs**: As you implement

## Related Commands

- `/opsx:new` - Create a new change first
- `/opsx:continue` / `/opsx:ff` - Create artifacts before applying
- `/opsx:verify` - Validate implementation against artifacts
- `/opsx:archive` - Finalize when complete
