---
name: opsx:new
description: Initiates structured change creation. Scaffolds a change directory, selects workflow schema, and generates the first artifact template for step-by-step artifact progression.
user-invocable: true
disable-model-invocation: false
---

# /opsx:new - Create New Change

Initiate a new structured change by creating a change directory and scaffolding the first artifact following a chosen workflow schema.

## What This Does

- **Creates change directory**: Organized container for all change artifacts
- **Selects workflow schema**: Choose step-by-step or fast-forward approach
- **Generates first artifact**: Proposal, specs, design, or tasks (depending on workflow)
- **Tracks progress**: Maintains completion status across artifacts
- **Sets up structure**: Organized naming and artifact dependencies

## How to Use

Start a new change:
```
/opsx:new
```

The workflow will:
1. Ask for a change name (e.g., "user-authentication", "payment-refunds")
2. Show available workflow schemas
3. Create the change directory
4. Generate and guide creation of the first artifact

## Change Directory Structure

```
changes/
└── user-authentication-2024-02-13/
    ├── PROPOSAL.md
    ├── SPEC.md
    ├── DESIGN.md
    ├── TASKS.md
    └── status.md
```

## Workflow Schemas

### Schema 1: Step-by-Step (Recommended)
Create artifacts in sequence: Proposal → Specs → Design → Tasks → Implementation

Best for:
- Complex features
- New team members
- Features requiring discussion/review
- Building consensus

### Schema 2: Fast-Forward (FF)
Create all artifacts at once in one session

Best for:
- Simple changes
- Well-understood requirements
- Experienced teams
- Quick iterations

## Artifact Types

Each artifact serves a specific purpose:

### 1. **PROPOSAL.md**
- What are we building and why?
- Problem statement and goals
- Success criteria
- High-level approach

### 2. **SPEC.md**
- Detailed requirements
- Given/When/Then scenarios (behavior)
- Edge cases and error handling
- Acceptance criteria

### 3. **DESIGN.md**
- Architecture and approach
- Code structure and components
- Data models
- Implementation strategy

### 4. **TASKS.md**
- Checklist of implementation steps
- Task dependencies
- Estimation and complexity
- Acceptance criteria per task

## Example: Create a Change

```
/opsx:new

[Change name?]
> user-password-reset

[Which workflow?]
1. Step-by-step (create artifacts one by one)
2. Fast-forward (create all artifacts now)
> 1

[Creating change: user-password-reset-2024-02-13...]
[Directory created. Let's start with PROPOSAL.md]
[What is the problem we're solving?]
```

## Status Tracking

The `status.md` file tracks:
- ✓ Completed artifacts
- ⏳ In-progress artifacts
- ○ Pending artifacts
- Links to related changes

Example:
```
# Change: user-password-reset

Status:
- ✓ PROPOSAL.md - COMPLETED
- ⏳ SPEC.md - IN PROGRESS
- ○ DESIGN.md - PENDING
- ○ TASKS.md - PENDING
```

## Next Steps

After creating the change:
- **Step-by-step workflow**: Use `/opsx:continue` to create the next artifact
- **Fast-forward workflow**: All artifacts are created; use `/opsx:apply` to implement
- **Review changes**: Use `/opsx:verify` to validate completeness

## Tips

1. **Name clearly**: Use kebab-case and be descriptive (e.g., "email-verification", "api-rate-limiting")
2. **Choose workflow**: Step-by-step for learning/collaboration, FF for simple changes
3. **Save frequently**: Each artifact is independently saved
4. **Cross-reference**: Link related changes in your proposals
5. **Use templates**: Leverage the scaffolded templates as starting points

## Common Patterns

### New Feature
Start with Proposal → Specs → Design → Tasks → Implement

### Bug Fix
Proposal (what's broken) → Specs (expected behavior) → Design → Tasks → Implement

### Refactoring
Proposal (why refactor) → Design (new structure) → Tasks → Implement

## Related Commands

- `/opsx:continue` - Create next artifact step-by-step
- `/opsx:ff` - Fast-forward (create all artifacts)
- `/opsx:apply` - Implement the change
- `/opsx:verify` - Validate against artifacts
