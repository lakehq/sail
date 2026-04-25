---
name: opsx:continue
description: Advances through artifact creation step-by-step. Reads completed artifact dependencies, generates the next artifact based on schema ordering, and updates progress tracking.
user-invocable: true
disable-model-invocation: false
---

# /opsx:continue - Create Next Artifact

Progress through artifact creation one at a time. Creates the next artifact in sequence based on workflow schema and dependency ordering.

## What This Does

- **Reads completed artifacts**: Understands prior context and decisions
- **Respects dependencies**: Creates artifacts in proper order
- **Generates next artifact**: Populates template based on previous work
- **Maintains context**: Carries forward decisions from earlier artifacts
- **Updates status**: Tracks progress as you go

## How to Use

Continue to the next artifact:
```
/opsx:continue
```

Optionally specify which change:
```
/opsx:continue user-authentication
```

The workflow will:
1. Find your current change (or ask which one)
2. Read all completed artifacts for context
3. Determine what comes next in the workflow
4. Generate the next artifact template
5. Guide you through creating it

## Artifact Sequence

### Step-by-Step Workflow

**Proposal** → **Spec** → **Design** → **Tasks** → Implementation

1. **Proposal**: Problem and approach
2. **Spec**: Detailed requirements from Proposal context
3. **Design**: Architecture based on Spec requirements
4. **Tasks**: Implementation steps derived from Design

### Respecting Dependencies

Each artifact uses prior artifacts:

```
PROPOSAL.md (why?)
    ↓ provides context for...
SPEC.md (what exactly?)
    ↓ provides requirements for...
DESIGN.md (how to build it?)
    ↓ provides structure for...
TASKS.md (step-by-step implementation)
```

When you `/opsx:continue`:
- **From Proposal to Spec**: Codex reads your proposal and helps write detailed specifications
- **From Spec to Design**: Codex understands requirements and suggests architecture
- **From Design to Tasks**: Codex breaks design into concrete implementation tasks

## Example Workflow

### Session 1: Create Proposal
```
/opsx:new
> email-verification
> step-by-step

[Creates PROPOSAL.md]
[You fill in problem statement, goals, approach]
```

### Session 2: Create Specs
```
/opsx:continue
> email-verification

[Codex reads PROPOSAL.md]
[Generates SPEC.md template]
[You add requirements and Given/When/Then scenarios]
```

### Session 3: Create Design
```
/opsx:continue
> email-verification

[Codex reads PROPOSAL.md and SPEC.md]
[Generates DESIGN.md template]
[You describe architecture and components]
```

### Session 4: Create Tasks
```
/opsx:continue
> email-verification

[Codex reads all prior artifacts]
[Generates TASKS.md with implementation checklist]
[You refine and estimate tasks]
```

## Context Inheritance

Each artifact automatically includes context:

**SPEC inherits from PROPOSAL:**
- Goals and success criteria
- Problem statement
- Any constraints mentioned

**DESIGN inherits from SPEC:**
- All requirements
- Edge cases
- Performance expectations

**TASKS inherits from all prior:**
- Everything that came before
- Forms the implementation checklist
- Guides actual coding

## Tips for Effective Progression

1. **Complete one artifact before continuing**: Don't skip steps
2. **Review prior artifacts**: Codex will summarize them for you
3. **Make changes**: If you disagree with inherited context, edit prior artifacts
4. **Ask questions**: Use the thinking mode to understand dependencies
5. **Iterate**: You can return and edit earlier artifacts

## Skipping Artifacts

If you want to skip steps:
- Use `/opsx:ff` to create all remaining artifacts at once
- Or manually create the artifact file (not recommended for strict workflow)

## Progress Tracking

Status automatically updates:
```
# Change: email-verification

Status:
- ✓ PROPOSAL.md - COMPLETED
- ✓ SPEC.md - COMPLETED
- ⏳ DESIGN.md - IN PROGRESS
- ○ TASKS.md - PENDING
```

## Editing Prior Artifacts

You can edit earlier artifacts and continue will adapt:

```
[Edit SPEC.md to add new requirement]
/opsx:continue

[Codex reads updated SPEC.md]
[Generates updated DESIGN.md with new considerations]
```

## Related Commands

- `/opsx:new` - Create a new change
- `/opsx:ff` - Skip to creating all remaining artifacts
- `/opsx:apply` - Start implementation (once TASKS.md is complete)
- `/opsx:verify` - Validate implementation against artifacts
