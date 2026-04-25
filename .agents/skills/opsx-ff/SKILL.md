---
name: opsx:ff
description: Fast-forward artifact creation. Generates all remaining artifacts (proposal, specs, design, tasks) in one batch operation, respecting dependency ordering.
user-invocable: true
disable-model-invocation: false
---

# /opsx:ff - Fast-Forward

Generate all remaining artifacts in one batch operation. Skip the step-by-step process and get all planning artifacts created at once.

## What This Does

- **Batch artifact creation**: Creates Proposal → Specs → Design → Tasks all at once
- **Respects dependencies**: Maintains proper ordering and context flow
- **Intelligent generation**: Later artifacts reference earlier ones appropriately
- **Single session**: Complete planning in one go
- **Ready to implement**: All artifacts ready for `/opsx:apply`

## How to Use

Fast-forward a change:
```
/opsx:ff
```

Or specify a change:
```
/opsx:ff email-verification
```

You can also fast-forward from the middle:
```
/opsx:ff
[Currently at SPEC.md]
[Creates DESIGN.md and TASKS.md in batch]
```

## When to Use FF

### ✅ Good Fit for FF
- Simple, well-understood features
- Experienced development teams
- Features with clear requirements
- Fixes with obvious solutions
- Tight timelines
- Iterating on existing patterns

### ❌ Use Step-by-Step Instead
- Complex new features
- Uncertain requirements
- Need for team discussion/approval
- First time with this feature area
- Cross-team coordination needed
- Training new developers

## FF Workflow

```
You create a PROPOSAL.md:
"Add dark mode toggle to settings"

/opsx:ff

↓ Codex creates SPEC.md (from Proposal)
- Requirements derived from proposal goals
- Scenarios covering dark mode behavior
- Edge cases (save preference, sync across devices)

↓ Codex creates DESIGN.md (from Proposal + Spec)
- Component architecture for theme switching
- Data model for user preferences
- CSS structure and styling approach

↓ Codex creates TASKS.md (from all prior artifacts)
- Create theme toggle component
- Implement theme state management
- Add CSS variable definitions
- Update all components to use theme
- Test across browsers
- Deploy theme assets
```

## Generated Artifact Quality

FF artifacts are:
- **Complete**: All sections filled with relevant content
- **Contextual**: Later artifacts reference earlier ones
- **Implementable**: Ready to code from TASKS.md immediately
- **Coherent**: Design and tasks align with specs
- **Testable**: Specs include Given/When/Then scenarios

## Typical FF Timeline

Starting from PROPOSAL.md:
- **FF generation**: 2-5 minutes
- **Review artifacts**: 5-10 minutes
- **Adjust as needed**: 5-15 minutes
- **Ready to implement**: 15-30 minutes total

## Example: FF User Registration

### Start with Proposal
```markdown
# User Registration

## Problem
New users should be able to sign up with email and password.

## Approach
- Simple email/password form
- Password strength validation
- Email verification flow
- Store in users table

## Success Criteria
- Users can register in under 2 minutes
- Secure password storage
- Email confirmation required
```

### Run FF
```
/opsx:ff
```

### Get All Artifacts

**SPEC.md** includes:
- Registration flow steps
- Password requirements
- Email validation rules
- Error scenarios

**DESIGN.md** includes:
- Form components
- Auth service implementation
- Database schema
- API endpoints needed

**TASKS.md** includes:
- Build registration form
- Implement password validation
- Create email verification service
- Set up database schema
- Write tests
- Deployment steps

## Editing FF Artifacts

You can edit artifacts after FF creates them:

```
[Read generated SPEC.md]
[Modify password requirements]
/opsx:ff  (or /opsx:continue)
[Later artifacts auto-update with changes]
```

## Limitations

- FF works best when the Proposal is clear
- Vague proposals lead to generic artifacts
- Complex features may need refinement after FF
- Always review before implementing

## Tips for Successful FF

1. **Clear Proposal**: Spend time on a good PROPOSAL.md first
2. **Specific details**: Include requirements, constraints, success criteria
3. **Review immediately**: Read all generated artifacts right after
4. **Ask for revisions**: Request edits if something doesn't fit
5. **Iterate**: Update artifacts as you implement

## Advanced: FF from Middle

If you've already created some artifacts:

```
PROPOSAL.md ✓
SPEC.md ✓
DESIGN.md ○
TASKS.md ○

/opsx:ff
[Creates DESIGN.md and TASKS.md, using context from PROPOSAL + SPEC]
```

## Related Commands

- `/opsx:new` - Create a new change (start with Proposal)
- `/opsx:continue` - Step-by-step instead of batch
- `/opsx:apply` - Implement once artifacts are ready
- `/opsx:verify` - Validate implementation
