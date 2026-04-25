---
name: opsx:onboard
description: Interactive tutorial walking users through a complete spec-driven development workflow cycle. Covers exploration, proposal creation, specs, design, tasks, implementation, verification, and archival using real codebase tasks.
user-invocable: true
disable-model-invocation: false
---

# /opsx:onboard - Interactive Workflow Tutorial

Complete interactive tutorial walking through the entire spec-driven development workflow. Learn by doing with a real change from your codebase.

## What This Does

- **Guided tutorial**: Step-by-step introduction to OPSX workflow
- **Real tasks**: Works with an actual change from your codebase
- **11-phase journey**: Complete cycle from exploration to archive
- **Feedback at each step**: Learn best practices as you go
- **Estimated 15 minutes**: Quick onboarding for new team members
- **Learn by doing**: Hands-on experience with real workflow

## Who Should Use This

- ✅ New team members joining project
- ✅ Teams new to spec-driven development
- ✅ Learning the OPSX workflow
- ✅ Understanding artifact-driven development
- ✅ First-time with OpenSpec

## How to Start

Begin the tutorial:
```
/opsx:onboard
```

The tutorial will guide you through all 11 phases with a real change.

## The 11 Phases

### Phase 1: Welcome & Overview (1 min)
- What is spec-driven development?
- What is OpenSpec OPSX?
- What you'll learn today
- Estimated timeline

### Phase 2: Understand the Workflow (2 min)
- 8 phases of development
- When to use each command
- How artifacts work
- Why this approach

### Phase 3: Explore the Codebase (2 min)
- Use `/opsx:explore` demonstration
- Explore an aspect of your codebase
- Practice asking good questions
- Understand system architecture

### Phase 4: Pick a Change (1 min)
- Select a real change to work on
- Small feature or bug fix recommended
- First-time friendly examples
- Tutorial will guide you through it

### Phase 5: Create Proposal (2 min)
- Use `/opsx:new` to start change
- Write PROPOSAL.md for your change
- Define problem, approach, success criteria
- Guided template with examples

### Phase 6: Create Specifications (2 min)
- Use `/opsx:continue` to progress
- Write detailed requirements in SPEC.md
- Define scenarios using Given/When/Then
- Real examples from your change

### Phase 7: Design Solution (2 min)
- Continue to DESIGN.md
- Describe architecture and components
- Show how you'll build it
- Real design decisions

### Phase 8: Break Down into Tasks (1 min)
- Generate TASKS.md
- See implementation checklist
- Understand task dependencies
- Ready to code

### Phase 9: Implement (2 min)
- Use `/opsx:apply` to start coding
- Work through 2-3 tasks
- See how artifact context guides coding
- Code with confidence

### Phase 10: Verify Quality (1 min)
- Use `/opsx:verify` to validate
- Check 3 dimensions: Completeness, Correctness, Coherence
- See how specs guide verification
- Confidence in work quality

### Phase 11: Archive & Celebrate (1 min)
- Use `/opsx:archive` to finalize
- See how artifacts are preserved
- Understand archive as knowledge base
- Celebrate completed work!

## Tutorial Flow

```
/opsx:onboard

┌─────────────────────────────────────────┐
│  Welcome to OpenSpec OPSX Onboarding!   │
│                                         │
│  You'll learn 8 commands in 11 phases   │
│  Estimated time: 15 minutes             │
│  Real change from your codebase         │
└─────────────────────────────────────────┘

[Phase 1] Welcome & Overview
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OpenSpec is a spec-driven development methodology...
[Explained in 1 minute]
Ready? (yes/help)
> yes

[Phase 2] Understand the Workflow
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The 8 OPSX commands...
[Shown visually with flow diagram]
Questions? (yes/no/continue)
> continue

[Phase 3] Explore the Codebase
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Let's explore together using /opsx:explore
What aspect of the code interests you?
[You explore, Codex guides]

... continues through all 11 phases ...

[Phase 11] Archive & Celebrate!
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Perfect! You've completed a full workflow cycle.

You learned:
✓ Exploration (/opsx:explore)
✓ Creating changes (/opsx:new)
✓ Step-by-step development (/opsx:continue)
✓ Implementation (/opsx:apply)
✓ Verification (/opsx:verify)
✓ Archival (/opsx:archive)
✓ Plus bonus: /opsx:ff, /opsx:sync, /opsx:bulk-archive

Next Steps:
1. Start your own change with /opsx:new
2. Reference this tutorial anytime
3. Explore advanced features

Great work! 🎉
```

## Example Change for Tutorial

If you don't have a change in mind, tutorial suggests examples:

```
Great changes for learning:
1. Add a new API endpoint
2. Fix a simple bug
3. Add a feature flag
4. Improve error messages
5. Add logging to a service

Which sounds good? (1-5 or enter your own)
```

## At Each Phase

You get:
- **Explanation**: What to do and why
- **Examples**: See best practices
- **Hands-on**: Actually do the step
- **Feedback**: Learn from your work
- **Tips**: Professional practices

## Learning Outcomes

After completing onboarding, you'll know:

1. **What is SDD**: Understand spec-driven development philosophy
2. **8 OPSX commands**: When and how to use each
3. **Artifacts**: Purpose and content of each file
4. **Workflow**: Complete cycle from idea to archive
5. **Best practices**: How to work effectively
6. **When to use what**: Decision making guide
7. **Real example**: Concrete change from your codebase

## Tutorial Resources

Within the tutorial:
- Visual workflow diagrams
- Code examples
- File templates
- Decision trees
- Quick reference guide

## Tips for the Tutorial

1. **Go at your own pace**: Take breaks if needed
2. **Ask questions**: Tutorial encourages exploration
3. **Use real examples**: Pick a real change if possible
4. **Try the commands**: Actually run each skill
5. **Review artifacts**: Read what you created
6. **Take notes**: Write down key insights
7. **Return anytime**: Tutorial is always available

## After Tutorial

Once complete:

### You're Ready to:
- Start your own changes with `/opsx:new`
- Guide your team on OPSX workflow
- Understand spec-driven development
- Use all 8 OPSX commands
- Create quality artifacts

### Next Resources:
- `/opsx:explore` - Explore new areas
- `/opsx:new` - Start a real change
- Team documentation - Project-specific guidance
- OpenSpec official docs - https://openspec.dev/

## Common Questions During Tutorial

**Q: Is this methodology strict?**
A: No! Guidelines are flexible. Adapt to your team.

**Q: Do I need all 8 commands?**
A: Start with explore → new → ff → apply → verify → archive

**Q: Can I skip phases?**
A: Yes, but onboarding covers full cycle.

**Q: How long is this really?**
A: 15 min average, 20-30 min if exploring deeply.

**Q: Will this slow me down?**
A: Initial ramp-up, but faster long-term with better clarity.

## Returning to Tutorial

You can always come back:

```
/opsx:onboard --resume
[Pick up where you left off]

/opsx:onboard --phase 7
[Jump to a specific phase]

/opsx:onboard --quick
[5-minute version for review]
```

## Share with Your Team

```
/opsx:onboard --generate-doc
[Creates markdown guide for team]

/opsx:onboard --share
[Generate shareable materials]
```

## Certification (Optional)

Complete the tutorial to get:
```
/opsx:onboard --certificate
[Print certificate of completion]
```

## Getting Help

During tutorial:
```
(help) - Get help
(explain) - Re-explain current phase
(back) - Go back a phase
(skip) - Skip to next phase
(quit) - Exit tutorial
```

## Contact

Questions about the workflow?
- Join the OpenSpec community
- Visit https://openspec.dev/
- Review documentation

Ready to start?

```
/opsx:onboard
```

Let's learn spec-driven development! 🚀
