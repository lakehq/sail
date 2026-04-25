---
name: opsx:explore
description: Thinking partner mode for investigating problems and architecture. Allows exploratory reading and analysis without writing code. Emphasizes visual diagrams, deep questions, and adaptive investigation.
user-invocable: true
disable-model-invocation: false
---

# /opsx:explore - Exploration Mode

Enter a thinking partner mode for investigating problems, architecture, and codebase without making changes. Perfect for understanding existing code before proposing changes.

## What This Does

- **Non-prescriptive exploration**: Investigate problems deeply before committing to solutions
- **Architecture understanding**: Read files, understand structure, ask clarifying questions
- **Visual analysis**: Create ASCII diagrams and visual representations
- **No code changes**: Purely investigative mode
- **Adaptive questioning**: Follow up based on what you learn

## How to Use

Start exploration mode:
```
/opsx:explore
```

The workflow will:
1. Ask what you want to explore
2. Read relevant files and understand the architecture
3. Ask clarifying questions about requirements
4. Create visual diagrams to explain structure
5. Summarize findings and suggest next steps

## When to Use

- ✅ Understanding new codebases
- ✅ Investigating bugs or issues
- ✅ Analyzing architectural decisions
- ✅ Planning larger changes
- ✅ Learning system behavior

## Exploration Output

You'll get:
- **Architecture diagrams** showing component relationships
- **File structure overview** of relevant code
- **Questions and insights** that help clarify requirements
- **Suggestions** for the next step (usually `/opsx:new`)

## Example Explorations

### Explore a Bug
```
/opsx:explore

[What should we explore?]
> We're getting timeout errors in the payment processing flow
```

Explores: payment service, error handling, timeout configs, related code paths

### Explore Architecture
```
/opsx:explore

[What should we explore?]
> How does the authentication system work?
```

Explores: auth services, user models, session handling, security checks

### Explore Before Refactoring
```
/opsx:explore

[What should we explore?]
> We're thinking about refactoring the database layer
```

Explores: current DB abstraction, usage patterns, dependencies, performance implications

## Tips

1. **Be specific**: "explore the payment system" is better than "explore the code"
2. **Ask questions**: Respond to Codex's questions to guide the exploration
3. **Review findings**: Look at the visual diagrams and summaries
4. **Plan next steps**: Once you understand the issue, move to `/opsx:new` to create a change

## Next Steps

After exploration, typically you'll:
- Use `/opsx:new` to create a change proposal
- Use `/opsx:apply` if the issue is clear-cut
- Use `/opsx:continue` if you're working through a larger change

## Working Together

This mode is collaborative:
- Ask Codex to explore specific files or patterns
- Request diagrams or visualizations
- Have Codex answer your questions about the codebase
- Iteratively understand the system together
