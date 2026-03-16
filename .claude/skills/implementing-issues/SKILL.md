---
name: implementing-issues
description: Implements a GitHub issue end-to-end. Reads the issue, creates a branch and PR, breaks work into tasks saved to plans/, executes tasks in parallel via sub-agents, and commits after each task. Use when the user says /implement, asks to work on an issue, or references an issue number to start implementing.
---

# Implementing Issues

## Workflow

Copy this checklist and track progress:

```
Implementation Progress:
- [ ] Step 1: Read issue and understand scope
- [ ] Step 2: Create branch and draft PR
- [ ] Step 3: Break down into tasks
- [ ] Step 4: Execute tasks (parallel where possible)
- [ ] Step 5: Verify and finalize
```

## Step 1: Read Issue

```bash
gh issue view <number> --json title,body,labels,milestone
```

Extract from the issue body:
- **Scenarios** — the acceptance criteria
- **Test Plan** — test function names and files
- **Dependencies** — blocked by / blocks

If the issue is blocked by open issues, stop and inform the user.

## Step 2: Create Branch and Draft PR

Follow the branch/PR naming convention: `<type>-<issue_number>-<short-description>`

Type mapping: `enhancement` → `feat`, `bug` → `fix`, `chore` → `chore`

```bash
git checkout main && git pull origin main
git checkout -b <type>-<number>-<description>
git push -u origin <type>-<number>-<description>
```

Create a **draft PR** immediately so progress is visible:

```bash
gh pr create --draft \
  --title "<type>-<number>-<description>" \
  --body "$(cat <<'EOF'
Closes #<number>

## Summary
- <to be updated on completion>

## Test Plan
<copy Test Plan table from issue, add Status column set to pending>
EOF
)"
```

## Step 3: Break Down into Tasks

Analyze the issue scenarios and create a task breakdown. Save to `.claude/plans/<issue_number>/tasks.json`.

```bash
mkdir -p .claude/plans/<issue_number>
```

See [TASK-FORMAT.md](TASK-FORMAT.md) for the JSON schema and decomposition rules.

**Key rules:**
- Each task maps to one or more scenarios from the issue
- Tasks that touch different files/modules can run in parallel
- Tests are a separate task per module, but run after implementation tasks
- Every task has a clear "done when" that maps back to a scenario

## Step 4: Execute Tasks

Read `.claude/plans/<issue_number>/tasks.json` and identify which tasks can run in parallel (no shared files, no dependency between them).

For each task, spawn a sub-agent with:
- The task description and acceptance criteria from tasks.json
- The relevant scenario(s) from the issue
- The specific files to create/modify
- Instructions to commit when done (see [AGENT-INSTRUCTIONS.md](AGENT-INSTRUCTIONS.md))

**Parallel execution rules:**
- Tasks in the same `parallel_group` run simultaneously
- Tasks with `depends_on` wait for dependencies to complete
- After each sub-agent finishes, update tasks.json status

```bash
# After each task completes, update the task status
# The sub-agent commits its own changes
```

## Step 5: Verify and Finalize

After all tasks complete:

```bash
# Run linter and type checker — must pass before PR is marked ready
uv run ruff check .
uv run ty check

# Run full test suite
uv run pytest tests/ -v

# Update the draft PR to ready
gh pr ready <pr_number>
```

If ruff, ty, or tests fail, fix the issues and commit before marking the PR ready.

Update the PR body with final summary and test plan status.

## Boundaries

**Always:**
- Create `.claude/plans/<issue_number>/tasks.json` before writing any code
- Commit after each task (not one big commit at the end)
- Run tests after implementation tasks before marking complete
- Run `uv run ruff check .` and `uv run ty check` before marking PR ready — lint and type check must pass
- Update tasks.json status as tasks complete

**Ask first:**
- If a task requires changing files not mentioned in the issue
- If dependencies need updating
- If the issue scope seems larger than expected

**Never:**
- Skip creating the task plan
- Modify files outside the task's listed scope without flagging it
- Force push or amend commits from other sub-agents
- Mark the PR as ready without passing tests