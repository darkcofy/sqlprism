# Sub-Agent Instructions

When spawning a sub-agent for a task, provide these instructions along with the task-specific context.

## Prompt Template

```
You are implementing task {task_id} for issue #{issue_number} in the SQLPrism project.

Tech stack: Python 3.12, DuckDB, sqlglot, FastMCP, click, pytest, uv.

## Your Task
{task_title}

## Files to Modify
{files list}

## Scenarios to Satisfy
{scenarios from the task, copied from the issue's BDD scenarios}

## Done When
{done_when}

## Instructions

1. Read the existing code in the files listed above before making changes
2. Implement the changes to satisfy the scenarios
3. If this is a test task, ensure test names match the issue's Test Plan exactly
4. Run lint and type check to verify:
   ```bash
   uv run ruff check .
   uv run ty check
   ```
5. Run the relevant tests to verify:
   ```bash
   uv run pytest {test_file} -v -k "{test_pattern}"
   ```
6. If lint and tests pass, commit with:
   ```bash
   git add {files}
   git commit -m "{commit_message}

   Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
   ```
7. If lint or tests fail, fix the issue and try again (max 3 attempts)
8. Return a summary: what changed, tests passed/failed, any concerns

## Boundaries
- Only modify files listed in your task
- If you need to change other files, return with a note explaining why — do not modify them
- Do not amend or rebase existing commits
- Do not push to remote (the main agent handles this)
```

## What the Sub-Agent Returns

Each sub-agent must return a structured result:

```
Task: {task_id}
Status: completed | failed
Files changed: {list}
Tests: {passed_count} passed, {failed_count} failed
Commit: {short sha} {commit message}
Notes: {any concerns, blockers, or scope questions}
```

The main agent uses this to update `tasks.json` and decide next steps.
