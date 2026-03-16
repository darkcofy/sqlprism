# Task Format

## Schema: `.claude/plans/<issue_number>/tasks.json`

```json
{
  "issue": 9,
  "title": "Add render_models() with --select support to dbt renderer",
  "branch": "feat-9-dbt-render-models",
  "tasks": [
    {
      "id": "T1",
      "title": "Add render_models() method to DbtRenderer",
      "status": "pending",
      "parallel_group": "A",
      "depends_on": [],
      "files": ["src/sqlprism/renderers/dbt.py"],
      "scenarios": [
        "Compile a single dbt model",
        "Compile multiple dbt models in one call",
        "Model compilation fails"
      ],
      "done_when": "render_models() compiles selected models via --select and returns parsed results",
      "commit_message": "feat(dbt): add render_models with --select support"
    },
    {
      "id": "T2",
      "title": "Ensure render_project remains unchanged",
      "status": "pending",
      "parallel_group": "A",
      "depends_on": [],
      "files": ["src/sqlprism/renderers/dbt.py"],
      "scenarios": ["render_project remains unchanged"],
      "done_when": "Existing render_project tests still pass with no code changes to that method",
      "commit_message": ""
    },
    {
      "id": "T3",
      "title": "Add tests for render_models",
      "status": "pending",
      "parallel_group": "B",
      "depends_on": ["T1"],
      "files": ["tests/test_renderers.py"],
      "scenarios": [
        "Compile a single dbt model",
        "Compile multiple dbt models in one call",
        "Model compilation fails",
        "render_project remains unchanged"
      ],
      "done_when": "All 4 test functions from Test Plan pass",
      "commit_message": "test(dbt): add tests for render_models"
    }
  ]
}
```

## Decomposition Rules

### Sizing
- Each task should be completable by a single sub-agent in one pass
- If a task touches more than 3 files, split it
- If a task has more than 4 scenarios, consider splitting by happy path vs edge cases

### Parallel Groups
- Tasks in the same `parallel_group` letter (A, B, C...) can run simultaneously
- Group A typically has no dependencies — these run first
- Group B depends on group A, group C on B, etc.
- Tasks sharing the same file **cannot** be in the same parallel group

### Task Types
| Type | Parallel? | Commit? |
|------|-----------|---------|
| Implementation (new code) | Yes, if different files | Yes |
| Refactor (modify existing) | Usually sequential | Yes |
| Tests | After implementation | Yes |
| Verification (no-op check) | With anything | No commit needed |

### Status Values
- `pending` — not started
- `in_progress` — sub-agent working on it
- `completed` — committed and verified
- `failed` — sub-agent encountered an error
- `skipped` — not needed (explain in notes)

### Commit Messages
Follow conventional commits: `<type>(<scope>): <description>`

| Type | When |
|------|------|
| `feat` | New functionality |
| `fix` | Bug fix |
| `test` | Adding/updating tests |
| `chore` | Migration, config, maintenance |
| `refactor` | Code change that doesn't add/fix |