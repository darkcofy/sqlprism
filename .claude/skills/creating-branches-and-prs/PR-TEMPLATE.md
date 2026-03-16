# PR Body Template

```markdown
Closes #<issue_number>

## Summary
- <what changed and why — 1-3 bullet points>

## Test Plan
| Scenario | Test | Status |
|----------|------|--------|
| <from issue> | `test_function_name` | <pass/fail/pending> |
| <from issue> | `test_function_name` | <pass/fail/pending> |

## Notes
<optional: anything reviewers should know — migration steps, breaking changes, etc.>
```

## Guidelines

- **Summary** should explain *why*, not just *what* — the diff shows the what
- **Test Plan** is copied from the issue's Test Plan section, with a Status column added
- **Closes** keyword auto-closes the linked issue on merge
- Keep the PR scoped to one issue — if it touches multiple issues, split into multiple PRs