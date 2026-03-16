---
name: creating-branches-and-prs
description: Creates git branches and GitHub pull requests following the project naming convention. Enforces the format type-issue_number-short-description for both branches and PR titles. Use when creating a branch, opening a PR, or asking about branch/PR naming.
---

# Creating Branches and PRs

## Naming Convention

Both branches and PR titles use the same format:

```
<type>-<issue_number>-<short-description>
```

### Rules

- **Lowercase, hyphens only** — no spaces, underscores, slashes, or camelCase
- **Type prefix** — matches the issue label
- **Issue number** — the GitHub issue this work addresses
- **Short description** — 2-4 words summarizing the change

### Types

| Type | When | Maps to label |
|------|------|---------------|
| `feat` | New functionality | `enhancement` |
| `fix` | Bug fix | `bug` |
| `chore` | Maintenance, CI, deps, migrations | `chore` |

### Examples

| Issue | Branch / PR title |
|-------|-------------------|
| #8 Add repo_type column | `chore-8-repo-type-column` |
| #9 dbt render_models | `feat-9-dbt-render-models` |
| #11 Indexer reindex_files | `feat-11-indexer-reindex-files` |
| #14 Tests for reindex | `feat-14-reindex-on-save-tests` |

## Creating a Branch

```bash
git checkout main
git pull origin main
git checkout -b <type>-<issue_number>-<short-description>
```

## Creating a PR

Link the PR to its issue using `Closes #<number>` in the body. This auto-closes the issue on merge.

```bash
gh pr create \
  --title "<type>-<issue_number>-<short-description>" \
  --body "$(cat <<'EOF'
Closes #<issue_number>

## Summary
<1-3 bullet points>

## Test plan
<checklist from the issue's Test Plan section>
EOF
)"
```

## PR Body Structure

See [PR-TEMPLATE.md](PR-TEMPLATE.md) for the full template.

## Validation Checklist

Before creating a branch or PR, verify:
- [ ] Type matches the issue label (`enhancement` → `feat`, `bug` → `fix`, `chore` → `chore`)
- [ ] Issue number exists and is open
- [ ] Branch is based on latest `main`
- [ ] PR body includes `Closes #<number>`