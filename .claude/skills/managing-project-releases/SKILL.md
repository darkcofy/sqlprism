---
name: managing-project-releases
description: Decomposes features or releases into BDD-formatted GitHub issues with Given/When/Then scenarios and test cases. Creates milestones, assigns labels, and adds issues to the project board. Use when starting a new release, planning a feature, or creating issue tickets.
---

# Managing Project Releases

## Context

- **GitHub repo:** darkcofy/sqlprism
- **GitHub Project:** SQLPrism (project number 1, owner darkcofy)
- **Labels:** `enhancement`, `bug`, `chore` (no sizing labels)
- **Branch convention:** `feat-<name>`, `fix-<name>`, `chore-<name>`

## Workflow

1. **Read the implementation plan** for the release from `.claude/plans/`
2. **Create a milestone** named after the release version (e.g., `v1.0.1`)
3. **Decompose into issues** — one per logical unit of work
4. **Write each issue in BDD format** — see [BDD-TEMPLATE.md](BDD-TEMPLATE.md)
5. **Add all issues** to the GitHub Project board
6. **Update dependency references** with actual issue numbers after creation

## Issue Decomposition Guidelines

Break work into issues that are:
- **Independent where possible** — parallelizable steps get separate issues
- **Ordered by dependency** — identify what blocks what
- **Testable** — every issue has BDD scenarios that become test cases
- **Single responsibility** — one issue = one logical change

Group by layer, not by file. Example layers:
- Schema/migration changes
- Renderer/parser changes
- Core logic (indexer, engine)
- API/tool surface (MCP tools, CLI commands)
- Tests (can be a single issue or per-layer)

## Creating Issues

```bash
gh issue create \
  --milestone "<version>" \
  --label "<type>" \
  --title "<concise title>" \
  --body "$(cat <<'EOF'
<BDD body — see BDD-TEMPLATE.md>
EOF
)"
```

After creating all issues, add them to the project:

```bash
gh project item-add 1 --owner darkcofy --url "<issue_url>"
```

Then update dependency cross-references to use actual issue numbers.

## Creating Milestones

```bash
gh api repos/darkcofy/sqlprism/milestones \
  -f title="<version>" \
  -f description="<one-line summary>" \
  -f state="open"
```

## Label Selection

| Label | When to use |
|-------|------------|
| `enhancement` | New features, new capabilities |
| `bug` | Fixing broken behavior |
| `chore` | Migrations, CI, deps, refactors |