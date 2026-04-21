# Contributing to SQLPrism

Thanks for your interest. This document covers the workflow, conventions, and
checks that apply to all changes.

## Development setup

Requirements:

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

```bash
git clone https://github.com/darkcofy/sqlprism.git
cd sqlprism
uv sync
```

This installs the package and dev dependencies (pytest, ruff, ty, mkdocs) into
a local `.venv`.

## Running checks locally

Every PR must pass these before it is marked ready:

```bash
uv run ruff check .       # lint
uv run ty check           # type check
uv run pytest tests/ -v   # tests
```

To run a single test:

```bash
uv run pytest tests/test_indexer.py::test_name -v
```

Coverage is configured with a floor of 80% (`[tool.coverage.report] fail_under`).

## Branch and PR conventions

Branch names follow the pattern `<type>-<issue>-<short-description>`:

- `feat-11-indexer-reindex-files`
- `fix-131-trace-cross-repo-shadow-nodes`
- `chore-134-critical-review-fixes`

Valid types:

- `feat` — new functionality or enhancement
- `fix` — bug fix
- `chore` — maintenance, refactors, docs, hygiene

PR titles should mirror the branch name style. The PR body should link the
issue with `Closes #<number>`. Open PRs as drafts first; mark them ready once
`ruff`, `ty`, and `pytest` all pass locally.

## Issue conventions

Issues use a BDD format where applicable: `Given / When / Then` scenarios
with concrete acceptance criteria. This makes it easier to decompose an issue
into tasks and to know when it is done.

## Documentation

User-facing docs live under `docs/` and are published with MkDocs Material.
Preview locally:

```bash
uv run mkdocs serve
```

Architectural or longer-form notes can also live under `docs/architecture/`.

## Reporting security issues

See [SECURITY.md](SECURITY.md). Please do not file public issues for
suspected vulnerabilities.
