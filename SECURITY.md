# Security policy

## Supported versions

SQLPrism is pre-1.x in spirit (published as 1.x with a "Beta" development
status). Only the latest minor release on the `main` branch receives security
fixes.

| Version | Supported |
|---------|-----------|
| 1.2.x   | Yes       |
| < 1.2   | No        |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for suspected vulnerabilities.

Instead, email the maintainer at **alfjohnfred@gmail.com** with:

- A description of the issue and its potential impact.
- Steps to reproduce, or a minimal proof of concept.
- Any relevant logs, stack traces, or affected commits.

You can expect an acknowledgement within **72 hours**. If the report is
confirmed, we will work on a fix and coordinate a release; you will be
credited in the `CHANGELOG.md` entry unless you prefer to remain anonymous.

## Scope

In scope:

- Arbitrary code execution, SQL injection, or path traversal in the parser,
  indexer, CLI, or MCP server.
- Secret leakage through logs, snippets, or the graph store.
- Unsafe subprocess handling in the dbt / sqlmesh renderers.

Out of scope:

- Vulnerabilities in upstream dependencies (DuckDB, sqlglot, dbt, sqlmesh) —
  please report those to the respective projects.
- Findings that require the attacker to already control the machine running
  `sqlprism serve`.
