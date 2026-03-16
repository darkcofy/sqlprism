# PR Comment Template

The synthesized review posted to GitHub follows this format exactly.

## Template

```markdown
## PR Review — #<number>

### Critical
| Reviewer | File:Line | Finding | Suggestion |
|----------|-----------|---------|------------|
| <SWE/DE/QA> | `path:line` | description | fix |

### Warnings
| Reviewer | File:Line | Finding | Suggestion |
|----------|-----------|---------|------------|
| <SWE/DE/QA> | `path:line` | description | fix |

### Suggestions
| Reviewer | File:Line | Finding | Suggestion |
|----------|-----------|---------|------------|
| <SWE/DE/QA> | `path:line` | description | fix |

### Test Coverage
- [ ] All scenarios from issue Test Plan have corresponding tests
- [ ] Edge cases covered
- [ ] No regression risk identified

### Summary
<2-3 sentence synthesis: overall assessment, key risks, recommendation>

---
*Reviewed by: SWE, Data Engineer, QA Engineer*
*🤖 Generated with [Claude Code](https://claude.com/claude-code)*
```

## Rules

- **Deduplicate**: if two reviewers flag the same issue, list it once and credit both
- **Order by severity**: critical first, then warnings, then suggestions
- **Omit empty sections**: if no criticals, remove the Critical table entirely
- **Keep it scannable**: no prose paragraphs in the tables, save that for Summary
- **Link to lines**: use `path:line` format so GitHub renders clickable links in the diff

## Example

```markdown
## PR Review — #9

### Critical
| Reviewer | File:Line | Finding | Suggestion |
|----------|-----------|---------|------------|
| DE | `src/sqlprism/renderers/dbt.py:87` | `--select` flag placed before `compile` subcommand — dbt ignores it | Move `--select` after `compile`: `dbt compile --select model` |

### Warnings
| Reviewer | File:Line | Finding | Suggestion |
|----------|-----------|---------|------------|
| SWE | `src/sqlprism/renderers/dbt.py:92` | No timeout on subprocess call — could hang indefinitely | Add `timeout=300` to `subprocess.run()` |
| QA | `tests/test_renderers.py` | No test for compilation timeout scenario | Add `test_dbt_render_models_timeout` |

### Suggestions
| Reviewer | File:Line | Finding | Suggestion |
|----------|-----------|---------|------------|
| SWE | `src/sqlprism/renderers/dbt.py:95` | Hardcoded `target/compiled/` path | Extract to constant `_DBT_COMPILED_DIR` |

### Test Coverage
- [x] All scenarios from issue #9 Test Plan have corresponding tests
- [ ] Edge cases covered — missing: timeout, empty model list
- [x] No regression risk identified

### Summary
The `--select` flag ordering is a blocking issue — dbt silently ignores it, so no models actually compile. The subprocess timeout is a real risk in CI. Otherwise solid implementation with good test coverage.

---
*Reviewed by: SWE, Data Engineer, QA Engineer*
*🤖 Generated with [Claude Code](https://claude.com/claude-code)*
```