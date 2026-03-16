# BDD Issue Template

Every issue follows this structure. Scenarios double as acceptance criteria AND test case specifications.

## Template

```markdown
## Description
Brief context: what is being done and why.

## Scenarios

### Scenario: <descriptive name — the happy path>
**Given** <precondition or initial state>
**When** <action or event>
**Then** <expected outcome>
**And** <additional assertions if needed>

### Scenario: <edge case or error path>
**Given** <precondition>
**When** <action>
**Then** <expected behavior>

### Scenario: <another edge case>
...

## Test Plan

| Scenario | Test | File |
|----------|------|------|
| <scenario name> | `test_<descriptive_function_name>` | `tests/<test_file>.py` |
| <scenario name> | `test_<descriptive_function_name>` | `tests/<test_file>.py` |

## Dependencies
- Blocked by: #<issue_number>, #<issue_number>
- Blocks: #<issue_number>

## Notes
Implementation hints, links to plan sections, design decisions.
```

## Writing Good Scenarios

### DO
- Name scenarios descriptively: `Scenario: File outside any configured repo is skipped`
- Keep each scenario focused on one behavior
- Include both happy path and error/edge case scenarios
- Use concrete values in examples: `"stg_orders.sql"` not `"a file"`
- Write scenarios that translate directly to test functions

### DON'T
- Don't write vague scenarios: `Scenario: It works correctly`
- Don't combine multiple behaviors in one scenario
- Don't skip edge cases — they're where bugs hide
- Don't write implementation details in Given/When/Then — describe behavior

## Scenario Categories

Each issue should cover these categories where applicable:

1. **Happy path** — the normal, expected use case
2. **Error handling** — what happens when things fail
3. **Edge cases** — boundary conditions, empty inputs, concurrent access
4. **Backwards compatibility** — existing behavior remains unchanged
5. **Integration** — how this interacts with adjacent components

## Example

```markdown
## Description
Add checksum-based skip logic to avoid re-parsing unchanged files during reindex.

## Scenarios

### Scenario: Unchanged file is skipped
**Given** a file `stg_orders.sql` already indexed with checksum `abc123`
**When** `reindex_files()` is called and the file still has checksum `abc123`
**Then** no parse or database write occurs
**And** the file appears in the result as `skipped: unchanged`

### Scenario: Changed file is reindexed
**Given** a file `stg_orders.sql` indexed with checksum `abc123`
**When** the file content changes (new checksum `def456`) and `reindex_files()` is called
**Then** the file is re-parsed and the graph is updated
**And** the stored checksum is updated to `def456`

### Scenario: New file has no previous checksum
**Given** a file `new_model.sql` that has never been indexed
**When** `reindex_files()` is called
**Then** the file is parsed and inserted
**And** no checksum comparison is attempted

## Test Plan

| Scenario | Test | File |
|----------|------|------|
| Unchanged file is skipped | `test_reindex_unchanged_file_skipped` | `tests/test_indexer.py` |
| Changed file is reindexed | `test_reindex_changed_file_updates_graph` | `tests/test_indexer.py` |
| New file has no previous checksum | `test_reindex_new_file_no_checksum_check` | `tests/test_indexer.py` |

## Dependencies
- Blocked by: #8
- Blocks: #12

## Notes
Checksum uses SHA-256 of file content. Stored in the `files` table.
```