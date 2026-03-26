# Conventions

The conventions engine automatically infers project patterns from the indexed SQL graph -- naming conventions, layer references, required columns, column naming style, and semantic tags. It runs after every reindex, giving AI agents (and developers) a machine-readable description of how the project is structured.

## How It Works

- Convention inference runs automatically after reindex.
- Detects layers from directory structure (`models/staging/`, `models/marts/`, etc.).
- Infers naming patterns per layer (e.g. `stg_{source}_{entity}`).
- Infers reference rules (which layers reference which).
- Detects required columns (columns appearing in >70% of models per layer).
- Detects column naming style (`snake_case`, `camelCase`, etc.).
- Assigns semantic tags via structural clustering (no ML, deterministic).

## Confidence Scores

All conventions have a confidence score (0.0--1.0):

| Range | Meaning |
|---|---|
| **>0.9** | High confidence. Follow this convention. |
| **0.7--0.9** | Moderate. Likely correct but worth verifying. |
| **<0.7** | Low confidence. Consider an explicit override. |

## Overrides

You can override inferred conventions with a YAML file:

```bash
sqlprism conventions init      # generates sqlprism.conventions.yml
# edit the file to add explicit overrides
sqlprism conventions refresh   # re-runs inference, preserving overrides
sqlprism conventions diff      # shows changes since last init
```

Overrides take precedence over inferred conventions (`source: 'override'` vs `source: 'inferred'`).

## MCP Tools

Five MCP tools expose conventions to AI agents:

| Tool | Description |
|---|---|
| `get_conventions` | Naming rules, reference rules, required columns per layer. |
| `find_similar_models` | Find existing models similar to what you're building. |
| `suggest_placement` | Recommend where to place a new model based on references. |
| `search_by_tag` | Find models by semantic tag (business domain concept). |
| `list_tags` | List all semantic tags with model counts and confidence. |

See [MCP Tools](mcp-tools.md) for parameter details.

## Semantic Tags

Tags are assigned by structural clustering -- models that share many upstream references get grouped and auto-labeled based on common name tokens. Tags represent business domain concepts (e.g. "customer", "order", "revenue").

Tag sources:

| Source | Description |
|---|---|
| **inferred** | Automatically assigned via clustering. |
| **anchor** | Manually specified in the YAML override as cluster anchors. |
| **explicit** | Manually assigned to specific models in the YAML override. |

## Example Workflow

```bash
# 1. Index your project
sqlprism reindex

# 2. Generate conventions file
sqlprism conventions init
# -> creates sqlprism.conventions.yml with inferred conventions

# 3. Review and adjust
# Edit the YAML to fix any conventions the engine got wrong

# 4. Re-run inference (preserves your overrides)
sqlprism conventions refresh

# 5. Check what changed
sqlprism conventions diff
```
