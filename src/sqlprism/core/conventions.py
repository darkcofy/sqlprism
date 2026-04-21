"""Convention inference engine.

Discovers project conventions from the knowledge graph: directory-based layers,
naming patterns per layer, and confidence scores. Runs after reindex and stores
results in the ``conventions`` table.

Design principles:
    - Infer first, override second. Zero-config.
    - Confidence everywhere (0.0-1.0).
    - No hardcoded domain keywords.
    - No ML, no embeddings. Pure SQL + Python string analysis.
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from sqlprism.core.graph import GraphDB

logger = logging.getLogger(__name__)


@dataclass
class Layer:
    """A detected project layer (e.g. staging, intermediate, marts)."""

    name: str
    path_pattern: str
    model_count: int
    model_names: list[str] = field(default_factory=list)
    confidence: float = 0.6


@dataclass
class NamingPattern:
    """An inferred naming convention for a layer."""

    pattern: str
    confidence: float
    matching_count: int
    total_count: int
    exceptions: list[str] = field(default_factory=list)


@dataclass
class ReferenceRule:
    """Inferred reference rule: which layers a source layer references."""

    source_layer: str
    allowed_targets: list[str]
    target_distribution: dict[str, float]
    confidence: float


@dataclass
class RequiredColumn:
    """A column that appears frequently in a layer."""

    column_name: str
    frequency: float
    source: str  # 'definition' | 'usage' | 'both'
    missing_in: list[str] = field(default_factory=list)


@dataclass
class ColumnStyle:
    """Dominant column naming convention for a layer."""

    style: str  # 'snake_case' | 'camelCase' | 'PascalCase' | 'SCREAMING_SNAKE'
    confidence: float


@dataclass
class TagAssignment:
    """A semantic tag assigned to a model via structural clustering."""

    tag_name: str
    node_id: int
    model_name: str
    confidence: float
    source: str  # 'inferred'


@dataclass
class Cluster:
    """A group of models with similar upstream references."""

    members: list[tuple[int, str]]  # (node_id, model_name)
    ref_sets: dict[int, set[int]]  # node_id -> set of upstream node_ids


class ConventionEngine:
    """Infers project conventions from the knowledge graph.

    Takes a ``GraphDB`` instance and repo_id. Call ``detect_layers()``
    and ``infer_naming_pattern()`` to analyse the graph.
    """

    def __init__(self, db: GraphDB, repo_id: int) -> None:
        self.db = db
        self.repo_id = repo_id

    def run_inference(
        self, project_path: str | Path | None = None
    ) -> dict:
        """Run all convention inference steps and store results.

        Detects layers, infers naming patterns, reference rules,
        common columns, and column style for each layer. Upserts
        results into the ``conventions`` table. Then applies any
        YAML overrides from the project directory.

        Args:
            project_path: Project directory for override file discovery.
                If None, no overrides are applied.

        Returns:
            Stats dict with layers_detected, conventions_stored, and
            overrides_applied counts.
        """
        layers = self.detect_layers()
        if not layers:
            logger.debug("No layers detected for repo %d", self.repo_id)
            return {
                "layers_detected": 0,
                "conventions_stored": 0,
                "overrides_applied": 0,
            }

        reference_rules = self.infer_reference_rules(layers)
        ref_rules_by_layer = {r.source_layer: r for r in reference_rules}

        # Load overrides from YAML before acquiring the write lock
        # (file I/O should not hold the DB lock).
        overrides = self.load_overrides(project_path)

        stored = 0
        overrides_applied = 0
        # Single transaction for both inference and overrides —
        # concurrent readers never see partial state.
        with self.db.write_transaction():
            for layer in layers:
                # Step 2: Naming pattern
                naming = self.infer_naming_pattern(layer.model_names)
                self._store_convention(
                    layer.name,
                    "naming",
                    {
                        "pattern": naming.pattern,
                        "matching_count": naming.matching_count,
                        "total_count": naming.total_count,
                        "exceptions": naming.exceptions,
                    },
                    naming.confidence,
                    layer.model_count,
                )
                stored += 1

                # Step 3: Reference rules
                ref_rule = ref_rules_by_layer.get(layer.name)
                if ref_rule:
                    self._store_convention(
                        layer.name,
                        "references",
                        {
                            "allowed_targets": ref_rule.allowed_targets,
                            "target_distribution": ref_rule.target_distribution,
                        },
                        ref_rule.confidence,
                        layer.model_count,
                    )
                    stored += 1

                # Step 4: Common columns
                common_cols = self.infer_common_columns(layer)
                if common_cols:
                    self._store_convention(
                        layer.name,
                        "required_columns",
                        {
                            "columns": [
                                {
                                    "name": c.column_name,
                                    "frequency": c.frequency,
                                    "source": c.source,
                                    "missing_in": c.missing_in,
                                }
                                for c in common_cols
                            ]
                        },
                        min(max(c.frequency for c in common_cols), 1.0),
                        layer.model_count,
                    )
                    stored += 1

                # Step 5: Column style
                style = self.detect_column_style(layer)
                if style.confidence > 0.0:
                    self._store_convention(
                        layer.name,
                        "column_style",
                        {"style": style.style},
                        min(style.confidence, 1.0),
                        layer.model_count,
                    )
                    stored += 1

            # Apply YAML overrides within the same transaction
            if overrides:
                overrides_applied = self._apply_overrides_inner(overrides)

        logger.info(
            "Convention inference: %d layers, %d conventions stored, "
            "%d overrides applied",
            len(layers),
            stored,
            overrides_applied,
        )
        return {
            "layers_detected": len(layers),
            "conventions_stored": stored,
            "overrides_applied": overrides_applied,
        }

    def _store_convention(
        self,
        layer: str,
        convention_type: str,
        payload: dict,
        confidence: float,
        model_count: int,
    ) -> None:
        """Upsert a convention into the conventions table.

        Skips rows with ``source='override'`` — explicit overrides
        are preserved and not clobbered by inference.
        Must be called inside ``write_transaction()``.
        """
        self.db._execute_write(
            "INSERT INTO conventions "
            "(repo_id, layer, convention_type, payload, "
            "confidence, source, model_count) "
            "VALUES (?, ?, ?, ?, ?, 'inferred', ?) "
            "ON CONFLICT (repo_id, layer, convention_type) "
            "DO UPDATE SET "
            "payload = EXCLUDED.payload, "
            "confidence = EXCLUDED.confidence, "
            "model_count = EXCLUDED.model_count "
            "WHERE conventions.source != 'override'",
            [
                self.repo_id,
                layer,
                convention_type,
                json.dumps(payload),
                min(confidence, 1.0),
                model_count,
            ],
        )

    def generate_yaml(self) -> str:
        """Generate a YAML conventions file with confidence scores as comments.

        Reads from the ``conventions`` table and formats the output
        as a human-readable YAML file with inline confidence comments.
        Suitable for writing to ``sqlprism.conventions.yml``.
        """
        rows = self.db._execute_read(
            "SELECT layer, convention_type, payload, confidence, source "
            "FROM conventions WHERE repo_id = ? "
            "ORDER BY layer, convention_type",
            [self.repo_id],
        ).fetchall()

        if not rows:
            return "# No conventions found. Run 'sqlprism reindex' first.\n"

        # Group by layer
        layers: dict[str, dict[str, tuple]] = {}
        for layer, conv_type, payload, conf, source in rows:
            layers.setdefault(layer, {})[conv_type] = (payload, conf, source)

        lines = [
            "# Auto-generated by sqlprism conventions --init",
            "# Review and adjust. Override values to set confidence to 1.0.",
            "",
            "conventions:",
        ]

        for layer_name in sorted(layers):
            convs = layers[layer_name]
            lines.append(f"  {layer_name}:")

            # Naming
            if "naming" in convs:
                payload, conf, source = convs["naming"]
                parsed = json.loads(payload) if isinstance(payload, str) else payload
                pattern = parsed.get("pattern", "")
                exceptions = parsed.get("exceptions", [])
                exc_note = f", exceptions: {exceptions}" if exceptions else ""
                lines.append(
                    f'    naming: "{pattern}"'
                    f"  # confidence: {conf:.2f}{exc_note}"
                )

            # References
            if "references" in convs:
                payload, conf, source = convs["references"]
                parsed = json.loads(payload) if isinstance(payload, str) else payload
                targets = parsed.get("allowed_targets", [])
                lines.append(f"    allowed_refs:  # confidence: {conf:.2f}")
                for t in targets:
                    lines.append(f'      - "{t}"')

            # Required columns
            if "required_columns" in convs:
                payload, conf, source = convs["required_columns"]
                parsed = json.loads(payload) if isinstance(payload, str) else payload
                columns = parsed.get("columns", [])
                lines.append(
                    "    required_columns:"
                    "  # frequency threshold: 0.70"
                )
                for col in columns:
                    name = col.get("name", "")
                    freq = col.get("frequency", 0)
                    lines.append(f"      - {name}  # frequency: {freq:.2f}")

            # Column style
            if "column_style" in convs:
                payload, conf, source = convs["column_style"]
                parsed = json.loads(payload) if isinstance(payload, str) else payload
                style = parsed.get("style", "")
                lines.append(
                    f'    column_style: "{style}"'
                    f"  # confidence: {conf:.2f}"
                )

            lines.append("")

        return "\n".join(lines) + "\n"

    def get_diff(self, yaml_path: str | Path) -> str:
        """Compare current conventions against a YAML file.

        Reports only actual differences: new/removed layers,
        changed naming patterns, changed references, etc.
        """
        yaml_path = Path(yaml_path)
        if not yaml_path.is_file():
            return "No conventions YAML file found to compare against."

        try:
            existing = yaml.safe_load(yaml_path.read_text()) or {}
        except (yaml.YAMLError, OSError) as e:
            return f"Failed to read {yaml_path}: {e}"

        existing_convs = existing.get("conventions", {})

        # Get current conventions from DB
        rows = self.db._execute_read(
            "SELECT layer, convention_type, payload, confidence "
            "FROM conventions WHERE repo_id = ?",
            [self.repo_id],
        ).fetchall()

        current: dict[str, dict] = {}
        for layer, conv_type, payload, conf in rows:
            parsed = json.loads(payload) if isinstance(payload, str) else payload
            current.setdefault(layer, {})[conv_type] = {
                "payload": parsed,
                "confidence": conf,
            }

        changes: list[str] = []

        # New/removed layers
        current_layers = set(current)
        yaml_layers = set(existing_convs)
        for layer in sorted(current_layers - yaml_layers):
            changes.append(f"+ New layer: {layer}")
        for layer in sorted(yaml_layers - current_layers):
            changes.append(f"- Removed layer: {layer}")

        # Changed conventions in shared layers
        for layer in sorted(current_layers & yaml_layers):
            curr = current.get(layer, {})
            yaml_layer = existing_convs.get(layer, {})

            # Naming: compare DB pattern vs YAML naming string
            curr_naming = curr.get("naming", {}).get("payload", {}).get("pattern", "")
            yaml_naming = yaml_layer.get("naming", "")
            if curr_naming and not yaml_naming:
                changes.append(f"+ {layer}.naming: \"{curr_naming}\"")
            elif yaml_naming and not curr_naming:
                changes.append(f"- {layer}.naming: \"{yaml_naming}\"")
            elif curr_naming != yaml_naming:
                changes.append(
                    f"  {layer}.naming: "
                    f"\"{yaml_naming}\" -> \"{curr_naming}\""
                )

            # References: compare allowed_refs lists
            curr_refs = curr.get("references", {}).get("payload", {}).get("allowed_targets", [])
            yaml_refs = yaml_layer.get("allowed_refs", [])
            if sorted(curr_refs) != sorted(yaml_refs):
                if curr_refs and not yaml_refs:
                    changes.append(f"+ {layer}.allowed_refs: {curr_refs}")
                elif yaml_refs and not curr_refs:
                    changes.append(f"- {layer}.allowed_refs: {yaml_refs}")
                else:
                    changes.append(
                        f"  {layer}.allowed_refs: {yaml_refs} -> {curr_refs}"
                    )

            # Column style
            curr_style = curr.get("column_style", {}).get("payload", {}).get("style", "")
            yaml_style = yaml_layer.get("column_style", "")
            if curr_style != yaml_style and (curr_style or yaml_style):
                changes.append(
                    f"  {layer}.column_style: "
                    f"\"{yaml_style}\" -> \"{curr_style}\""
                )

        if not changes:
            return "No changes detected."
        return "\n".join(changes) + "\n"

    def load_overrides(
        self, project_path: str | Path | None = None
    ) -> dict | None:
        """Load convention overrides from YAML file.

        Discovery order:
        1. ``sqlprism.conventions.yml`` in project_path
        2. ``.sqlprism/sqlprism.conventions.yml`` in project_path

        Returns parsed YAML dict, or None if no override file found.
        """
        if project_path is None:
            return None

        project_path = Path(project_path)
        candidates = [
            project_path / "sqlprism.conventions.yml",
            project_path / ".sqlprism" / "sqlprism.conventions.yml",
        ]

        for path in candidates:
            if path.is_file():
                try:
                    data = yaml.safe_load(path.read_text())
                    if isinstance(data, dict):
                        logger.info("Loaded convention overrides from %s", path)
                        return data
                except (yaml.YAMLError, OSError) as e:
                    logger.warning("Failed to load overrides from %s: %s", path, e)

        return None

    def apply_overrides(self, overrides: dict) -> int:
        """Apply explicit convention overrides to the conventions table.

        Overrides replace inferred values entirely with ``confidence=1.0``
        and ``source='override'``. Layers not in overrides keep their
        inferred values. Layers in overrides but not in inference are
        created.

        Args:
            overrides: Parsed YAML dict with ``conventions`` and/or
                ``semantic_tags`` keys.

        Returns:
            Number of override conventions stored.
        """
        with self.db.write_transaction():
            return self._apply_overrides_inner(overrides)

    def _apply_overrides_inner(self, overrides: dict) -> int:
        """Apply overrides. Must be called inside write_transaction()."""
        conventions = overrides.get("conventions", {})
        if not conventions:
            return 0

        stored = 0
        for layer_name, layer_config in conventions.items():
            if not isinstance(layer_config, dict):
                continue

            # Naming pattern override
            naming = layer_config.get("naming")
            if isinstance(naming, str):
                self._store_override(
                    layer_name,
                    "naming",
                    {"pattern": naming},
                )
                stored += 1

            # Allowed references override
            refs = layer_config.get("allowed_refs")
            if isinstance(refs, list):
                self._store_override(
                    layer_name,
                    "references",
                    {
                        "allowed_targets": refs,
                        "target_distribution": {},
                    },
                )
                stored += 1

            # Required columns override
            cols = layer_config.get("required_columns")
            if isinstance(cols, list):
                self._store_override(
                    layer_name,
                    "required_columns",
                    {
                        "columns": [
                            {
                                "name": c,
                                "frequency": 1.0,
                                "source": "override",
                                "missing_in": [],
                            }
                            for c in cols
                            if isinstance(c, str)
                        ]
                    },
                )
                stored += 1

            # Column style override
            style = layer_config.get("column_style")
            if isinstance(style, str):
                self._store_override(
                    layer_name,
                    "column_style",
                    {"style": style},
                )
                stored += 1

        return stored

    def _store_override(
        self,
        layer: str,
        convention_type: str,
        payload: dict,
    ) -> None:
        """Store a convention override (confidence=1.0, source='override').

        Replaces any existing value (inferred or override).
        Must be called inside ``write_transaction()``.
        """
        self.db._execute_write(
            "INSERT INTO conventions "
            "(repo_id, layer, convention_type, payload, "
            "confidence, source, model_count) "
            "VALUES (?, ?, ?, ?, 1.0, 'override', 0) "
            "ON CONFLICT (repo_id, layer, convention_type) "
            "DO UPDATE SET "
            "payload = EXCLUDED.payload, "
            "confidence = 1.0, "
            "source = 'override', "
            "model_count = 0",
            [
                self.repo_id,
                layer,
                convention_type,
                json.dumps(payload),
            ],
        )

    def detect_layers(self) -> list[Layer]:
        """Detect layers from directory structure.

        Handles both flat (staging/, marts/) and nested (models/staging/).
        If all files share a common prefix dir, strips it and uses the
        next segment.
        """
        rows = self.db._execute_read(
            """
            SELECT DISTINCT
                f.path,
                n.name
            FROM nodes n
            JOIN files f ON n.file_id = f.file_id
            WHERE n.kind IN ('table', 'view')
              AND f.repo_id = ?
            """,
            [self.repo_id],
        ).fetchall()

        if not rows:
            return []

        # Parse directory segments from file paths
        path_models: list[tuple[list[str], str]] = []
        for file_path, model_name in rows:
            parts = file_path.replace("\\", "/").split("/")
            # Remove the filename, keep directory segments
            dirs = parts[:-1]
            path_models.append((dirs, model_name))

        # Detect and strip common prefix directory
        dir_segments = [dirs for dirs, _ in path_models if dirs]
        if not dir_segments:
            return []

        prefix_len = self._common_prefix_length(dir_segments)

        # If prefix stripping would leave no layer segments for ALL models,
        # back off one level so the last prefix segment becomes the layer.
        all_at_prefix = all(
            len(dirs) <= prefix_len for dirs, _ in path_models if dirs
        )
        if all_at_prefix and prefix_len > 0:
            prefix_len -= 1

        # First pass: detect domain-nested structure by checking if
        # second-level dir names repeat across different first-level dirs.
        # E.g. finance/staging + marketing/staging → domain-nested.
        # But staging/by_source + staging/manual → NOT domain-nested.
        second_level_by_first: dict[str, set[str]] = {}
        for dirs, _ in path_models:
            remaining = dirs[prefix_len:]
            if len(remaining) >= 2:
                first, second = remaining[0], remaining[1]
                second_level_by_first.setdefault(first, set()).add(second)

        # Domain-nested if same second-level name appears under 2+ first-level dirs
        all_seconds: Counter[str] = Counter()
        for seconds in second_level_by_first.values():
            for s in seconds:
                all_seconds[s] += 1
        use_nested = any(count > 1 for count in all_seconds.values())

        # Group models by their layer directory
        layer_groups: dict[str, list[str]] = {}
        for dirs, model_name in path_models:
            if len(dirs) <= prefix_len:
                layer_key = ""
            else:
                remaining = dirs[prefix_len:]
                layer_key = remaining[0]

                # Use two-segment key only for domain-nested structures.
                # Note: models at depth 1 (e.g. finance/stg_flat.sql) keep
                # the flat key while depth 2+ get nested keys. This is
                # acceptable — mixed-depth within a domain dir is rare.
                if use_nested and len(remaining) > 1:
                    layer_key = "/".join(remaining[:2])

            if layer_key:
                layer_groups.setdefault(layer_key, []).append(model_name)

        # Collapse domain-nested layers if sub-layer names repeat
        # (e.g. finance/staging + marketing/staging → staging)
        layer_groups = self._collapse_domain_layers(layer_groups)

        # Build Layer objects, skip groups with < 2 models
        layers = []
        for name, models in sorted(
            layer_groups.items(), key=lambda x: len(x[1]), reverse=True
        ):
            if len(models) < 2:
                continue
            confidence = self._layer_confidence(len(models))
            prefix = "/".join(dir_segments[0][:prefix_len]) if dir_segments else ""
            path_pat = f"{prefix}/{name}/**" if prefix else f"{name}/**"
            layers.append(
                Layer(
                    name=name,
                    path_pattern=path_pat,
                    model_count=len(models),
                    model_names=sorted(models),
                    confidence=confidence,
                )
            )

        return layers

    def infer_naming_pattern(
        self, model_names: list[str]
    ) -> NamingPattern:
        """Infer naming pattern from a list of model names.

        Tokenizes by ``_``, finds common prefixes, classifies variable
        segments, and builds a pattern template.
        """
        if not model_names:
            return NamingPattern(
                pattern="",
                confidence=0.0,
                matching_count=0,
                total_count=0,
            )

        total = len(model_names)

        # Tokenize all names
        tokenized = [name.split("_") for name in model_names]

        # Find common prefix tokens
        prefix_tokens = self._find_common_prefix_tokens(tokenized)
        prefix = "_".join(prefix_tokens) + "_" if prefix_tokens else ""

        # Strip prefix and analyse remaining tokens
        stripped = []
        matching_prefix = 0
        exceptions = []
        for name, tokens in zip(model_names, tokenized, strict=False):
            if prefix_tokens and tokens[: len(prefix_tokens)] == prefix_tokens:
                stripped.append(tokens[len(prefix_tokens) :])
                matching_prefix += 1
            else:
                stripped.append(tokens)
                exceptions.append(name)

        # Classify variable segments by position
        if prefix_tokens:
            # With prefix: analyse segment structure after prefix
            segment_counts = Counter(len(s) for s in stripped if s)
            if segment_counts:
                most_common_len = segment_counts.most_common(1)[0][0]
            else:
                most_common_len = 0

            # Build pattern from prefix + variable segments
            var_labels = self._classify_segments(stripped, most_common_len)
            pattern = prefix + "_".join(f"{{{v}}}" for v in var_labels)
            confidence = matching_prefix / total if total > 0 else 0.0
        else:
            # No common prefix — try to find structural patterns
            segment_counts = Counter(len(tokens) for tokens in tokenized)
            if segment_counts:
                most_common_len = segment_counts.most_common(1)[0][0]
            else:
                most_common_len = 0

            var_labels = self._classify_segments(tokenized, most_common_len)
            pattern = "_".join(f"{{{v}}}" for v in var_labels)
            # Lower confidence without a clear prefix
            matching_structure = sum(
                1 for tokens in tokenized if len(tokens) == most_common_len
            )
            confidence = (
                matching_structure / total * 0.7 if total > 0 else 0.0
            )
            # Models not matching the dominant structure are exceptions
            exceptions = [
                name
                for name, tokens in zip(model_names, tokenized, strict=False)
                if len(tokens) != most_common_len
            ]

        # Cap confidence for small samples
        if total < 5:
            confidence = min(confidence, 0.6)

        return NamingPattern(
            pattern=pattern,
            confidence=round(confidence, 2),
            matching_count=total - len(exceptions),
            total_count=total,
            exceptions=sorted(exceptions),
        )

    def infer_reference_rules(
        self, layers: list[Layer]
    ) -> list[ReferenceRule]:
        """Infer layer-to-layer reference rules from edges.

        For each source layer, computes what percentage of references
        go to each target layer. High concentration → high confidence.
        """
        if not layers:
            return []

        # Build model → layer mapping
        model_layer = {}
        for layer in layers:
            for model in layer.model_names:
                model_layer[model] = layer.name

        # Query edges between table/view nodes in this repo.
        # Scope both source and target by repo_id (target may be a
        # phantom node with file_id IS NULL — include those too).
        rows = self.db._execute_read(
            """
            SELECT
                sn.name AS source_name,
                tn.name AS target_name
            FROM edges e
            JOIN nodes sn ON e.source_id = sn.node_id
            JOIN nodes tn ON e.target_id = tn.node_id
            JOIN files sf ON sn.file_id = sf.file_id
            LEFT JOIN files tf ON tn.file_id = tf.file_id
            WHERE sf.repo_id = ?
              AND (tf.repo_id = ? OR tn.file_id IS NULL)
              AND e.relationship IN ('references', 'cte_references')
            """,
            [self.repo_id, self.repo_id],
        ).fetchall()

        # Count edges per (source_layer, target_layer)
        edge_counts: dict[str, Counter[str]] = {}
        for src_name, tgt_name in rows:
            src_layer = model_layer.get(src_name)
            tgt_layer = model_layer.get(tgt_name)
            if src_layer is None:
                continue
            if tgt_layer is None:
                # Target not in any known layer — skip
                continue
            if src_layer == tgt_layer:
                # Skip within-layer references (e.g. CTE self-refs)
                continue
            edge_counts.setdefault(src_layer, Counter())[tgt_layer] += 1

        rules = []
        for layer in layers:
            counts = edge_counts.get(layer.name)
            if not counts:
                continue
            total = sum(counts.values())
            if total == 0:
                continue

            distribution = {
                tgt: round(count / total, 2)
                for tgt, count in counts.most_common()
            }
            # Dominant target = highest count
            _top_target, top_count = counts.most_common(1)[0]
            confidence = round(top_count / total, 2)

            allowed = [
                tgt for tgt, pct in distribution.items() if pct >= 0.1
            ]

            rules.append(
                ReferenceRule(
                    source_layer=layer.name,
                    allowed_targets=allowed,
                    target_distribution=distribution,
                    confidence=confidence,
                )
            )

        return rules

    def infer_common_columns(
        self,
        layer: Layer,
        threshold: float = 0.7,
    ) -> list[RequiredColumn]:
        """Detect columns appearing in >=threshold fraction of models.

        Merges two sources: ``columns`` table (definitions, more
        authoritative) and ``column_usage`` table (usage in SELECT).
        """
        if not layer.model_names:
            return []
        model_count = len(layer.model_names)
        if model_count == 0:
            return []

        # Columns from definitions (columns table)
        def_rows = self.db._execute_read(
            """
            SELECT
                c.column_name,
                COUNT(DISTINCT c.node_id) AS usage_count
            FROM columns c
            JOIN nodes n ON c.node_id = n.node_id
            JOIN files f ON n.file_id = f.file_id
            WHERE f.repo_id = ?
              AND n.name IN ({placeholders})
            GROUP BY c.column_name
            """.format(
                placeholders=",".join(["?"] * len(layer.model_names))
            ),
            [self.repo_id, *layer.model_names],
        ).fetchall()

        def_freq: dict[str, float] = {
            col: min(count / model_count, 1.0) for col, count in def_rows
        }

        # Columns from usage (column_usage table, SELECT only)
        usage_rows = self.db._execute_read(
            """
            SELECT
                cu.column_name,
                COUNT(DISTINCT cu.node_id) AS usage_count
            FROM column_usage cu
            JOIN nodes n ON cu.node_id = n.node_id
            JOIN files f ON n.file_id = f.file_id
            WHERE f.repo_id = ?
              AND n.name IN ({placeholders})
              AND cu.usage_type = 'select'
            GROUP BY cu.column_name
            """.format(
                placeholders=",".join(["?"] * len(layer.model_names))
            ),
            [self.repo_id, *layer.model_names],
        ).fetchall()

        usage_freq: dict[str, float] = {
            col: min(count / model_count, 1.0) for col, count in usage_rows
        }

        # Merge: definition is more authoritative
        all_cols = set(def_freq) | set(usage_freq)
        results = []
        for col in sorted(all_cols):
            d_freq = def_freq.get(col, 0.0)
            u_freq = usage_freq.get(col, 0.0)
            # Use the higher frequency, prefer definition source
            freq = max(d_freq, u_freq)
            if freq < threshold:
                continue

            if col in def_freq and col in usage_freq:
                source = "both"
            elif col in def_freq:
                source = "definition"
            else:
                source = "usage"

            # Find models missing this column
            missing = self._find_models_missing_column(
                layer.model_names, col
            )

            results.append(
                RequiredColumn(
                    column_name=col,
                    frequency=round(freq, 2),
                    source=source,
                    missing_in=missing,
                )
            )

        return results

    def detect_column_style(
        self, layer: Layer
    ) -> ColumnStyle:
        """Classify dominant column naming convention in a layer.

        Checks: snake_case, camelCase, PascalCase, SCREAMING_SNAKE.
        """
        if not layer.model_names:
            return ColumnStyle(style="snake_case", confidence=0.0)

        # Get column names from models in this layer
        rows = self.db._execute_read(
            """
            SELECT DISTINCT c.column_name
            FROM columns c
            JOIN nodes n ON c.node_id = n.node_id
            JOIN files f ON n.file_id = f.file_id
            WHERE f.repo_id = ?
              AND n.name IN ({placeholders})
            """.format(
                placeholders=",".join(["?"] * len(layer.model_names))
            ),
            [self.repo_id, *layer.model_names],
        ).fetchall()

        column_names = [r[0] for r in rows]
        if not column_names:
            # Fall back to column_usage
            usage_rows = self.db._execute_read(
                """
                SELECT DISTINCT cu.column_name
                FROM column_usage cu
                JOIN nodes n ON cu.node_id = n.node_id
                JOIN files f ON n.file_id = f.file_id
                WHERE f.repo_id = ?
                  AND n.name IN ({placeholders})
                """.format(
                    placeholders=",".join(
                        ["?"] * len(layer.model_names)
                    )
                ),
                [self.repo_id, *layer.model_names],
            ).fetchall()
            column_names = [r[0] for r in usage_rows]

        return self._classify_column_style(column_names)

    def _find_models_missing_column(
        self, model_names: list[str], column_name: str
    ) -> list[str]:
        """Find models in a layer that don't have a given column."""
        if not model_names:
            return []
        placeholders = ",".join(["?"] * len(model_names))
        rows = self.db._execute_read(
            f"""
            SELECT DISTINCT n.name
            FROM columns c
            JOIN nodes n ON c.node_id = n.node_id
            JOIN files f ON n.file_id = f.file_id
            WHERE f.repo_id = ?
              AND n.name IN ({placeholders})
              AND c.column_name = ?
            UNION
            SELECT DISTINCT n.name
            FROM column_usage cu
            JOIN nodes n ON cu.node_id = n.node_id
            JOIN files f ON n.file_id = f.file_id
            WHERE f.repo_id = ?
              AND n.name IN ({placeholders})
              AND cu.column_name = ?
            """,
            [
                self.repo_id, *model_names, column_name,
                self.repo_id, *model_names, column_name,
            ],
        ).fetchall()

        models_with_col = {r[0] for r in rows}
        return sorted(
            name for name in model_names if name not in models_with_col
        )

    @staticmethod
    def _classify_column_style(
        column_names: list[str],
    ) -> ColumnStyle:
        """Classify column naming style from a list of column names."""
        if not column_names:
            return ColumnStyle(style="snake_case", confidence=0.0)

        counts: dict[str, int] = {
            "snake_case": 0,
            "camelCase": 0,
            "PascalCase": 0,
            "SCREAMING_SNAKE": 0,
        }
        neutral = 0  # single-word lowercase — ambiguous style

        for name in column_names:
            if not name:
                neutral += 1
                continue
            if name == name.upper() and "_" in name:
                counts["SCREAMING_SNAKE"] += 1
            elif name == name.lower() and "_" in name:
                counts["snake_case"] += 1
            elif name[0].islower() and any(c.isupper() for c in name):
                counts["camelCase"] += 1
            elif name[0].isupper() and any(c.islower() for c in name):
                counts["PascalCase"] += 1
            else:
                # Single-word lowercase (e.g. "id", "name") — style-ambiguous
                neutral += 1

        classified = len(column_names) - neutral
        if classified == 0:
            return ColumnStyle(style="snake_case", confidence=0.0)

        dominant = max(counts, key=lambda k: counts[k])
        confidence = counts[dominant] / classified
        return ColumnStyle(
            style=dominant, confidence=round(confidence, 2)
        )

    # ── Private helpers ──

    @staticmethod
    def _common_prefix_length(dir_lists: list[list[str]]) -> int:
        """Find the common directory prefix length across all paths.

        Returns 0 if paths share no common prefix, or the number of
        shared leading directory segments.
        """
        if not dir_lists:
            return 0

        min_len = min(len(d) for d in dir_lists)
        prefix_len = 0
        for i in range(min_len):
            vals = {d[i] for d in dir_lists}
            if len(vals) == 1:
                prefix_len += 1
            else:
                break
        return prefix_len

    @staticmethod
    def _collapse_domain_layers(
        layer_groups: dict[str, list[str]],
    ) -> dict[str, list[str]]:
        """Collapse domain-nested layers if sub-layers repeat.

        If keys look like ``finance/staging``, ``marketing/staging``,
        merge into a single ``staging`` group.
        """
        nested_keys = [k for k in layer_groups if "/" in k]
        if not nested_keys:
            return layer_groups

        # Extract sub-layer names (second part after /)
        sub_layers: dict[str, list[str]] = {}
        for key in nested_keys:
            _, sub = key.split("/", 1)
            sub_layers.setdefault(sub, []).extend(layer_groups[key])

        # Only collapse if sub-layer names repeat across domains
        if len(nested_keys) > len(sub_layers):
            # Sub-layers repeat — collapse
            result = {
                k: v for k, v in layer_groups.items() if "/" not in k
            }
            for sub, models in sub_layers.items():
                result.setdefault(sub, []).extend(models)
            return result

        return layer_groups

    @staticmethod
    def _layer_confidence(model_count: int) -> float:
        """Compute confidence based on model count."""
        if model_count >= 10:
            return 0.9
        if model_count >= 5:
            return 0.8
        return 0.6

    @staticmethod
    def _find_common_prefix_tokens(
        tokenized: list[list[str]],
    ) -> list[str]:
        """Find prefix tokens shared by >=70% of names."""
        if not tokenized:
            return []

        total = len(tokenized)
        threshold = 0.7

        # Check first token frequency
        first_tokens = Counter(
            tokens[0] for tokens in tokenized if tokens
        )
        if not first_tokens:
            return []

        most_common_token, count = first_tokens.most_common(1)[0]
        if count / total >= threshold:
            return [most_common_token]

        return []

    @staticmethod
    def _classify_segments(
        stripped_tokens: list[list[str]],
        expected_len: int,
    ) -> list[str]:
        """Classify variable segments by position into semantic labels.

        Uses heuristics: known source system names → 'source',
        short tokens → 'description', otherwise 'entity'.
        """
        if expected_len <= 0:
            return ["description"]

        # Collect tokens by position
        by_position: list[Counter[str]] = [
            Counter() for _ in range(expected_len)
        ]
        for tokens in stripped_tokens:
            for i, tok in enumerate(tokens[:expected_len]):
                by_position[i][tok] += 1

        labels = []
        for i, counter in enumerate(by_position):
            distinct = len(counter)
            total = sum(counter.values())
            if total == 0:
                labels.append("description")
                continue

            # High cardinality relative to total → entity-like
            # Low cardinality → source/category-like
            ratio = distinct / total
            if ratio < 0.3 and distinct <= 5:
                labels.append("source")
            elif i == expected_len - 1:
                labels.append("entity")
            else:
                labels.append("domain" if i == 0 else "description")

        return labels if labels else ["description"]

    # ── Semantic tag clustering ──

    # Prefixes and dot-delimited segments to strip before tokenizing
    # model names for label extraction.
    _LAYER_PREFIXES = re.compile(
        r"^(stg|int|fct|dim|raw|src|base|snap|rpt|agg)_"
    )
    _LAYER_DOT_PREFIXES = re.compile(
        r"^(marts|staging|intermediate|raw|sources)\."
    )

    def infer_semantic_tags(
        self,
        threshold: float = 0.5,
        existing_tags: list[TagAssignment] | None = None,
    ) -> list[TagAssignment]:
        """Cluster models by shared upstream references and auto-label.

        Pipeline:
        1. Query upstream refs per model.
        2. Agglomerative clustering by Jaccard similarity.
        3. Auto-label each cluster from most frequent name token.
        4. Score per-model confidence.

        Args:
            threshold: Jaccard similarity threshold for merging clusters.
            existing_tags: Previously assigned tags for stability check.

        Returns:
            List of ``TagAssignment`` objects, one per model in a cluster.
        """
        ref_sets = self._get_model_references()
        if len(ref_sets) < 5:
            logger.debug(
                "Skipping semantic tags: repo %d has < 5 models with refs",
                self.repo_id,
            )
            return []

        clusters = self._agglomerative_cluster(ref_sets, threshold)

        # Build existing tag lookup for stability
        existing_by_node: dict[int, TagAssignment] = {}
        if existing_tags:
            for tag in existing_tags:
                existing_by_node[tag.node_id] = tag

        assignments: list[TagAssignment] = []
        for cluster in clusters:
            tag_name, _label_confidence = self._label_cluster(cluster)
            if not tag_name:
                continue

            for node_id, model_name in cluster.members:
                confidence = self._compute_member_confidence(
                    node_id, model_name, tag_name, cluster,
                )

                # Stability: keep existing tag if still above threshold
                prev = existing_by_node.get(node_id)
                if prev and prev.tag_name != tag_name:
                    # Check if the model still fits the old cluster
                    old_still_valid = self._check_tag_stability(
                        node_id, prev.tag_name, clusters, ref_sets, threshold,
                    )
                    if old_still_valid:
                        assignments.append(prev)
                        continue

                assignments.append(
                    TagAssignment(
                        tag_name=tag_name,
                        node_id=node_id,
                        model_name=model_name,
                        confidence=round(min(confidence, 1.0), 2),
                        source="inferred",
                    )
                )

        return assignments

    def _get_model_references(self) -> dict[int, set[int]]:
        """Query upstream references for each table/view in the repo.

        For directly-inserted edges (test helpers, dbt/sqlmesh), tables
        have ``references`` edges directly.  For parsed SQL, a ``query``
        node holds the ``references`` edge and a separate ``defines``
        edge points to the table.  This query handles both patterns by
        unioning direct table refs with query-mediated refs.

        Returns:
            Dict mapping table/view node_id to set of upstream node_ids.
        """
        # 1) Direct: table/view → references → target
        rows_direct = self.db._execute_read(
            """
            SELECT e.source_id, e.target_id
            FROM edges e
            JOIN nodes sn ON e.source_id = sn.node_id
            JOIN files sf ON sn.file_id = sf.file_id
            WHERE sf.repo_id = ?
              AND e.relationship IN ('references', 'cte_references')
              AND sn.kind IN ('table', 'view')
            """,
            [self.repo_id],
        ).fetchall()

        # 2) Query-mediated: query → defines → table, query → references → target
        #    Map each table to the refs of the query that defines it.
        rows_query = self.db._execute_read(
            """
            SELECT def_e.target_id AS table_id, ref_e.target_id AS ref_id
            FROM edges def_e
            JOIN edges ref_e ON def_e.source_id = ref_e.source_id
            JOIN nodes qn ON def_e.source_id = qn.node_id
            JOIN nodes tn ON def_e.target_id = tn.node_id
            JOIN files tf ON tn.file_id = tf.file_id
            WHERE tf.repo_id = ?
              AND def_e.relationship = 'defines'
              AND ref_e.relationship IN ('references', 'cte_references')
              AND qn.kind = 'query'
              AND tn.kind IN ('table', 'view')
            """,
            [self.repo_id],
        ).fetchall()

        ref_sets: dict[int, set[int]] = {}
        for source_id, target_id in rows_direct:
            ref_sets.setdefault(source_id, set()).add(target_id)
        for table_id, ref_id in rows_query:
            ref_sets.setdefault(table_id, set()).add(ref_id)

        return ref_sets

    @staticmethod
    def _jaccard(a: set, b: set) -> float:
        """Compute Jaccard similarity between two sets."""
        if not a and not b:
            return 0.0
        return len(a & b) / len(a | b)

    def _agglomerative_cluster(
        self,
        ref_sets: dict[int, set[int]],
        threshold: float = 0.5,
    ) -> list[Cluster]:
        """Agglomerative clustering of models by Jaccard similarity.

        Starts with each model as its own cluster and iteratively
        merges the most similar pair until no pair exceeds the
        threshold.

        Args:
            ref_sets: Mapping of node_id to upstream reference set.
            threshold: Minimum Jaccard similarity to merge.

        Returns:
            List of ``Cluster`` objects.
        """
        # Get model names for cluster members
        node_ids = list(ref_sets)
        if not node_ids:
            return []

        if len(node_ids) > 500:
            logger.warning(
                "Skipping clustering: %d models exceeds 500-model limit "
                "(O(n³) cost). Consider filtering by layer.",
                len(node_ids),
            )
            return []

        placeholders = ",".join(["?"] * len(node_ids))
        rows = self.db._execute_read(
            f"""
            SELECT node_id, name
            FROM nodes
            WHERE node_id IN ({placeholders})
            """,
            node_ids,
        ).fetchall()
        name_map = {nid: name for nid, name in rows}

        # Initialize: each node is its own cluster
        # Use list index as cluster id
        clusters: list[list[int]] = [[nid] for nid in node_ids]
        # Precompute merged ref sets per cluster (union of member refs)
        cluster_refs: list[set[int]] = [
            ref_sets[nid].copy() for nid in node_ids
        ]

        while True:
            best_sim = -1.0
            best_i = -1
            best_j = -1

            for i in range(len(clusters)):
                if not clusters[i]:
                    continue
                for j in range(i + 1, len(clusters)):
                    if not clusters[j]:
                        continue
                    sim = self._jaccard(cluster_refs[i], cluster_refs[j])
                    if sim > best_sim:
                        best_sim = sim
                        best_i = i
                        best_j = j

            if best_sim < threshold:
                break

            # Merge j into i
            clusters[best_i].extend(clusters[best_j])
            cluster_refs[best_i] = cluster_refs[best_i] | cluster_refs[best_j]
            clusters[best_j] = []
            cluster_refs[best_j] = set()

        # Build Cluster objects, skip singletons
        result: list[Cluster] = []
        for members_list in clusters:
            if len(members_list) < 2:
                continue
            members = [
                (nid, name_map.get(nid, f"node_{nid}"))
                for nid in members_list
            ]
            c_ref_sets = {
                nid: ref_sets[nid] for nid in members_list
            }
            result.append(Cluster(members=members, ref_sets=c_ref_sets))

        return result

    def _label_cluster(
        self, cluster: Cluster
    ) -> tuple[str, float]:
        """Auto-label a cluster from most frequent name token.

        Strips layer prefixes, tokenizes by ``_``, and picks the
        most common non-trivial token as the tag name.

        Returns:
            Tuple of (tag_name, label_confidence). Empty tag_name
            if no suitable label found.
        """
        token_counter: Counter[str] = Counter()
        trivial = {"id", "at", "by", "to", "is", "on", "in", "of", "no"}

        for _node_id, model_name in cluster.members:
            stripped = self._strip_layer_prefix(model_name)
            tokens = stripped.split("_")
            # Deduplicate tokens within a single model name
            seen: set[str] = set()
            for tok in tokens:
                tok_lower = tok.lower()
                if tok_lower and tok_lower not in trivial and tok_lower not in seen:
                    seen.add(tok_lower)
                    token_counter[tok_lower] += 1

        if not token_counter:
            return ("", 0.0)

        tag_name, count = token_counter.most_common(1)[0]
        label_confidence = count / len(cluster.members)
        return (tag_name, round(label_confidence, 2))

    def _strip_layer_prefix(self, model_name: str) -> str:
        """Strip known layer prefixes from a model name.

        Handles both underscore prefixes (``stg_``, ``fct_``) and
        dot-delimited prefixes (``marts.``, ``staging.``).
        """
        # Strip dot-delimited prefix first
        name = self._LAYER_DOT_PREFIXES.sub("", model_name)
        # Then strip underscore prefix
        name = self._LAYER_PREFIXES.sub("", name)
        return name

    def _compute_member_confidence(
        self,
        node_id: int,
        model_name: str,
        tag_name: str,
        cluster: Cluster,
    ) -> float:
        """Compute per-model confidence within a tagged cluster.

        Confidence is based on:
        - Position in cluster (core vs edge member): 0.60 - 0.85 base
        - Name token match: +0.10
        - Description match: +0.10
        """
        # Compute average Jaccard similarity to other cluster members
        own_refs = cluster.ref_sets.get(node_id, set())
        other_sims = []
        for other_id, _name in cluster.members:
            if other_id == node_id:
                continue
            other_refs = cluster.ref_sets.get(other_id, set())
            other_sims.append(self._jaccard(own_refs, other_refs))

        if other_sims:
            avg_sim = sum(other_sims) / len(other_sims)
        else:
            avg_sim = 0.0

        # Linear interpolation between edge (0.60) and core (0.85)
        # based on avg similarity. Threshold 0.5 maps to ~0.60,
        # similarity 1.0 maps to ~0.85.
        base = 0.60 + (avg_sim - 0.5) * (0.85 - 0.60) / 0.5
        base = max(0.60, min(0.85, base))

        # Name token match bonus
        stripped = self._strip_layer_prefix(model_name)
        if tag_name.lower() in stripped.lower():
            base += 0.10

        # Description match bonus (check if model has a description
        # containing the tag name)
        base += self._description_bonus(node_id, tag_name)

        return min(base, 1.0)

    def _description_bonus(
        self, node_id: int, tag_name: str
    ) -> float:
        """Check if a model's column descriptions mention the tag name.

        Returns 0.10 if any column description contains the tag name,
        0.0 otherwise.  Queries the ``columns`` table which carries
        ``description`` from dbt/sqlmesh schema YAML.
        """
        rows = self.db._execute_read(
            """
            SELECT description
            FROM columns
            WHERE node_id = ?
              AND description IS NOT NULL
              AND description != ''
            """,
            [node_id],
        ).fetchall()

        if not rows:
            return 0.0

        tag_lower = tag_name.lower()
        for (desc,) in rows:
            if isinstance(desc, str) and tag_lower in desc.lower():
                return 0.10
        return 0.0

    def _check_tag_stability(
        self,
        node_id: int,
        old_tag: str,
        clusters: list[Cluster],
        ref_sets: dict[int, set[int]],
        threshold: float,
    ) -> bool:
        """Check if a model still fits its old tag cluster.

        For stability, an existing tag is preserved if the model
        has Jaccard similarity >= threshold with at least one member
        of a cluster labeled with the old tag.
        """
        own_refs = ref_sets.get(node_id, set())
        if not own_refs:
            return False

        for cluster in clusters:
            tag_name, _ = self._label_cluster(cluster)
            if tag_name != old_tag:
                continue
            # Check if node has sufficient similarity to any member
            for member_id, _ in cluster.members:
                if member_id == node_id:
                    continue
                member_refs = cluster.ref_sets.get(member_id, set())
                if self._jaccard(own_refs, member_refs) >= threshold:
                    return True

        return False
