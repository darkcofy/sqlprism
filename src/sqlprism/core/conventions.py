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

import logging
from collections import Counter
from dataclasses import dataclass, field

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


class ConventionEngine:
    """Infers project conventions from the knowledge graph.

    Takes a ``GraphDB`` instance and repo_id. Call ``detect_layers()``
    and ``infer_naming_pattern()`` to analyse the graph.
    """

    def __init__(self, db: GraphDB, repo_id: int) -> None:
        self.db = db
        self.repo_id = repo_id

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
        for name, tokens in zip(model_names, tokenized):
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
                for name, tokens in zip(model_names, tokenized)
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
