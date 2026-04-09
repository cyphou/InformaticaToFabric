"""
Migration Review Module (DD11)
==============================
Provides merge, optimization, and rework capabilities for migration review:
  - DuplicateDetector — fingerprint matching for duplicate artifacts
  - SimilarityScorer — Jaccard-based similarity scoring
  - AntiPatternDetector — detects 10 common migration anti-patterns
  - ConsolidationRecommender — recommends merge opportunities
  - SQLOptimizer — basic SQL optimization suggestions
  - ReworkClassifier — classifies items needing rework by severity
  - ReviewQueueManager — manages the review queue with priorities

All features work locally with no external dependencies.
"""

import hashlib
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

_logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Configuration loader
# ─────────────────────────────────────────────

def load_review_config(config):
    """Extract review config from migration.yaml dict."""
    idmc = (config or {}).get("idmc", {})
    review = idmc.get("review", {})
    return {
        "merge_threshold": review.get("merge_threshold", 0.8),
        "auto_apply": review.get("auto_apply", False),
        "anti_patterns": review.get("anti_patterns", True),
        "sql_optimization": review.get("sql_optimization", True),
    }


# ─────────────────────────────────────────────
#  Duplicate Detection
# ─────────────────────────────────────────────

class DuplicateDetector:
    """Detects duplicate or near-duplicate migration artifacts using fingerprinting."""

    def __init__(self, threshold=0.85):
        self.threshold = threshold
        self.fingerprints = {}

    def fingerprint(self, content, artifact_type="generic"):
        """Generate a fingerprint for content."""
        normalized = self._normalize(content)
        fp = hashlib.sha256(normalized.encode()).hexdigest()
        return fp

    def fingerprint_file(self, file_path):
        """Fingerprint a file and store it."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        except (OSError, UnicodeDecodeError):
            return None
        fp = self.fingerprint(content)
        self.fingerprints[file_path] = {
            "fingerprint": fp,
            "size": len(content),
            "lines": content.count("\n") + 1,
        }
        return fp

    def find_duplicates(self, files=None, directory=None):
        """Find duplicate files.

        Args:
            files: list of file paths
            directory: directory to scan

        Returns:
            list of duplicate groups
        """
        if directory:
            files = []
            for root, _, filenames in os.walk(directory):
                for fn in filenames:
                    if fn.endswith((".py", ".sql", ".json")):
                        files.append(os.path.join(root, fn))

        if not files:
            return []

        # Fingerprint all files
        for f in files:
            self.fingerprint_file(f)

        # Group by fingerprint
        groups = defaultdict(list)
        for path, info in self.fingerprints.items():
            groups[info["fingerprint"]].append(path)

        duplicates = []
        for fp, paths in groups.items():
            if len(paths) > 1:
                duplicates.append({
                    "fingerprint": fp,
                    "files": paths,
                    "count": len(paths),
                })

        return duplicates

    def _normalize(self, content):
        """Normalize content for comparison (strip whitespace, comments)."""
        # Remove single-line comments
        content = re.sub(r"#.*$", "", content, flags=re.MULTILINE)
        content = re.sub(r"--.*$", "", content, flags=re.MULTILINE)
        # Remove multi-line comments
        content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
        # Normalize whitespace
        content = re.sub(r"\s+", " ", content).strip()
        return content.lower()


# ─────────────────────────────────────────────
#  Similarity Scoring
# ─────────────────────────────────────────────

class SimilarityScorer:
    """Jaccard-based similarity scorer for migration artifacts."""

    def score(self, content_a, content_b):
        """Compute Jaccard similarity between two content strings.

        Returns:
            float between 0 and 1
        """
        tokens_a = self._tokenize(content_a)
        tokens_b = self._tokenize(content_b)

        if not tokens_a and not tokens_b:
            return 1.0
        if not tokens_a or not tokens_b:
            return 0.0

        intersection = tokens_a & tokens_b
        union = tokens_a | tokens_b

        return len(intersection) / len(union)

    def score_files(self, path_a, path_b):
        """Score similarity between two files."""
        try:
            with open(path_a, "r", encoding="utf-8") as f:
                content_a = f.read()
            with open(path_b, "r", encoding="utf-8") as f:
                content_b = f.read()
        except (OSError, UnicodeDecodeError):
            return 0.0
        return self.score(content_a, content_b)

    def find_similar_pairs(self, files, threshold=0.7):
        """Find pairs of similar files above threshold.

        Returns:
            list of (path_a, path_b, score)
        """
        pairs = []
        for i, f1 in enumerate(files):
            for f2 in files[i + 1:]:
                score = self.score_files(f1, f2)
                if score >= threshold:
                    pairs.append((f1, f2, round(score, 3)))
        return sorted(pairs, key=lambda x: x[2], reverse=True)

    def _tokenize(self, content):
        """Tokenize content into a set of normalized tokens."""
        # Remove comments
        content = re.sub(r"#.*$", "", content, flags=re.MULTILINE)
        content = re.sub(r"--.*$", "", content, flags=re.MULTILINE)
        # Split into tokens
        tokens = re.findall(r"[a-zA-Z_]\w*", content)
        return set(t.lower() for t in tokens)


# ─────────────────────────────────────────────
#  Anti-Pattern Detection (10 patterns)
# ─────────────────────────────────────────────

ANTI_PATTERNS = [
    {
        "id": "AP001",
        "name": "Hardcoded Connection String",
        "severity": "high",
        "pattern": r"""(?:jdbc:|Server=|Data Source=|host=)[^\s'"]+""",
        "message": "Hardcoded connection string found. Use parameterized connections.",
    },
    {
        "id": "AP002",
        "name": "SELECT * Usage",
        "severity": "medium",
        "pattern": r"SELECT\s+\*\s+FROM",
        "message": "SELECT * found. Specify columns explicitly for performance.",
    },
    {
        "id": "AP003",
        "name": "Missing Error Handling",
        "severity": "medium",
        "pattern": r"(?:\.write\.|\.save\(|\.insertInto\()(?!.*(?:try|except|catch|error))",
        "message": "Write operation without visible error handling.",
    },
    {
        "id": "AP004",
        "name": "Cross-Partition Query",
        "severity": "high",
        "pattern": r"(?:fan.?out|cross.?partition|scatter.?gather)",
        "message": "Potential cross-partition query pattern detected.",
    },
    {
        "id": "AP005",
        "name": "Unbounded Collect",
        "severity": "critical",
        "pattern": r"\.collect\(\)(?!\s*\[:\d+\])",
        "message": "Unbounded .collect() call — can cause OOM on large datasets.",
    },
    {
        "id": "AP006",
        "name": "Hardcoded Credentials",
        "severity": "critical",
        "pattern": r"""(?:password|secret|api.?key|token)\s*[=:]\s*['"][^'"]{4,}""",
        "message": "Potential hardcoded credentials. Use Key Vault or env vars.",
    },
    {
        "id": "AP007",
        "name": "Missing Partition Pruning",
        "severity": "medium",
        "pattern": r"(?:\.read|spark\.sql).*(?:full.?scan|without.?partition)",
        "message": "Read without partition pruning — may scan entire dataset.",
    },
    {
        "id": "AP008",
        "name": "Redundant Transformation",
        "severity": "low",
        "pattern": r"\.withColumn\([^)]+\).*\.withColumn\([^)]+\).*\.withColumn\(",
        "message": "Multiple chained withColumn calls — consider using select().",
    },
    {
        "id": "AP009",
        "name": "Missing Schema Validation",
        "severity": "medium",
        "pattern": r"\.read\.format\(.*\)\.load\((?!.*schema)",
        "message": "Reading data without schema specification.",
    },
    {
        "id": "AP010",
        "name": "Synchronous API Call in Loop",
        "severity": "high",
        "pattern": r"for\s+\w+\s+in\s+.*:.*(?:requests\.get|urlopen|api_call)",
        "message": "Synchronous API call inside a loop — consider batch API.",
    },
]


class AntiPatternDetector:
    """Detects common migration anti-patterns in generated artifacts."""

    def __init__(self, patterns=None):
        self.patterns = patterns or ANTI_PATTERNS

    def scan_content(self, content, source="unknown"):
        """Scan content for anti-patterns.

        Returns:
            list of detected anti-patterns
        """
        findings = []
        for ap in self.patterns:
            matches = list(re.finditer(ap["pattern"], content, re.IGNORECASE))
            if matches:
                for match in matches:
                    line_num = content[:match.start()].count("\n") + 1
                    findings.append({
                        "id": ap["id"],
                        "name": ap["name"],
                        "severity": ap["severity"],
                        "message": ap["message"],
                        "source": source,
                        "line": line_num,
                        "match": match.group()[:100],
                    })
        return findings

    def scan_file(self, file_path):
        """Scan a file for anti-patterns."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        except (OSError, UnicodeDecodeError):
            return []
        return self.scan_content(content, source=file_path)

    def scan_directory(self, directory, extensions=(".py", ".sql", ".json")):
        """Scan all files in a directory for anti-patterns.

        Returns:
            list of all findings + summary
        """
        all_findings = []
        for root, _, filenames in os.walk(directory):
            for fn in filenames:
                if any(fn.endswith(ext) for ext in extensions):
                    path = os.path.join(root, fn)
                    findings = self.scan_file(path)
                    all_findings.extend(findings)

        summary = defaultdict(int)
        for f in all_findings:
            summary[f["severity"]] += 1

        return {
            "findings": all_findings,
            "total": len(all_findings),
            "by_severity": dict(summary),
        }


# ─────────────────────────────────────────────
#  Consolidation Recommender
# ─────────────────────────────────────────────

class ConsolidationRecommender:
    """Recommends merge opportunities for similar artifacts."""

    def __init__(self, threshold=0.8):
        self.threshold = threshold
        self.scorer = SimilarityScorer()
        self.detector = DuplicateDetector()

    def analyze(self, output_dir):
        """Analyze output directory for consolidation opportunities.

        Returns:
            dict with duplicates, similar pairs, and recommendations
        """
        # Find all artifacts
        artifacts = []
        for root, _, files in os.walk(output_dir):
            for fn in files:
                if fn.endswith((".py", ".sql", ".json")):
                    artifacts.append(os.path.join(root, fn))

        # Find exact duplicates
        duplicates = self.detector.find_duplicates(files=artifacts)

        # Find similar pairs
        similar = self.scorer.find_similar_pairs(artifacts, threshold=self.threshold)

        # Generate recommendations
        recommendations = []
        for dup in duplicates:
            recommendations.append({
                "type": "merge",
                "action": "Merge identical files into one shared artifact",
                "files": dup["files"],
                "confidence": 1.0,
            })
        for path_a, path_b, score in similar:
            if score < 1.0:  # Not exact duplicate
                recommendations.append({
                    "type": "review",
                    "action": f"Review similar files (similarity: {score})",
                    "files": [path_a, path_b],
                    "confidence": score,
                })

        return {
            "duplicates": duplicates,
            "similar_pairs": [(a, b, s) for a, b, s in similar],
            "recommendations": recommendations,
            "merge_candidates": len(duplicates),
            "review_candidates": len([s for s in similar if s[2] < 1.0]),
        }


# ─────────────────────────────────────────────
#  SQL Optimizer
# ─────────────────────────────────────────────

class SQLOptimizer:
    """Basic SQL optimization suggestions for migrated SQL."""

    RULES = [
        {
            "id": "OPT001",
            "pattern": r"SELECT\s+\*",
            "suggestion": "Replace SELECT * with explicit column list",
            "category": "performance",
        },
        {
            "id": "OPT002",
            "pattern": r"(?:NOT\s+IN|<>\s*ALL)\s*\(",
            "suggestion": "Replace NOT IN with NOT EXISTS for nullable columns",
            "category": "correctness",
        },
        {
            "id": "OPT003",
            "pattern": r"UNION\s+ALL.*UNION\s+ALL",
            "suggestion": "Consider using a single source with CASE expressions",
            "category": "performance",
        },
        {
            "id": "OPT004",
            "pattern": r"ORDER\s+BY.*(?:LIMIT|FETCH\s+FIRST)",
            "suggestion": "Ensure ORDER BY columns are indexed for Top-N queries",
            "category": "performance",
        },
        {
            "id": "OPT005",
            "pattern": r"(?:DISTINCT|GROUP\s+BY).*(?:DISTINCT|GROUP\s+BY)",
            "suggestion": "Redundant DISTINCT/GROUP BY — simplify the query",
            "category": "performance",
        },
        {
            "id": "OPT006",
            "pattern": r"LIKE\s+'%[^']+%'",
            "suggestion": "Leading wildcard prevents index usage — consider full-text search",
            "category": "performance",
        },
    ]

    def analyze_sql(self, sql_content, source="unknown"):
        """Analyze SQL content for optimization opportunities."""
        suggestions = []
        for rule in self.RULES:
            if re.search(rule["pattern"], sql_content, re.IGNORECASE | re.DOTALL):
                suggestions.append({
                    "rule_id": rule["id"],
                    "suggestion": rule["suggestion"],
                    "category": rule["category"],
                    "source": source,
                })
        return suggestions

    def analyze_directory(self, directory):
        """Analyze all SQL files in a directory."""
        all_suggestions = []
        for root, _, files in os.walk(directory):
            for fn in files:
                if fn.endswith(".sql"):
                    path = os.path.join(root, fn)
                    try:
                        with open(path, "r", encoding="utf-8") as f:
                            content = f.read()
                        suggestions = self.analyze_sql(content, source=path)
                        all_suggestions.extend(suggestions)
                    except (OSError, UnicodeDecodeError):
                        pass
        return all_suggestions


# ─────────────────────────────────────────────
#  Rework Classifier
# ─────────────────────────────────────────────

class ReworkClassifier:
    """Classifies migration items that need rework."""

    REWORK_INDICATORS = [
        {"marker": "TODO", "severity": "medium", "message": "TODO marker found"},
        {"marker": "FIXME", "severity": "high", "message": "FIXME marker found"},
        {"marker": "HACK", "severity": "high", "message": "HACK marker found"},
        {"marker": "XXX", "severity": "medium", "message": "XXX marker found"},
        {"marker": "MANUAL REVIEW", "severity": "high", "message": "Manual review required"},
        {"marker": "NOT SUPPORTED", "severity": "critical", "message": "Unsupported transformation"},
        {"marker": "PLACEHOLDER", "severity": "medium", "message": "Placeholder code found"},
    ]

    def classify_content(self, content, source="unknown"):
        """Classify content for rework items."""
        items = []
        lines = content.split("\n")
        for i, line in enumerate(lines, 1):
            for indicator in self.REWORK_INDICATORS:
                if indicator["marker"].lower() in line.lower():
                    items.append({
                        "source": source,
                        "line": i,
                        "marker": indicator["marker"],
                        "severity": indicator["severity"],
                        "message": indicator["message"],
                        "context": line.strip()[:200],
                    })
        return items

    def classify_directory(self, directory, extensions=(".py", ".sql")):
        """Classify all files in a directory for rework."""
        all_items = []
        for root, _, files in os.walk(directory):
            for fn in files:
                if any(fn.endswith(ext) for ext in extensions):
                    path = os.path.join(root, fn)
                    try:
                        with open(path, "r", encoding="utf-8") as f:
                            content = f.read()
                        items = self.classify_content(content, source=path)
                        all_items.extend(items)
                    except (OSError, UnicodeDecodeError):
                        pass

        by_severity = defaultdict(int)
        for item in all_items:
            by_severity[item["severity"]] += 1

        return {
            "items": all_items,
            "total": len(all_items),
            "by_severity": dict(by_severity),
        }


# ─────────────────────────────────────────────
#  Review Queue Manager
# ─────────────────────────────────────────────

class ReviewQueueManager:
    """Manages the migration review queue with priorities."""

    PRIORITY_MAP = {"critical": 0, "high": 1, "medium": 2, "low": 3}

    def __init__(self, config=None):
        self.config = load_review_config(config)
        self.queue = []
        self._counter = 0

    def add_item(self, title, category, severity, details=None, source=None):
        """Add an item to the review queue."""
        self._counter += 1
        item = {
            "id": f"REV-{self._counter:04d}",
            "title": title,
            "category": category,  # merge, optimize, rework, anti-pattern
            "severity": severity,
            "priority": self.PRIORITY_MAP.get(severity, 3),
            "status": "pending",  # pending, in_review, approved, rejected, applied
            "details": details or {},
            "source": source,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.queue.append(item)
        return item

    def get_queue(self, category=None, status=None, sort_by_priority=True):
        """Get review queue items, optionally filtered."""
        items = list(self.queue)
        if category:
            items = [i for i in items if i["category"] == category]
        if status:
            items = [i for i in items if i["status"] == status]
        if sort_by_priority:
            items.sort(key=lambda x: x["priority"])
        return items

    def update_status(self, item_id, status, notes=""):
        """Update the status of a review item."""
        for item in self.queue:
            if item["id"] == item_id:
                item["status"] = status
                item["notes"] = notes
                item["updated_at"] = datetime.now(timezone.utc).isoformat()
                return item
        return None

    def get_summary(self):
        """Get review queue summary."""
        by_category = defaultdict(int)
        by_status = defaultdict(int)
        by_severity = defaultdict(int)
        for item in self.queue:
            by_category[item["category"]] += 1
            by_status[item["status"]] += 1
            by_severity[item["severity"]] += 1
        return {
            "total": len(self.queue),
            "by_category": dict(by_category),
            "by_status": dict(by_status),
            "by_severity": dict(by_severity),
        }


# ─────────────────────────────────────────────
#  Integrated Review Runner
# ─────────────────────────────────────────────

def run_migration_review(output_dir="output", config=None):
    """Run a full migration review: duplicates, anti-patterns, SQL optimization, rework.

    Returns:
        dict with review results and queue
    """
    review_cfg = load_review_config(config)
    queue = ReviewQueueManager(config)

    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "output_dir": output_dir,
    }

    # 1. Duplicate detection
    detector = DuplicateDetector()
    dups = detector.find_duplicates(directory=output_dir)
    results["duplicates"] = dups
    for d in dups:
        queue.add_item(
            title=f"Duplicate artifacts: {len(d['files'])} files",
            category="merge",
            severity="medium",
            details=d,
        )

    # 2. Consolidation analysis
    recommender = ConsolidationRecommender(threshold=review_cfg.get("merge_threshold", 0.8))
    consolidation = recommender.analyze(output_dir)
    results["consolidation"] = {
        "merge_candidates": consolidation["merge_candidates"],
        "review_candidates": consolidation["review_candidates"],
        "recommendations": consolidation["recommendations"],
    }
    for rec in consolidation["recommendations"]:
        if rec["type"] == "merge":
            queue.add_item(
                title=f"Merge opportunity: {len(rec['files'])} files",
                category="merge",
                severity="medium",
                details=rec,
            )

    # 3. Anti-pattern detection
    if review_cfg.get("anti_patterns", True):
        ap_detector = AntiPatternDetector()
        ap_results = ap_detector.scan_directory(output_dir)
        results["anti_patterns"] = ap_results
        for finding in ap_results["findings"][:50]:  # Limit queue entries
            queue.add_item(
                title=f"[{finding['id']}] {finding['name']}",
                category="anti-pattern",
                severity=finding["severity"],
                details=finding,
                source=finding["source"],
            )

    # 4. SQL optimization
    sql_dir = os.path.join(output_dir, "sql")
    if review_cfg.get("sql_optimization", True) and os.path.isdir(sql_dir):
        optimizer = SQLOptimizer()
        sql_suggestions = optimizer.analyze_directory(sql_dir)
        results["sql_optimization"] = sql_suggestions
        for sug in sql_suggestions:
            queue.add_item(
                title=f"[{sug['rule_id']}] {sug['suggestion'][:60]}",
                category="optimize",
                severity="low",
                details=sug,
                source=sug.get("source"),
            )

    # 5. Rework classification
    classifier = ReworkClassifier()
    rework = classifier.classify_directory(output_dir)
    results["rework"] = rework
    for item in rework["items"][:50]:
        queue.add_item(
            title=f"Rework: {item['marker']} in {os.path.basename(item['source'])}",
            category="rework",
            severity=item["severity"],
            details=item,
            source=item["source"],
        )

    results["queue"] = queue.get_summary()
    results["all_items"] = queue.get_queue()

    # Save report
    report_path = os.path.join(output_dir, "migration_review_report.json")
    os.makedirs(output_dir, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    return results
