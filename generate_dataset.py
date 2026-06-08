"""
=============================================================================
  Dataset Generator — Temporal Versioning in Distributed JSON: Wiki Edits
  Topic 79 · Team ChronoJSON
=============================================================================

Generates synthetic "Project Specification" wiki-edit data:
  - 50 documents × 20 versions each = 1,000 total versions
  - Outputs: full_snapshots.json, delta_edits.json
  - Includes time-travel query functions for both strategies
  - Prints comparative storage statistics

Usage:
    python generate_dataset.py            # generate + stats
    python generate_dataset.py --test     # run unit tests
"""

import json
import copy
import random
import string
import sys
import os
import time
import unittest
from datetime import datetime, timedelta
from typing import Any

import jsonpatch

# ============================================================================
# Configuration
# ============================================================================
NUM_DOCS = 50
VERSIONS_PER_DOC = 20
START_DATE = datetime(2024, 1, 1, 8, 0, 0)
END_DATE = datetime(2024, 1, 31, 23, 59, 59)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
SEED = 42

random.seed(SEED)

# ============================================================================
# Sample pools for realistic content generation
# ============================================================================
TITLES = [
    "Cloud Migration Platform", "AI-Powered Analytics Dashboard",
    "E-Commerce Microservices Refactor", "Mobile Payment Gateway",
    "Real-Time Chat Infrastructure", "Data Lake Architecture",
    "Kubernetes Orchestration Layer", "CI/CD Pipeline Modernization",
    "Customer 360 Data Platform", "IoT Sensor Management System",
    "Blockchain Supply Chain Tracker", "Serverless API Gateway",
    "Machine Learning Feature Store", "Video Streaming Backend",
    "Identity & Access Management v2", "Distributed Cache Layer",
    "Event-Driven Order Processing", "Multi-Tenant SaaS Platform",
    "Geospatial Analytics Engine", "Recommendation Engine Overhaul",
    "Compliance Audit Automation", "GraphQL Federation Gateway",
    "Edge Computing Framework", "Digital Twin Simulation",
    "Zero-Trust Security Architecture", "Content Delivery Optimization",
    "Automated Testing Framework", "Search Engine Re-Index",
    "Notification Service Revamp", "Data Governance Platform",
    "API Rate Limiting Service", "Log Aggregation Pipeline",
    "Feature Flag Management", "A/B Testing Infrastructure",
    "Workflow Automation Engine", "Document Management System",
    "Service Mesh Implementation", "Database Sharding Strategy",
    "Message Queue Migration", "Performance Monitoring Suite",
    "Disaster Recovery Automation", "Multi-Region Deployment",
    "Data Anonymization Pipeline", "Smart Contract Platform",
    "Natural Language Processing API", "Augmented Reality SDK",
    "Predictive Maintenance System", "Fleet Management Dashboard",
    "Healthcare Data Integration", "FinTech Reconciliation Engine",
]

REQUIREMENTS = [
    "Support 10K concurrent users", "99.99% uptime SLA",
    "Sub-200ms API response time", "GDPR compliance",
    "End-to-end encryption", "OAuth 2.0 / OIDC integration",
    "Automated failover < 30 seconds", "Horizontal auto-scaling",
    "Multi-region data replication", "Role-based access control",
    "Audit logging for all mutations", "Rate limiting (1000 req/min)",
    "Webhook notification support", "Batch processing (100K records/hr)",
    "Data retention policy (90 days)", "Real-time event streaming",
    "Blue-green deployment support", "Canary release mechanism",
    "Circuit breaker pattern", "Distributed tracing (OpenTelemetry)",
    "Schema versioning with backward compatibility",
    "Automated database migrations", "Health check endpoints",
    "Graceful shutdown handling", "Configuration hot-reload",
    "API versioning (v1, v2)", "Request idempotency support",
    "Bulk import/export (CSV, JSON)", "Full-text search capability",
    "Dashboard with real-time charts", "Email + SMS notifications",
    "Two-factor authentication", "IP whitelisting",
    "Custom domain support", "Internationalization (i18n)",
    "Accessibility (WCAG 2.1 AA)", "Dark mode support",
    "Offline-first mobile support", "Push notification service",
    "SSO with SAML 2.0",
]

ASSIGNEES = [
    "alice.nguyen", "bob.tran", "charlie.le", "diana.pham",
    "edward.vu", "fiona.hoang", "george.do", "hannah.bui",
    "ivan.ngo", "julia.dang", "kevin.mai", "laura.ly",
    "mike.trinh", "nancy.vo", "oscar.lam", "paula.tang",
    "quentin.cao", "rachel.duong", "steve.ha", "tina.luong",
]

STATUSES = ["draft", "in_review", "approved", "in_progress", "testing", "deployed", "archived"]
PRIORITIES = ["low", "medium", "high", "critical"]
DESCRIPTIONS_TEMPLATES = [
    "This project aims to {verb} the existing {noun} infrastructure for improved {adj} performance.",
    "A comprehensive initiative to {verb} our {noun} capabilities with {adj} architecture.",
    "Redesigning the {noun} layer to {verb} scalability and ensure {adj} reliability.",
    "Building a {adj} {noun} solution that will {verb} operational efficiency.",
]
VERBS = ["modernize", "overhaul", "optimize", "redesign", "migrate", "refactor", "enhance", "rebuild"]
NOUNS = ["backend", "frontend", "data pipeline", "API", "microservice", "platform", "infrastructure"]
ADJS = ["cloud-native", "high-performance", "fault-tolerant", "enterprise-grade", "next-generation"]


# ============================================================================
# Document Generation
# ============================================================================

def generate_description() -> str:
    """Generate a realistic project description."""
    template = random.choice(DESCRIPTIONS_TEMPLATES)
    return template.format(
        verb=random.choice(VERBS),
        noun=random.choice(NOUNS),
        adj=random.choice(ADJS),
    )


def generate_initial_document(doc_index: int) -> dict:
    """Create the initial version (v1) of a project specification document."""
    num_reqs = random.randint(3, 6)
    num_assignees = random.randint(2, 4)

    return {
        "title": TITLES[doc_index % len(TITLES)],
        "description": generate_description(),
        "requirements": random.sample(REQUIREMENTS, num_reqs),
        "assignees": random.sample(ASSIGNEES, num_assignees),
        "status": "draft",
        "priority": random.choice(PRIORITIES[:2]),  # start low/medium
        "updated_by": random.choice(ASSIGNEES),
    }


def apply_random_edit(document: dict) -> dict:
    """
    Apply 1-3 random field mutations to simulate a wiki edit.
    Returns a NEW document (deep copy) with changes applied.
    """
    doc = copy.deepcopy(document)
    num_edits = random.randint(1, 3)
    edit_types = random.sample([
        "add_requirement", "remove_requirement", "change_title_suffix",
        "change_status", "change_priority", "add_assignee",
        "remove_assignee", "update_description", "change_updated_by",
    ], min(num_edits, 9))

    for edit_type in edit_types:
        if edit_type == "add_requirement":
            available = [r for r in REQUIREMENTS if r not in doc["requirements"]]
            if available:
                doc["requirements"].append(random.choice(available))

        elif edit_type == "remove_requirement":
            if len(doc["requirements"]) > 1:
                doc["requirements"].pop(random.randint(0, len(doc["requirements"]) - 1))

        elif edit_type == "change_title_suffix":
            base = doc["title"].split(" — ")[0].split(" (")[0]
            suffixes = ["(Revised)", "(v2)", "(Updated)", "— Phase 2", "(Final)", "(Draft 3)"]
            doc["title"] = f"{base} {random.choice(suffixes)}"

        elif edit_type == "change_status":
            current_idx = STATUSES.index(doc["status"]) if doc["status"] in STATUSES else 0
            # Status tends to move forward
            new_idx = min(current_idx + random.randint(0, 2), len(STATUSES) - 1)
            doc["status"] = STATUSES[new_idx]

        elif edit_type == "change_priority":
            doc["priority"] = random.choice(PRIORITIES)

        elif edit_type == "add_assignee":
            available = [a for a in ASSIGNEES if a not in doc["assignees"]]
            if available:
                doc["assignees"].append(random.choice(available))

        elif edit_type == "remove_assignee":
            if len(doc["assignees"]) > 1:
                doc["assignees"].pop(random.randint(0, len(doc["assignees"]) - 1))

        elif edit_type == "update_description":
            doc["description"] = generate_description()

        elif edit_type == "change_updated_by":
            doc["updated_by"] = random.choice(ASSIGNEES)

    return doc


def generate_timestamps(n: int) -> list[str]:
    """Generate n sorted random ISO-8601 timestamps between START_DATE and END_DATE."""
    total_seconds = int((END_DATE - START_DATE).total_seconds())
    offsets = sorted(random.sample(range(total_seconds), n))
    return [(START_DATE + timedelta(seconds=s)).strftime("%Y-%m-%dT%H:%M:%S") for s in offsets]


# ============================================================================
# Dataset Builder
# ============================================================================

def build_dataset() -> tuple[list[dict], list[dict]]:
    """
    Build the complete dataset.
    Returns:
        (full_snapshots, delta_edits) — two lists of version records.
    """
    full_snapshots: list[dict] = []
    delta_edits: list[dict] = []

    for doc_idx in range(NUM_DOCS):
        doc_id = f"doc_{doc_idx + 1:03d}"
        timestamps = generate_timestamps(VERSIONS_PER_DOC)

        # Version 1: initial document
        current_doc = generate_initial_document(doc_idx)
        previous_doc = copy.deepcopy(current_doc)

        for ver_idx in range(VERSIONS_PER_DOC):
            version = ver_idx + 1
            ts = timestamps[ver_idx]
            author = current_doc.get("updated_by", random.choice(ASSIGNEES))

            # ---- Full Snapshot record ----
            snapshot_record = {
                "version_id": f"{doc_id}_v{version:03d}",
                "doc_id": doc_id,
                "version": version,
                "timestamp": ts,
                "author_id": author,
                "full_content": copy.deepcopy(current_doc),
            }
            full_snapshots.append(snapshot_record)

            # ---- Delta record ----
            if version == 1:
                # First version: store as full snapshot (base)
                delta_record = {
                    "version_id": f"{doc_id}_v{version:03d}",
                    "doc_id": doc_id,
                    "version": version,
                    "timestamp": ts,
                    "author_id": author,
                    "base_version": None,
                    "delta_patch": None,
                    "full_content": copy.deepcopy(current_doc),
                }
            else:
                # Compute JSON Patch (RFC 6902)
                patch = jsonpatch.make_patch(previous_doc, current_doc)
                delta_record = {
                    "version_id": f"{doc_id}_v{version:03d}",
                    "doc_id": doc_id,
                    "version": version,
                    "timestamp": ts,
                    "author_id": author,
                    "base_version": version - 1,
                    "delta_patch": patch.patch,
                    "full_content": None,  # only stored for v1
                }
            delta_edits.append(delta_record)

            # Prepare next version
            previous_doc = copy.deepcopy(current_doc)
            if ver_idx < VERSIONS_PER_DOC - 1:
                current_doc = apply_random_edit(current_doc)

    return full_snapshots, delta_edits


# ============================================================================
# Time-Travel Query Functions
# ============================================================================

def get_document_at_time_snapshot(
    snapshots: list[dict], doc_id: str, timestamp: str
) -> dict | None:
    """
    Time-travel query using Full Snapshot strategy.
    Returns the document state at or just before the given timestamp.
    Complexity: O(n) scan, but O(1) once the correct version is found
    (no reconstruction needed).
    """
    candidates = [
        s for s in snapshots
        if s["doc_id"] == doc_id and s["timestamp"] <= timestamp
    ]
    if not candidates:
        return None
    # Get the latest version at or before timestamp
    best = max(candidates, key=lambda s: s["timestamp"])
    return copy.deepcopy(best["full_content"])


def get_document_at_time_delta(
    deltas: list[dict], doc_id: str, timestamp: str
) -> dict | None:
    """
    Time-travel query using Delta Encoding strategy.
    Reconstructs document by applying patches from base snapshot up to target time.
    Complexity: O(n) where n = number of versions to apply.
    """
    # Get all versions for this doc, sorted by version number
    doc_versions = sorted(
        [d for d in deltas if d["doc_id"] == doc_id],
        key=lambda d: d["version"],
    )
    if not doc_versions:
        return None

    # Find the base (v1) — must have full_content
    base = doc_versions[0]
    if base["full_content"] is None:
        return None

    current = copy.deepcopy(base["full_content"])

    # Apply deltas sequentially up to the target timestamp
    for ver in doc_versions[1:]:
        if ver["timestamp"] > timestamp:
            break
        if ver["delta_patch"]:
            patch = jsonpatch.JsonPatch(ver["delta_patch"])
            current = patch.apply(current)

    # Check if even v1 is after the target
    if base["timestamp"] > timestamp:
        return None

    return current


# ============================================================================
# Index Builder (for faster lookups — simulates SQLite index)
# ============================================================================

def build_index(records: list[dict]) -> dict[str, list[dict]]:
    """Build a doc_id → sorted list of records index (like a SQLite index)."""
    index: dict[str, list[dict]] = {}
    for r in records:
        doc_id = r["doc_id"]
        if doc_id not in index:
            index[doc_id] = []
        index[doc_id].append(r)
    for doc_id in index:
        index[doc_id].sort(key=lambda r: r["timestamp"])
    return index


def get_document_at_time_snapshot_indexed(
    index: dict[str, list[dict]], doc_id: str, timestamp: str
) -> dict | None:
    """Indexed version of snapshot time-travel query. O(log n) with binary search."""
    versions = index.get(doc_id)
    if not versions:
        return None

    # Binary search for the latest version <= timestamp
    lo, hi, result = 0, len(versions) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if versions[mid]["timestamp"] <= timestamp:
            result = versions[mid]
            lo = mid + 1
        else:
            hi = mid - 1

    return copy.deepcopy(result["full_content"]) if result else None


def get_document_at_time_delta_indexed(
    index: dict[str, list[dict]], doc_id: str, timestamp: str
) -> dict | None:
    """Indexed version of delta time-travel query."""
    versions = index.get(doc_id)
    if not versions or versions[0]["timestamp"] > timestamp:
        return None

    base = versions[0]
    if base["full_content"] is None:
        return None

    current = copy.deepcopy(base["full_content"])
    for ver in versions[1:]:
        if ver["timestamp"] > timestamp:
            break
        if ver["delta_patch"]:
            patch = jsonpatch.JsonPatch(ver["delta_patch"])
            current = patch.apply(current)

    return current


# ============================================================================
# Statistics & Output
# ============================================================================

def compute_stats(full_snapshots: list[dict], delta_edits: list[dict]) -> dict:
    """Compute and return storage statistics."""
    full_json = json.dumps(full_snapshots, ensure_ascii=False)
    delta_json = json.dumps(delta_edits, ensure_ascii=False)

    full_size = len(full_json.encode("utf-8"))
    delta_size = len(delta_json.encode("utf-8"))

    ratio = (1 - delta_size / full_size) * 100 if full_size > 0 else 0

    return {
        "num_documents": NUM_DOCS,
        "versions_per_doc": VERSIONS_PER_DOC,
        "total_versions": NUM_DOCS * VERSIONS_PER_DOC,
        "full_snapshot_size_bytes": full_size,
        "delta_size_bytes": delta_size,
        "compression_ratio_pct": ratio,
    }


def print_stats(stats: dict):
    """Print formatted statistics to console."""
    print("\n" + "=" * 64)
    print("  DATASET GENERATION — STATISTICS")
    print("=" * 64)
    print(f"  Documents generated:    {stats['num_documents']}")
    print(f"  Versions per document:  {stats['versions_per_doc']}")
    print(f"  Total versions:         {stats['total_versions']}")
    print("-" * 64)
    print(f"  Full Snapshot size:     {stats['full_snapshot_size_bytes']:>12,} bytes "
          f"({stats['full_snapshot_size_bytes'] / 1024:.1f} KB)")
    print(f"  Delta Encoding size:    {stats['delta_size_bytes']:>12,} bytes "
          f"({stats['delta_size_bytes'] / 1024:.1f} KB)")
    print(f"  Storage savings:        {stats['compression_ratio_pct']:.1f}%")
    print("=" * 64)


def save_dataset(full_snapshots: list[dict], delta_edits: list[dict]):
    """Save both datasets to JSON files."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    full_path = os.path.join(OUTPUT_DIR, "full_snapshots.json")
    delta_path = os.path.join(OUTPUT_DIR, "delta_edits.json")

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(full_snapshots, f, ensure_ascii=False, indent=2)

    with open(delta_path, "w", encoding="utf-8") as f:
        json.dump(delta_edits, f, ensure_ascii=False, indent=2)

    print(f"\n  Files saved:")
    print(f"    -> {full_path}")
    print(f"    -> {delta_path}")


# ============================================================================
# Benchmark: Time-Travel Query Performance
# ============================================================================

def benchmark_queries(full_snapshots: list[dict], delta_edits: list[dict], num_queries: int = 100):
    """Run random time-travel queries and measure latency for both strategies."""
    print("\n" + "=" * 64)
    print("  TIME-TRAVEL QUERY BENCHMARK")
    print("=" * 64)

    # Build indexes
    snap_index = build_index(full_snapshots)
    delta_index = build_index(delta_edits)

    # Generate random queries
    doc_ids = [f"doc_{i + 1:03d}" for i in range(NUM_DOCS)]
    total_seconds = int((END_DATE - START_DATE).total_seconds())
    queries = []
    for _ in range(num_queries):
        doc_id = random.choice(doc_ids)
        offset = random.randint(0, total_seconds)
        ts = (START_DATE + timedelta(seconds=offset)).strftime("%Y-%m-%dT%H:%M:%S")
        queries.append((doc_id, ts))

    # Benchmark snapshot queries
    snap_times = []
    for doc_id, ts in queries:
        t0 = time.perf_counter_ns()
        get_document_at_time_snapshot_indexed(snap_index, doc_id, ts)
        t1 = time.perf_counter_ns()
        snap_times.append((t1 - t0) / 1_000_000)  # convert to ms

    # Benchmark delta queries
    delta_times = []
    for doc_id, ts in queries:
        t0 = time.perf_counter_ns()
        get_document_at_time_delta_indexed(delta_index, doc_id, ts)
        t1 = time.perf_counter_ns()
        delta_times.append((t1 - t0) / 1_000_000)

    avg_snap = sum(snap_times) / len(snap_times)
    avg_delta = sum(delta_times) / len(delta_times)
    max_snap = max(snap_times)  # noqa: E501
    max_delta = max(delta_times)  # noqa: E501
    speedup = avg_delta / avg_snap if avg_snap > 0 else 0

    print(f"  Queries executed:       {num_queries}")
    print("-" * 64)
    print(f"  Snapshot - avg latency: {avg_snap:.3f} ms  |  max: {max_snap:.3f} ms")
    print(f"  Delta    - avg latency: {avg_delta:.3f} ms  |  max: {max_delta:.3f} ms")
    print(f"  Delta / Snapshot ratio: {speedup:.1f}x")
    print("=" * 64)


# ============================================================================
# Unit Tests
# ============================================================================

class TestTimeTravelQuery(unittest.TestCase):
    """Unit tests for time-travel query correctness."""

    @classmethod
    def setUpClass(cls):
        """Generate a small dataset for testing."""
        random.seed(SEED)
        cls.full_snapshots, cls.delta_edits = build_dataset()
        cls.snap_index = build_index(cls.full_snapshots)
        cls.delta_index = build_index(cls.delta_edits)

    def test_snapshot_returns_none_before_first_version(self):
        """Query before any version exists should return None."""
        result = get_document_at_time_snapshot(
            self.full_snapshots, "doc_001", "2023-12-31T23:59:59"
        )
        self.assertIsNone(result)

    def test_delta_returns_none_before_first_version(self):
        """Delta query before any version exists should return None."""
        result = get_document_at_time_delta(
            self.delta_edits, "doc_001", "2023-12-31T23:59:59"
        )
        self.assertIsNone(result)

    def test_snapshot_and_delta_produce_same_result(self):
        """Both strategies must reconstruct the exact same document at any timestamp."""
        # Test with multiple documents and timestamps
        test_timestamps = [
            "2024-01-05T12:00:00",
            "2024-01-10T18:30:00",
            "2024-01-15T10:00:00",
            "2024-01-20T08:00:00",
            "2024-01-25T23:59:59",
            "2024-01-31T23:59:59",
        ]
        test_docs = ["doc_001", "doc_010", "doc_025", "doc_050"]

        for doc_id in test_docs:
            for ts in test_timestamps:
                snap_result = get_document_at_time_snapshot(
                    self.full_snapshots, doc_id, ts
                )
                delta_result = get_document_at_time_delta(
                    self.delta_edits, doc_id, ts
                )
                self.assertEqual(
                    snap_result, delta_result,
                    f"Mismatch for {doc_id} at {ts}"
                )

    def test_indexed_matches_linear(self):
        """Indexed query should produce same results as linear scan."""
        doc_id = "doc_005"
        ts = "2024-01-15T10:00:00"

        linear = get_document_at_time_snapshot(self.full_snapshots, doc_id, ts)
        indexed = get_document_at_time_snapshot_indexed(self.snap_index, doc_id, ts)
        self.assertEqual(linear, indexed)

        linear_d = get_document_at_time_delta(self.delta_edits, doc_id, ts)
        indexed_d = get_document_at_time_delta_indexed(self.delta_index, doc_id, ts)
        self.assertEqual(linear_d, indexed_d)

    def test_latest_version_returned(self):
        """Querying after all edits should return the last version."""
        doc_id = "doc_001"
        ts = "2024-12-31T23:59:59"  # far future

        result = get_document_at_time_snapshot(self.full_snapshots, doc_id, ts)
        # Should equal the last snapshot for doc_001
        last_snap = [s for s in self.full_snapshots if s["doc_id"] == doc_id][-1]
        self.assertEqual(result, last_snap["full_content"])

    def test_exact_timestamp_match(self):
        """Querying at the exact timestamp of a version should return that version."""
        # Pick the 5th version of doc_003
        target = [s for s in self.full_snapshots
                  if s["doc_id"] == "doc_003" and s["version"] == 5][0]
        ts = target["timestamp"]

        snap_result = get_document_at_time_snapshot(self.full_snapshots, "doc_003", ts)
        delta_result = get_document_at_time_delta(self.delta_edits, "doc_003", ts)

        self.assertEqual(snap_result, target["full_content"])
        self.assertEqual(delta_result, target["full_content"])

    def test_nonexistent_doc_returns_none(self):
        """Querying a doc_id that doesn't exist should return None."""
        self.assertIsNone(
            get_document_at_time_snapshot(self.full_snapshots, "doc_999", "2024-01-15T10:00:00")
        )
        self.assertIsNone(
            get_document_at_time_delta(self.delta_edits, "doc_999", "2024-01-15T10:00:00")
        )

    def test_document_structure(self):
        """Reconstructed document should have all expected fields."""
        result = get_document_at_time_snapshot(
            self.full_snapshots, "doc_001", "2024-01-31T23:59:59"
        )
        self.assertIsNotNone(result)
        expected_keys = {"title", "description", "requirements", "assignees",
                         "status", "priority", "updated_by"}
        self.assertEqual(set(result.keys()), expected_keys)
        self.assertIsInstance(result["requirements"], list)
        self.assertIsInstance(result["assignees"], list)


# ============================================================================
# Main
# ============================================================================

def main():
    if "--test" in sys.argv:
        print("\n  Running unit tests...\n")
        # Remove --test from argv so unittest doesn't get confused
        sys.argv = [sys.argv[0]]
        unittest.main(verbosity=2)
        return

    print("\n  Generating dataset...")
    print(f"  Config: {NUM_DOCS} documents × {VERSIONS_PER_DOC} versions\n")

    t_start = time.perf_counter()
    full_snapshots, delta_edits = build_dataset()
    t_gen = time.perf_counter() - t_start

    # Save files
    save_dataset(full_snapshots, delta_edits)

    # Compute & print stats
    stats = compute_stats(full_snapshots, delta_edits)
    print_stats(stats)
    print(f"\n  Generation time:        {t_gen:.2f} seconds")

    # Run query benchmark
    benchmark_queries(full_snapshots, delta_edits, num_queries=200)

    # Quick verification: snapshot & delta produce same result
    print("\n  Verification: Snapshot vs Delta consistency check...")
    test_cases = [
        ("doc_001", "2024-01-15T10:00:00"),
        ("doc_025", "2024-01-20T14:30:00"),
        ("doc_050", "2024-01-31T23:59:59"),
    ]
    all_ok = True
    for doc_id, ts in test_cases:
        s = get_document_at_time_snapshot(full_snapshots, doc_id, ts)
        d = get_document_at_time_delta(delta_edits, doc_id, ts)
        match = s == d
        status = "[PASS]" if match else "[FAIL]"
        print(f"    {status}  {doc_id} @ {ts}")
        if not match:
            all_ok = False

    print(f"\n  {'All checks passed!' if all_ok else 'SOME CHECKS FAILED!'}")
    print()


if __name__ == "__main__":
    main()
