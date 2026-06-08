"""
=============================================================================
  Benchmark Client — Temporal Versioning in Distributed JSON
  Runs experiments against Node A (5001) and Node B (5002)
=============================================================================

Usage:
    1. Start both nodes first:
       cd node_a && python app.py
       cd node_b && python app.py

    2. Run benchmark:
       cd client && python benchmark.py

Experiments:
    1. Write 50 documents x 20 versions each
    2. Measure write latency (snapshot vs delta)
    3. Run 200 time-travel queries on both strategies
    4. Compare storage sizes
    5. Test failure scenario (Node B down)
"""

import json
import time
import copy
import random
import sys
import os
import statistics
from datetime import datetime, timedelta
from typing import Optional

import requests

# ============================================================================
# Configuration
# ============================================================================

NODE_A = "http://localhost:5001"
NODE_B = "http://localhost:5002"
NUM_DOCS = 50
VERSIONS_PER_DOC = 20
SEED = 42
TIMEOUT = 10

random.seed(SEED)

# ============================================================================
# Sample data pools
# ============================================================================

TITLES = [
    "Cloud Migration Platform", "AI Analytics Dashboard",
    "E-Commerce Microservices", "Mobile Payment Gateway",
    "Real-Time Chat System", "Data Lake Architecture",
    "K8s Orchestration Layer", "CI/CD Modernization",
    "Customer 360 Platform", "IoT Sensor Manager",
    "Blockchain Supply Chain", "Serverless API Gateway",
    "ML Feature Store", "Video Streaming Backend",
    "IAM v2 System", "Distributed Cache Layer",
    "Event-Driven Processing", "Multi-Tenant SaaS",
    "Geospatial Engine", "Recommendation Engine",
    "Compliance Automation", "GraphQL Federation",
    "Edge Computing Framework", "Digital Twin Sim",
    "Zero-Trust Security", "CDN Optimization",
    "Test Automation Framework", "Search Re-Index",
    "Notification Revamp", "Data Governance",
    "Rate Limiting Service", "Log Aggregation",
    "Feature Flag Manager", "A/B Testing Infra",
    "Workflow Automation", "Document Management",
    "Service Mesh Impl", "DB Sharding Strategy",
    "Message Queue Migration", "Perf Monitoring",
    "Disaster Recovery", "Multi-Region Deploy",
    "Data Anonymization", "Smart Contracts",
    "NLP API Service", "AR SDK Platform",
    "Predictive Maintenance", "Fleet Management",
    "Healthcare Integration", "FinTech Reconciliation",
]

REQUIREMENTS = [
    "10K concurrent users", "99.99% uptime SLA",
    "Sub-200ms response", "GDPR compliance",
    "E2E encryption", "OAuth 2.0 integration",
    "Auto-failover < 30s", "Horizontal scaling",
    "Multi-region replication", "RBAC support",
    "Audit logging", "Rate limiting 1000/min",
    "Webhook support", "Batch processing 100K/hr",
    "90-day data retention", "Real-time streaming",
    "Blue-green deploy", "Canary releases",
    "Circuit breaker", "Distributed tracing",
]

ASSIGNEES = [
    "alice.nguyen", "bob.tran", "charlie.le", "diana.pham",
    "edward.vu", "fiona.hoang", "george.do", "hannah.bui",
    "ivan.ngo", "julia.dang", "kevin.mai", "laura.ly",
]

STATUSES = ["draft", "in_review", "approved", "in_progress", "testing", "deployed"]
PRIORITIES = ["low", "medium", "high", "critical"]


def generate_document(doc_idx: int) -> dict:
    """Generate an initial project specification document."""
    return {
        "title": TITLES[doc_idx % len(TITLES)],
        "description": f"Project specification for {TITLES[doc_idx % len(TITLES)]}.",
        "requirements": random.sample(REQUIREMENTS, random.randint(3, 6)),
        "assignees": random.sample(ASSIGNEES, random.randint(2, 4)),
        "status": "draft",
        "priority": random.choice(PRIORITIES[:2]),
        "updated_by": random.choice(ASSIGNEES),
    }


def apply_edit(doc: dict) -> dict:
    """Apply random edits to a document."""
    doc = copy.deepcopy(doc)
    edits = random.sample([
        "add_req", "rm_req", "status", "priority", "add_assignee", "updated_by"
    ], random.randint(1, 3))

    for e in edits:
        if e == "add_req":
            avail = [r for r in REQUIREMENTS if r not in doc["requirements"]]
            if avail:
                doc["requirements"].append(random.choice(avail))
        elif e == "rm_req" and len(doc["requirements"]) > 1:
            doc["requirements"].pop(random.randint(0, len(doc["requirements"]) - 1))
        elif e == "status":
            idx = STATUSES.index(doc["status"]) if doc["status"] in STATUSES else 0
            doc["status"] = STATUSES[min(idx + random.randint(0, 2), len(STATUSES) - 1)]
        elif e == "priority":
            doc["priority"] = random.choice(PRIORITIES)
        elif e == "add_assignee":
            avail = [a for a in ASSIGNEES if a not in doc["assignees"]]
            if avail:
                doc["assignees"].append(random.choice(avail))
        elif e == "updated_by":
            doc["updated_by"] = random.choice(ASSIGNEES)
    return doc


def generate_timestamps(n: int) -> list[str]:
    """Generate n sorted ISO timestamps in January 2024."""
    start = datetime(2024, 1, 1, 8, 0, 0)
    end = datetime(2024, 1, 31, 23, 59, 59)
    total_sec = int((end - start).total_seconds())
    offsets = sorted(random.sample(range(total_sec), n))
    return [(start + timedelta(seconds=s)).strftime("%Y-%m-%dT%H:%M:%S") for s in offsets]


# ============================================================================
# Benchmark Functions
# ============================================================================

def check_health(node_url: str, name: str) -> bool:
    """Check if a node is healthy."""
    try:
        r = requests.get(f"{node_url}/health", timeout=3)
        if r.status_code == 200:
            print(f"  [OK] {name} is healthy")
            return True
    except Exception:
        pass
    print(f"  [FAIL] {name} is NOT reachable at {node_url}")
    return False


def run_write_experiment() -> dict:
    """Experiment 1: Write documents and measure latency."""
    print("\n" + "=" * 64)
    print("  EXPERIMENT 1: WRITE THROUGHPUT")
    print("=" * 64)

    write_latencies = []
    all_timestamps = {}  # doc_id -> [timestamps]

    for doc_idx in range(NUM_DOCS):
        doc_id = f"doc_{doc_idx + 1:03d}"
        timestamps = generate_timestamps(VERSIONS_PER_DOC)
        all_timestamps[doc_id] = timestamps

        current_doc = generate_document(doc_idx)

        for ver_idx in range(VERSIONS_PER_DOC):
            ts = timestamps[ver_idx]
            payload = {
                "content": current_doc,
                "author_id": current_doc["updated_by"],
                "timestamp": ts,
                "title": current_doc.get("title"),
            }

            t0 = time.perf_counter_ns()
            try:
                r = requests.post(
                    f"{NODE_A}/document/{doc_id}",
                    json=payload,
                    timeout=TIMEOUT,
                )
                latency_ms = (time.perf_counter_ns() - t0) / 1_000_000
                write_latencies.append(latency_ms)

                if r.status_code != 201:
                    print(f"  WARN: Write failed for {doc_id} v{ver_idx+1}: HTTP {r.status_code}")
            except Exception as e:
                print(f"  ERROR: {doc_id} v{ver_idx+1}: {e}")

            # Prepare next version
            if ver_idx < VERSIONS_PER_DOC - 1:
                current_doc = apply_edit(current_doc)

        if (doc_idx + 1) % 10 == 0:
            print(f"  Written {doc_idx + 1}/{NUM_DOCS} documents...")

    # Stats
    total = len(write_latencies)
    avg = statistics.mean(write_latencies) if write_latencies else 0
    med = statistics.median(write_latencies) if write_latencies else 0
    p95 = sorted(write_latencies)[int(total * 0.95)] if write_latencies else 0
    mx = max(write_latencies) if write_latencies else 0

    print(f"\n  Results ({total} writes):")
    print(f"    Avg latency:  {avg:.2f} ms")
    print(f"    Median:       {med:.2f} ms")
    print(f"    P95:          {p95:.2f} ms")
    print(f"    Max:          {mx:.2f} ms")

    return {
        "total_writes": total,
        "avg_latency_ms": round(avg, 3),
        "median_latency_ms": round(med, 3),
        "p95_latency_ms": round(p95, 3),
        "max_latency_ms": round(mx, 3),
        "all_timestamps": all_timestamps,
    }


def run_query_experiment(all_timestamps: dict) -> dict:
    """Experiment 2: Time-travel queries comparing snapshot vs delta."""
    print("\n" + "=" * 64)
    print("  EXPERIMENT 2: TIME-TRAVEL QUERY LATENCY")
    print("=" * 64)

    num_queries = 200
    snap_latencies = []
    delta_latencies = []
    consistency_checks = 0
    consistency_passes = 0

    doc_ids = list(all_timestamps.keys())

    for i in range(num_queries):
        doc_id = random.choice(doc_ids)
        ts_list = all_timestamps[doc_id]
        # Pick a random timestamp between first and last
        ts = random.choice(ts_list)

        try:
            r = requests.get(
                f"{NODE_A}/document/{doc_id}",
                params={"at": ts, "strategy": "both"},
                timeout=TIMEOUT,
            )
            if r.status_code == 200:
                data = r.json()
                snap = data.get("snapshot")
                delta = data.get("delta")

                if snap and "query_latency_ms" in snap:
                    snap_latencies.append(snap["query_latency_ms"])
                if delta and "query_latency_ms" in delta:
                    delta_latencies.append(delta["query_latency_ms"])

                # Consistency check: both strategies should return same content
                if snap and delta:
                    consistency_checks += 1
                    if snap.get("content") == delta.get("content"):
                        consistency_passes += 1
        except Exception as e:
            print(f"  ERROR query {i}: {e}")

        if (i + 1) % 50 == 0:
            print(f"  Queried {i + 1}/{num_queries}...")

    # Stats
    def calc_stats(latencies, name):
        if not latencies:
            return {}
        return {
            f"{name}_avg_ms": round(statistics.mean(latencies), 3),
            f"{name}_median_ms": round(statistics.median(latencies), 3),
            f"{name}_p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 3),
            f"{name}_max_ms": round(max(latencies), 3),
        }

    snap_stats = calc_stats(snap_latencies, "snapshot")
    delta_stats = calc_stats(delta_latencies, "delta")

    ratio = (
        statistics.mean(delta_latencies) / statistics.mean(snap_latencies)
        if snap_latencies and delta_latencies and statistics.mean(snap_latencies) > 0
        else 0
    )

    print(f"\n  Results ({num_queries} queries):")
    print(f"    Snapshot avg:  {snap_stats.get('snapshot_avg_ms', 0):.3f} ms")
    print(f"    Delta avg:     {delta_stats.get('delta_avg_ms', 0):.3f} ms")
    print(f"    Ratio:         {ratio:.1f}x")
    print(f"    Consistency:   {consistency_passes}/{consistency_checks} passed")

    return {
        "num_queries": num_queries,
        **snap_stats,
        **delta_stats,
        "ratio": round(ratio, 1),
        "consistency_checks": consistency_checks,
        "consistency_passes": consistency_passes,
    }


def run_storage_comparison() -> dict:
    """Experiment 3: Compare storage sizes on both nodes."""
    print("\n" + "=" * 64)
    print("  EXPERIMENT 3: STORAGE COMPARISON")
    print("=" * 64)

    results = {}
    for name, url in [("Node A", NODE_A), ("Node B", NODE_B)]:
        try:
            r = requests.get(f"{url}/stats", timeout=5)
            if r.status_code == 200:
                stats = r.json()
                results[name] = stats
                print(f"\n  {name}:")
                print(f"    Documents:      {stats['documents']}")
                print(f"    Snapshot total:  {stats['snapshots']['total_kb']:.1f} KB "
                      f"({stats['snapshots']['versions']} versions)")
                print(f"    Delta total:     {stats['deltas']['total_kb']:.1f} KB "
                      f"({stats['deltas']['versions']} versions)")
                print(f"    Savings:         {stats['savings_pct']}%")
                print(f"    Pending syncs:   {stats['pending_syncs']}")
        except Exception as e:
            print(f"  {name}: unreachable ({e})")

    return results


def run_failure_scenario() -> dict:
    """Experiment 4: Write while Node B is assumed down."""
    print("\n" + "=" * 64)
    print("  EXPERIMENT 4: FAILURE SCENARIO")
    print("=" * 64)
    print("  Testing: Write to Node A when Node B may be unreachable")

    doc_id = "doc_failure_test"
    content = {
        "title": "Failure Test Document",
        "description": "Testing write when peer node is down",
        "requirements": ["fault tolerance", "data consistency"],
        "assignees": ["test.user"],
        "status": "testing",
        "priority": "critical",
        "updated_by": "test.user",
    }

    # Write to Node A
    try:
        r = requests.post(
            f"{NODE_A}/document/{doc_id}",
            json={
                "content": content,
                "author_id": "test.user",
                "title": "Failure Test",
            },
            timeout=TIMEOUT,
        )
        print(f"\n  Write to Node A: HTTP {r.status_code}")
        if r.status_code == 201:
            print("  Node A accepted the write successfully")

        # Check if Node A can serve the data
        r2 = requests.get(f"{NODE_A}/document/{doc_id}", timeout=5)
        print(f"  Read from Node A: HTTP {r2.status_code}")

        # Check Node B (might be down)
        try:
            r3 = requests.get(f"{NODE_B}/document/{doc_id}", timeout=3)
            print(f"  Read from Node B: HTTP {r3.status_code}")
            if r3.status_code == 200:
                data_a = r2.json()["snapshot"]["content"]
                data_b = r3.json()["snapshot"]["content"]
                consistent = data_a == data_b
                print(f"  Cross-node consistency: {'PASS' if consistent else 'FAIL'}")
                return {"node_a_write": True, "node_b_synced": True, "consistent": consistent}
        except Exception:
            print("  Node B unreachable - write queued for sync")
            return {"node_a_write": True, "node_b_synced": False, "consistent": "pending"}

    except Exception as e:
        print(f"  ERROR: {e}")
        return {"node_a_write": False, "error": str(e)}

    return {}


# ============================================================================
# Main
# ============================================================================

def main():
    print("\n" + "=" * 64)
    print("  DISTRIBUTED TEMPORAL JSON VERSIONING — BENCHMARK")
    print("  Team ChronoJSON | Topic 79")
    print("=" * 64)

    # Health check
    print("\n  Checking node health...")
    a_ok = check_health(NODE_A, "Node A (5001)")
    b_ok = check_health(NODE_B, "Node B (5002)")

    if not a_ok:
        print("\n  FATAL: Node A must be running. Aborting.")
        sys.exit(1)

    if not b_ok:
        print("\n  WARNING: Node B is not running. Continuing with Node A only.\n")

    # Run experiments
    write_results = run_write_experiment()

    # Wait a moment for async syncs to complete
    print("\n  Waiting 5s for async replication to complete...")
    time.sleep(5)

    query_results = run_query_experiment(write_results["all_timestamps"])
    storage_results = run_storage_comparison()
    failure_results = run_failure_scenario()

    # Final summary
    print("\n" + "=" * 64)
    print("  FINAL SUMMARY")
    print("=" * 64)
    print(f"  Total documents written:    {NUM_DOCS}")
    print(f"  Total versions written:     {write_results['total_writes']}")
    print(f"  Avg write latency:          {write_results['avg_latency_ms']:.2f} ms")
    print(f"  Snapshot query avg:         {query_results.get('snapshot_avg_ms', 'N/A')} ms")
    print(f"  Delta query avg:            {query_results.get('delta_avg_ms', 'N/A')} ms")
    print(f"  Query ratio (delta/snap):   {query_results.get('ratio', 'N/A')}x")
    print(f"  Consistency checks:         {query_results.get('consistency_passes', 0)}"
          f"/{query_results.get('consistency_checks', 0)} passed")
    print("=" * 64)

    # Save results to JSON
    output = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "num_docs": NUM_DOCS,
            "versions_per_doc": VERSIONS_PER_DOC,
            "seed": SEED,
        },
        "write_experiment": {k: v for k, v in write_results.items() if k != "all_timestamps"},
        "query_experiment": query_results,
        "storage_comparison": storage_results,
        "failure_scenario": failure_results,
    }

    os.makedirs(os.path.dirname(os.path.abspath(__file__)), exist_ok=True)
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmark_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n  Results saved to: {output_path}\n")


if __name__ == "__main__":
    main()
