"""
=============================================================================
  Auto Benchmark — Fully Automated
  Starts Node A + B, runs experiments, prints results, shuts down.
=============================================================================
"""

import subprocess
import sys
import os
import time
import json
import copy
import random
import shutil
import signal
import statistics
from datetime import datetime, timedelta

import requests

# ============================================================================
# Configuration
# ============================================================================

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
NODE_A_DIR = os.path.join(PROJECT_DIR, "node_a")
NODE_B_DIR = os.path.join(PROJECT_DIR, "node_b")
NODE_A_URL = "http://127.0.0.1:5001"
NODE_B_URL = "http://127.0.0.1:5002"

NUM_DOCS = 50
VERSIONS_PER_DOC = 20
TOTAL_VERSIONS = NUM_DOCS * VERSIONS_PER_DOC
NUM_QUERIES = 200
SEED = 42
TIMEOUT = 10

random.seed(SEED)

# ============================================================================
# Data generation helpers
# ============================================================================

TITLES = [
    "Cloud Migration Platform", "AI Analytics Dashboard",
    "E-Commerce Microservices", "Payment Gateway v2",
    "Real-Time Chat System", "Data Lake Architecture",
    "K8s Orchestration Layer", "CI/CD Modernization",
    "Customer 360 Platform", "IoT Sensor Manager",
    "Blockchain Supply Chain", "Serverless Gateway",
    "ML Feature Store", "Video Streaming Backend",
    "IAM v2 System", "Distributed Cache",
    "Event Processing Engine", "Multi-Tenant SaaS",
    "Geospatial Engine", "Recommendation Engine",
    "Compliance Automation", "GraphQL Federation",
    "Edge Computing SDK", "Digital Twin Sim",
    "Zero-Trust Security", "CDN Optimization",
    "Test Automation", "Search Re-Index",
    "Notification Service", "Data Governance",
    "Rate Limiter", "Log Aggregation",
    "Feature Flags", "A/B Testing Infra",
    "Workflow Engine", "Document Manager",
    "Service Mesh", "DB Sharding",
    "MQ Migration", "Perf Monitor",
    "DR Automation", "Multi-Region Deploy",
    "Data Anonymization", "Smart Contracts",
    "NLP API", "AR SDK",
    "Predictive Maintenance", "Fleet Manager",
    "Healthcare Integration", "FinTech Engine",
]

REQUIREMENTS = [
    "10K concurrent users", "99.99% uptime SLA",
    "Sub-200ms response", "GDPR compliance",
    "E2E encryption", "OAuth 2.0",
    "Auto-failover < 30s", "Horizontal scaling",
    "Multi-region replication", "RBAC",
    "Audit logging", "Rate limiting",
    "Webhook support", "Batch 100K/hr",
    "90-day retention", "Real-time streaming",
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


def make_doc(idx):
    return {
        "title": TITLES[idx % len(TITLES)],
        "description": f"Specification for {TITLES[idx % len(TITLES)]}",
        "requirements": random.sample(REQUIREMENTS, random.randint(3, 6)),
        "assignees": random.sample(ASSIGNEES, random.randint(2, 4)),
        "status": "draft",
        "priority": random.choice(PRIORITIES[:2]),
        "updated_by": random.choice(ASSIGNEES),
    }


def edit_doc(doc):
    doc = copy.deepcopy(doc)
    for action in random.sample(["add_req","rm_req","status","priority","assignee","author"], random.randint(1,3)):
        if action == "add_req":
            avail = [r for r in REQUIREMENTS if r not in doc["requirements"]]
            if avail: doc["requirements"].append(random.choice(avail))
        elif action == "rm_req" and len(doc["requirements"]) > 1:
            doc["requirements"].pop(random.randint(0, len(doc["requirements"])-1))
        elif action == "status":
            i = STATUSES.index(doc["status"]) if doc["status"] in STATUSES else 0
            doc["status"] = STATUSES[min(i + random.randint(0,2), len(STATUSES)-1)]
        elif action == "priority":
            doc["priority"] = random.choice(PRIORITIES)
        elif action == "assignee":
            avail = [a for a in ASSIGNEES if a not in doc["assignees"]]
            if avail: doc["assignees"].append(random.choice(avail))
        elif action == "author":
            doc["updated_by"] = random.choice(ASSIGNEES)
    return doc


def make_timestamps(n):
    start = datetime(2024, 1, 1, 8, 0, 0)
    total = int((datetime(2024, 1, 31, 23, 59, 59) - start).total_seconds())
    offsets = sorted(random.sample(range(total), n))
    return [(start + timedelta(seconds=s)).strftime("%Y-%m-%dT%H:%M:%S") for s in offsets]


# ============================================================================
# Node lifecycle
# ============================================================================

def clean_node_data(node_dir):
    """Remove old database and data files."""
    for f in ["snapshots.db", "deltas.db", "node_a.db", "node_b.db",
              "snapshots.db-wal", "deltas.db-wal", "snapshots.db-shm", "deltas.db-shm",
              "node_a.db-wal", "node_b.db-wal", "node_a.db-shm", "node_b.db-shm"]:
        p = os.path.join(node_dir, f)
        if os.path.exists(p):
            os.remove(p)
    data_dir = os.path.join(node_dir, "data")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def start_node(node_dir, name):
    """Start a Flask node as a subprocess."""
    log_file = open(os.path.join(node_dir, "server.log"), "w")
    proc = subprocess.Popen(
        [sys.executable, "app.py"],
        cwd=node_dir,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0,
    )
    # Store the log file in the process object so we can close it later
    proc.log_file = log_file
    print(f"  Started {name} (PID {proc.pid})")
    return proc


def wait_for_health(url, name, timeout=10):
    """Wait until node responds to /health."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{url}/health", timeout=2)
            if r.status_code == 200:
                print(f"  {name} is healthy")
                return True
        except Exception:
            pass
        time.sleep(0.3)
    print(f"  WARN: {name} did not become healthy in {timeout}s")
    return False


def stop_node(proc, name):
    """Gracefully stop a node subprocess."""
    try:
        if os.name == 'nt':
            proc.terminate()
        else:
            proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
    if hasattr(proc, 'log_file'):
        proc.log_file.close()
    print(f"  Stopped {name}")


# ============================================================================
# Experiments
# ============================================================================

def run_writes():
    """Write 50 docs x 20 versions, collect per-strategy latency."""
    print(f"\n  Writing {TOTAL_VERSIONS} versions ({NUM_DOCS} docs x {VERSIONS_PER_DOC} ver)...")

    snap_write_ms = []
    delta_write_ms = []
    all_timestamps = {}
    
    session = requests.Session()

    for di in range(NUM_DOCS):
        doc_id = f"doc_{di+1:03d}"
        timestamps = make_timestamps(VERSIONS_PER_DOC)
        all_timestamps[doc_id] = timestamps
        doc = make_doc(di)

        for vi in range(VERSIONS_PER_DOC):
            payload = {
                "content": doc,
                "author_id": doc["updated_by"],
                "timestamp": timestamps[vi],
                "title": doc.get("title"),
            }
            try:
                r = session.post(f"{NODE_A_URL}/document/{doc_id}", json=payload, timeout=TIMEOUT)
                if r.status_code == 201:
                    d = r.json()
                    snap_write_ms.append(d["snapshot_write_ms"])
                    delta_write_ms.append(d["delta_write_ms"])
            except Exception as e:
                print(f"    ERROR write {doc_id} v{vi+1}: {e}")

            if vi < VERSIONS_PER_DOC - 1:
                doc = edit_doc(doc)

        if (di+1) % 10 == 0:
            print(f"    {di+1}/{NUM_DOCS} docs written")

    return snap_write_ms, delta_write_ms, all_timestamps


def run_queries(all_timestamps):
    """Run 200 random time-travel queries, collect per-strategy latency."""
    print(f"\n  Running {NUM_QUERIES} time-travel queries...")

    snap_query_ms = []
    delta_query_ms = []
    patches_applied_list = []
    consistency_ok = 0
    consistency_total = 0

    doc_ids = list(all_timestamps.keys())
    session = requests.Session()

    for i in range(NUM_QUERIES):
        doc_id = random.choice(doc_ids)
        ts = random.choice(all_timestamps[doc_id])

        try:
            r = session.get(f"{NODE_A_URL}/document/{doc_id}", params={"at": ts}, timeout=TIMEOUT)
            if r.status_code == 200:
                d = r.json()
                snap = d.get("snapshot")
                delta = d.get("delta")

                if snap and "query_ms" in snap:
                    snap_query_ms.append(snap["query_ms"])
                if delta and "query_ms" in delta:
                    delta_query_ms.append(delta["query_ms"])
                    if "patches_applied" in delta:
                        patches_applied_list.append(delta["patches_applied"])

                # Consistency check
                if snap and delta:
                    consistency_total += 1
                    if snap.get("content") == delta.get("content"):
                        consistency_ok += 1
        except Exception as e:
            print(f"    ERROR query {i}: {e}")

        if (i+1) % 50 == 0:
            print(f"    {i+1}/{NUM_QUERIES} queries done")

    return snap_query_ms, delta_query_ms, patches_applied_list, consistency_ok, consistency_total


def get_storage_stats():
    """Get storage sizes from both nodes."""
    result = {}
    for name, url in [("Node A", NODE_A_URL), ("Node B", NODE_B_URL)]:
        try:
            r = requests.get(f"{url}/stats", timeout=5)
            if r.status_code == 200:
                result[name] = r.json()
        except Exception:
            pass
    return result


# ============================================================================
# Main
# ============================================================================

def main():
    print("\n" + "=" * 64)
    print("  AUTO BENCHMARK - Temporal Versioning in Distributed JSON")
    print("  Team ChronoJSON | Topic 79")
    print("=" * 64)

    # ── 0. Clean old data ──
    print("\n[0] Cleaning old data...")
    clean_node_data(NODE_A_DIR)
    clean_node_data(NODE_B_DIR)
    print("  Done")

    # ── 1. Start nodes ──
    print("\n[1] Starting nodes...")
    proc_a = start_node(NODE_A_DIR, "Node A (:5001)")
    proc_b = start_node(NODE_B_DIR, "Node B (:5002)")

    print("  Waiting for nodes to be ready...")
    time.sleep(2)
    a_ok = wait_for_health(NODE_A_URL, "Node A")
    b_ok = wait_for_health(NODE_B_URL, "Node B")

    if not a_ok:
        print("\n  FATAL: Node A not ready. Aborting.")
        stop_node(proc_a, "Node A")
        stop_node(proc_b, "Node B")
        sys.exit(1)

    try:
        # ── 2. Write experiment ──
        print("\n[2] WRITE EXPERIMENT")
        snap_w, delta_w, all_ts = run_writes()

        # Wait for replication
        print("\n  Waiting 3s for replication...")
        time.sleep(3)

        # ── 3. Query experiment ──
        print("\n[3] TIME-TRAVEL QUERY EXPERIMENT")
        snap_q, delta_q, patches_list, con_ok, con_total = run_queries(all_ts)

        # ── 4. Storage stats ──
        print("\n[4] STORAGE STATS")
        storage = get_storage_stats()

        # ── 5. Print results ──
        snap_w_avg = statistics.mean(snap_w) if snap_w else 0
        delta_w_avg = statistics.mean(delta_w) if delta_w else 0
        snap_q_avg = statistics.mean(snap_q) if snap_q else 0
        delta_q_avg = statistics.mean(delta_q) if delta_q else 0
        patches_avg = statistics.mean(patches_list) if patches_list else 0

        node_a = storage.get("Node A", {})
        snap_kb = node_a.get("snapshot_kb", 0)
        delta_kb = node_a.get("delta_kb", 0)
        savings = node_a.get("savings_pct", 0)

        print("\n")
        print("=" * 64)
        print("=== SO LIEU CHO BUOC 5 ===")
        print(f"- Full Snapshot write latency: {snap_w_avg:.2f} ms avg")
        print(f"- Delta write latency: {delta_w_avg:.2f} ms avg")
        print(f"- Full Snapshot time-travel query latency: {snap_q_avg:.3f} ms avg")
        print(f"- Delta time-travel query latency: {delta_q_avg:.3f} ms avg")
        print(f"- So deltas replay trung binh moi query: {patches_avg:.1f}")
        print(f"- Full Snapshot storage: {snap_kb} KB")
        print(f"- Delta storage: {delta_kb} KB")
        print(f"- Tiet kiem storage: {savings}%")
        print("=" * 64)

        # Extra details
        print(f"\n  Consistency: {con_ok}/{con_total} queries matched")
        if snap_q_avg > 0:
            print(f"  Query ratio (delta/snapshot): {delta_q_avg/snap_q_avg:.1f}x")
        print(f"  Total writes: {len(snap_w)}")
        print(f"  Total queries: {NUM_QUERIES}")

        # Node B stats
        node_b = storage.get("Node B", {})
        if node_b:
            print(f"\n  Node B snapshot: {node_b.get('snapshot_kb', 0)} KB")
            print(f"  Node B delta:    {node_b.get('delta_kb', 0)} KB")
            b_snap = node_b.get("snapshot_kb", 0)
            a_snap = snap_kb
            print(f"  Replication:     {'MATCH' if b_snap == a_snap else 'MISMATCH'}")

        # Save results JSON
        results_path = os.path.join(PROJECT_DIR, "benchmark_results.json")
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "config": {"docs": NUM_DOCS, "versions_per_doc": VERSIONS_PER_DOC},
                "write": {
                    "snapshot_avg_ms": round(snap_w_avg, 3),
                    "delta_avg_ms": round(delta_w_avg, 3),
                },
                "query": {
                    "snapshot_avg_ms": round(snap_q_avg, 3),
                    "delta_avg_ms": round(delta_q_avg, 3),
                    "patches_avg": round(patches_avg, 1),
                },
                "storage": storage,
                "consistency": f"{con_ok}/{con_total}",
            }, f, indent=2)
        print(f"\n  Results saved: {results_path}")

    finally:
        # ── 6. Stop nodes ──
        print("\n[6] Stopping nodes...")
        stop_node(proc_a, "Node A")
        stop_node(proc_b, "Node B")

    print("\n  Done!\n")


if __name__ == "__main__":
    main()
