"""
Node B — Backup Replica (port 5002)
Same logic as Node A, different config.
"""

import os, sys, json, time, logging, threading
import requests as http_client
from flask import Flask, request, jsonify
from storage import FullSnapshotStorage, DeltaStorage

NODE_NAME = "node_b"
PORT = 5002
PEER_URL = "http://127.0.0.1:5001"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_SNAP  = os.path.join(BASE_DIR, "snapshots.db")
DB_DELTA = os.path.join(BASE_DIR, "deltas.db")
SYNC_TIMEOUT = 3

app = Flask(__name__)
app.json.ensure_ascii = False

snap_store  = FullSnapshotStorage(db_path=DB_SNAP, data_dir=DATA_DIR)
delta_store = DeltaStorage(db_path=DB_DELTA, data_dir=DATA_DIR)

logging.basicConfig(
    level=logging.INFO,
    format=f"[{NODE_NAME.upper()}] %(asctime)s  %(message)s", datefmt="%H:%M:%S",
)
log = logging.getLogger(NODE_NAME)


def _replicate(payload, doc_id, version):
    try:
        r = http_client.post(f"{PEER_URL}/sync", json=payload, timeout=SYNC_TIMEOUT)
        if r.status_code == 200:
            log.info(f"Replicated {doc_id} v{version}")
    except Exception:
        pass


@app.route("/health")
def health():
    return jsonify({"status": "healthy", "node": NODE_NAME, "port": PORT})


@app.route("/document/<doc_id>", methods=["POST"])
def write_document(doc_id):
    data = request.get_json()
    if not data or "content" not in data or "author_id" not in data:
        return jsonify({"error": "Need content + author_id"}), 400

    content = data["content"]
    author = data["author_id"]
    timestamp = data.get("timestamp")
    title = data.get("title")

    prev = snap_store.get_latest(doc_id)
    prev_content = prev["content"] if prev else None

    t0 = time.perf_counter_ns()
    snap_res = snap_store.save_version(doc_id, content, timestamp, author, title)
    snap_ms = (time.perf_counter_ns() - t0) / 1e6

    t0 = time.perf_counter_ns()
    delta_res = delta_store.save_version(
        doc_id, content, timestamp, author, title, previous_content=prev_content
    )
    delta_ms = (time.perf_counter_ns() - t0) / 1e6

    _replicate({
        "doc_id": doc_id, "version": snap_res["version"],
        "timestamp": timestamp, "author_id": author,
        "content": content, "title": title,
        "previous_content": prev_content,
    }, doc_id, snap_res["version"])

    return jsonify({
        "doc_id": doc_id, "version": snap_res["version"], "timestamp": timestamp,
        "snapshot_write_ms": round(snap_ms, 3), "delta_write_ms": round(delta_ms, 3),
        "snapshot_size": snap_res["size_bytes"], "delta_size": delta_res["size_bytes"],
        "node": NODE_NAME,
    }), 201


@app.route("/document/<doc_id>", methods=["GET"])
def read_document(doc_id):
    ts = request.args.get("at")
    if ts:
        t0 = time.perf_counter_ns()
        snap = snap_store.get_at_time(doc_id, ts)
        snap_ms = (time.perf_counter_ns() - t0) / 1e6

        t0 = time.perf_counter_ns()
        delta = delta_store.get_at_time(doc_id, ts)
        delta_ms = (time.perf_counter_ns() - t0) / 1e6

        if not snap and not delta:
            return jsonify({"error": "No version found"}), 404
        return jsonify({
            "snapshot": {**(snap or {}), "query_ms": round(snap_ms, 3)} if snap else None,
            "delta": {**(delta or {}), "query_ms": round(delta_ms, 3)} if delta else None,
            "query": {"doc_id": doc_id, "at": ts, "node": NODE_NAME},
        })
    else:
        snap = snap_store.get_latest(doc_id)
        if not snap:
            return jsonify({"error": "Not found"}), 404
        return jsonify(snap)


@app.route("/document/<doc_id>/history")
def history(doc_id):
    return jsonify({
        "snapshot_history": snap_store.get_history(doc_id),
        "delta_history": delta_store.get_history(doc_id),
    })


@app.route("/sync", methods=["POST"])
def receive_sync():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Empty"}), 400
    prev = snap_store.get_latest(data["doc_id"])
    prev_content = prev["content"] if prev else data.get("previous_content")
    snap_store.save_version(
        data["doc_id"], data["content"], data["timestamp"],
        data["author_id"], data.get("title"),
    )
    delta_store.save_version(
        data["doc_id"], data["content"], data["timestamp"],
        data["author_id"], data.get("title"), previous_content=prev_content,
    )
    return jsonify({"status": "synced", "doc_id": data["doc_id"], "version": data["version"]})


@app.route("/stats")
def stats():
    snap_bytes = snap_store.get_storage_size()
    delta_bytes = delta_store.get_storage_size()
    return jsonify({
        "node": NODE_NAME,
        "snapshot_bytes": snap_bytes, "snapshot_kb": round(snap_bytes / 1024, 2),
        "delta_bytes": delta_bytes, "delta_kb": round(delta_bytes / 1024, 2),
        "savings_pct": round((1 - delta_bytes / snap_bytes) * 100, 1) if snap_bytes > 0 else 0,
    })


if __name__ == "__main__":
    log.info(f"Starting {NODE_NAME.upper()} on :{PORT} | Peer: {PEER_URL}")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
