"""
Node A — Primary Coordinator (port 5001)
Flask REST API using FullSnapshotStorage + DeltaStorage
"""

import os, sys, json, time, logging, threading
import requests as http_client
from flask import Flask, request, jsonify
from storage import FullSnapshotStorage, DeltaStorage

# ── Config ──────────────────────────────────────────────────────────
NODE_NAME = "node_a"
PORT = 5001
PEER_URL = "http://127.0.0.1:5002"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_SNAP  = os.path.join(BASE_DIR, "snapshots.db")
DB_DELTA = os.path.join(BASE_DIR, "deltas.db")
SYNC_TIMEOUT = 3

# ── Init ────────────────────────────────────────────────────────────
app = Flask(__name__)
app.json.ensure_ascii = False

@app.after_request
def add_cors(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    resp.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
    return resp

snap_store  = FullSnapshotStorage(db_path=DB_SNAP, data_dir=DATA_DIR)
delta_store = DeltaStorage(db_path=DB_DELTA, data_dir=DATA_DIR)

logging.basicConfig(
    level=logging.INFO,
    format=f"[{NODE_NAME.upper()}] %(asctime)s  %(message)s", datefmt="%H:%M:%S",
)
log = logging.getLogger(NODE_NAME)


# ── Async replication ───────────────────────────────────────────────
def _replicate(payload, doc_id, version):
    try:
        r = http_client.post(f"{PEER_URL}/sync", json=payload, timeout=SYNC_TIMEOUT)
        if r.status_code == 200:
            log.info(f"Replicated {doc_id} v{version}")
    except Exception:
        pass  # peer down — acceptable under eventual consistency


# ── API ─────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "healthy", "node": NODE_NAME, "port": PORT})


@app.route("/document/<doc_id>", methods=["POST"])
def write_document(doc_id):
    """Write new version. Returns per-strategy latency."""
    data = request.get_json()
    if not data or "content" not in data or "author_id" not in data:
        return jsonify({"error": "Need content + author_id"}), 400

    content    = data["content"]
    author     = data["author_id"]
    timestamp  = data.get("timestamp")
    title      = data.get("title")

    # Get previous content for delta computation
    prev = snap_store.get_latest(doc_id)
    prev_content = prev["content"] if prev else None

    # ── Write Snapshot (timed) ──
    t0 = time.perf_counter_ns()
    snap_res = snap_store.save_version(doc_id, content, timestamp, author, title)
    snap_ms = (time.perf_counter_ns() - t0) / 1e6

    # ── Write Delta (timed) ──
    t0 = time.perf_counter_ns()
    delta_res = delta_store.save_version(
        doc_id, content, timestamp, author, title, previous_content=prev_content
    )
    delta_ms = (time.perf_counter_ns() - t0) / 1e6

    # Async replicate
    _replicate({
        "doc_id": doc_id, "version": snap_res["version"],
        "timestamp": timestamp, "author_id": author,
        "content": content, "title": title,
        "previous_content": prev_content,
    }, doc_id, snap_res["version"])

    return jsonify({
        "doc_id": doc_id,
        "version": snap_res["version"],
        "timestamp": timestamp,
        "snapshot_write_ms": round(snap_ms, 3),
        "delta_write_ms": round(delta_ms, 3),
        "snapshot_size": snap_res["size_bytes"],
        "delta_size": delta_res["size_bytes"],
        "delta_is_base": delta_res["is_base"],
        "node": NODE_NAME,
    }), 201


@app.route("/document/<doc_id>", methods=["GET"])
def read_document(doc_id):
    """Read document. ?at=ISO_timestamp for time-travel query."""
    ts = request.args.get("at")

    if ts:
        # ── Time-Travel: Snapshot (timed) ──
        t0 = time.perf_counter_ns()
        snap = snap_store.get_at_time(doc_id, ts)
        snap_ms = (time.perf_counter_ns() - t0) / 1e6

        # ── Time-Travel: Delta (timed) ──
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
    """Receive replicated version from peer."""
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
        "snapshot_versions": snap_store.get_version_count(),
        "snapshot_bytes": snap_bytes,
        "snapshot_kb": round(snap_bytes / 1024, 2),
        "delta_versions": delta_store.get_version_count(),
        "delta_bytes": delta_bytes,
        "delta_kb": round(delta_bytes / 1024, 2),
        "savings_pct": round((1 - delta_bytes / snap_bytes) * 100, 1) if snap_bytes > 0 else 0,
    })

DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ChronoJSON — Dashboard Demo</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a0e1a;--surface:#111827;--surface2:#1a2236;--border:#1e2d4a;--text:#e2e8f0;--text2:#94a3b8;--accent:#3b82f6;--accent2:#8b5cf6;--green:#10b981;--orange:#f59e0b;--red:#ef4444;--cyan:#06b6d4;--radius:12px}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden}
header{background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 50%,#0f172a 100%);border-bottom:1px solid var(--border);padding:20px 32px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;backdrop-filter:blur(12px)}
.logo{display:flex;align-items:center;gap:12px}
.logo-icon{width:40px;height:40px;background:linear-gradient(135deg,var(--accent),var(--accent2));border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:20px;box-shadow:0 4px 15px rgba(59,130,246,0.3)}
.logo h1{font-size:20px;font-weight:700}.logo h1 span{color:var(--accent)}
.logo-sub{font-size:12px;color:var(--text2)}
.node-status{display:flex;gap:12px}
.node-badge{display:flex;align-items:center;gap:6px;background:var(--surface);border:1px solid var(--border);padding:6px 14px;border-radius:20px;font-size:12px;font-weight:500}
.dot{width:8px;height:8px;border-radius:50%;background:var(--red)}
.dot.on{background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
main{padding:24px 32px;max-width:1400px;margin:0 auto}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}
.grid-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;margin-bottom:20px}
.full-width{margin-bottom:20px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px;transition:border-color .2s,box-shadow .2s}
.card:hover{border-color:rgba(59,130,246,.3);box-shadow:0 4px 20px rgba(59,130,246,.08)}
.card-title{font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;color:var(--text2);margin-bottom:16px;display:flex;align-items:center;gap:8px}
.card-title .icon{font-size:16px}
.stat-value{font-size:32px;font-weight:800;background:linear-gradient(135deg,var(--accent),var(--cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stat-label{font-size:13px;color:var(--text2);margin-top:4px}
.form-row{display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap}
label{display:block;font-size:12px;font-weight:500;color:var(--text2);margin-bottom:6px}
select,input[type="text"]{background:var(--surface2);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px 14px;font-size:14px;font-family:'Inter',sans-serif;outline:none;transition:border-color .2s;min-width:200px}
select:focus,input:focus{border-color:var(--accent)}
.btn{padding:10px 20px;border:none;border-radius:8px;font-size:14px;font-weight:600;font-family:'Inter',sans-serif;cursor:pointer;transition:all .2s;display:flex;align-items:center;gap:6px}
.btn-primary{background:linear-gradient(135deg,var(--accent),var(--accent2));color:white;box-shadow:0 4px 12px rgba(59,130,246,.3)}
.btn-primary:hover{transform:translateY(-1px);box-shadow:0 6px 20px rgba(59,130,246,.4)}
.btn-secondary{background:var(--surface2);color:var(--text);border:1px solid var(--border)}
.btn-secondary:hover{border-color:var(--accent)}
.compare-row{display:flex;align-items:center;gap:12px;margin-bottom:14px}
.compare-label{width:120px;font-size:13px;font-weight:500;flex-shrink:0}
.bar-track{flex:1;height:28px;background:var(--surface2);border-radius:6px;overflow:hidden;position:relative}
.bar-fill{height:100%;border-radius:6px;display:flex;align-items:center;padding-left:10px;font-size:12px;font-weight:600;color:white;transition:width .8s cubic-bezier(.16,1,.3,1);min-width:50px}
.bar-snap{background:linear-gradient(90deg,#3b82f6,#60a5fa)}
.bar-delta{background:linear-gradient(90deg,#8b5cf6,#a78bfa)}
.json-viewer{background:#0d1117;border:1px solid var(--border);border-radius:8px;padding:16px;font-family:'Consolas','Monaco',monospace;font-size:13px;line-height:1.6;max-height:400px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.json-key{color:#7dd3fc}.json-str{color:#a5d6a7}.json-num{color:#ffab91}.json-bool{color:#ce93d8}.json-null{color:#90a4ae}
.history-table{width:100%;border-collapse:collapse;font-size:13px}
.history-table th{text-align:left;padding:10px 12px;background:var(--surface2);font-weight:600;color:var(--text2);border-bottom:1px solid var(--border);font-size:12px;text-transform:uppercase;letter-spacing:.3px}
.history-table td{padding:10px 12px;border-bottom:1px solid rgba(30,45,74,.5)}
.history-table tr:hover td{background:rgba(59,130,246,.05)}
.tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}
.tag-snap{background:rgba(59,130,246,.15);color:#60a5fa}
.tag-delta{background:rgba(139,92,246,.15);color:#a78bfa}
.latency-badge{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;border-radius:6px;font-size:14px;font-weight:700;font-family:'Consolas',monospace}
.latency-fast{background:rgba(16,185,129,.12);color:var(--green)}
.latency-slow{background:rgba(245,158,11,.12);color:var(--orange)}
.empty-state{text-align:center;padding:40px;color:var(--text2);font-size:14px}
.spinner{width:24px;height:24px;border:3px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .8s linear infinite;margin:0 auto 12px}
@keyframes spin{to{transform:rotate(360deg)}}
@media(max-width:900px){.grid-2,.grid-3{grid-template-columns:1fr}header{flex-direction:column;gap:12px}main{padding:16px}.form-row{flex-direction:column}select,input[type="text"]{min-width:100%}}
</style>
</head>
<body>
<header>
  <div class="logo">
    <div class="logo-icon">&#9201;</div>
    <div>
      <h1>Chrono<span>JSON</span></h1>
      <div class="logo-sub">&#272;&#7873; t&#224;i 79 &#8212; Temporal Versioning in Distributed JSON</div>
    </div>
  </div>
  <div class="node-status">
    <div class="node-badge"><div class="dot" id="dotA"></div>Node A :5001</div>
    <div class="node-badge"><div class="dot" id="dotB"></div>Node B :5002</div>
  </div>
</header>
<main>
  <div class="grid-3">
    <div class="card">
      <div class="card-title"><span class="icon">&#128196;</span> T&#7893;ng phi&#234;n b&#7843;n</div>
      <div class="stat-value" id="totalVersions">&#8212;</div>
      <div class="stat-label">versions &#273;ang l&#432;u tr&#7919;</div>
    </div>
    <div class="card">
      <div class="card-title"><span class="icon">&#128190;</span> Dung l&#432;&#7907;ng Snapshot</div>
      <div class="stat-value" id="snapSize">&#8212;</div>
      <div class="stat-label" id="snapSizeBytes"></div>
    </div>
    <div class="card">
      <div class="card-title"><span class="icon">&#128230;</span> Dung l&#432;&#7907;ng Delta</div>
      <div class="stat-value" id="deltaSize">&#8212;</div>
      <div class="stat-label" id="deltaSizeDetail"></div>
    </div>
  </div>
  <div class="card full-width">
    <div class="card-title"><span class="icon">&#128202;</span> So s&#225;nh dung l&#432;&#7907;ng l&#432;u tr&#7919;</div>
    <div class="compare-row">
      <div class="compare-label">Full Snapshot</div>
      <div class="bar-track"><div class="bar-fill bar-snap" id="barSnap" style="width:0%"></div></div>
    </div>
    <div class="compare-row">
      <div class="compare-label">Delta Encoding</div>
      <div class="bar-track"><div class="bar-fill bar-delta" id="barDelta" style="width:0%"></div></div>
    </div>
    <div style="text-align:center;margin-top:8px">
      <span style="font-size:14px;color:var(--green);font-weight:700" id="savingsText"></span>
    </div>
  </div>
  <div class="card full-width">
    <div class="card-title"><span class="icon">&#128368;</span> Truy v&#7845;n ng&#432;&#7907;c th&#7901;i gian (Time-Travel Query)</div>
    <div class="form-row" style="margin-bottom:20px">
      <div><label>T&#224;i li&#7879;u (Document ID)</label><select id="docSelect"></select></div>
      <div><label>Th&#7901;i &#273;i&#7875;m truy v&#7845;n</label><input type="text" id="queryTime" value="2024-01-15T10:00:00"></div>
      <div><label>Node</label><select id="nodeSelect"><option value="5001">Node A (:5001)</option><option value="5002">Node B (:5002)</option></select></div>
      <button class="btn btn-primary" onclick="doTimeTravelQuery()">&#9201; Truy v&#7845;n</button>
    </div>
    <div id="latencyResult" style="display:none">
      <div class="grid-2" style="margin-bottom:16px">
        <div style="text-align:center;padding:16px;background:var(--surface2);border-radius:8px">
          <div style="font-size:12px;color:var(--text2);margin-bottom:6px">&#9889; Full Snapshot</div>
          <div class="latency-badge latency-fast" id="snapLatency"></div>
          <div style="font-size:11px;color:var(--text2);margin-top:6px">Lookup tr&#7921;c ti&#7871;p &#8212; O(1)</div>
        </div>
        <div style="text-align:center;padding:16px;background:var(--surface2);border-radius:8px">
          <div style="font-size:12px;color:var(--text2);margin-bottom:6px">&#128295; Delta Encoding</div>
          <div class="latency-badge latency-slow" id="deltaLatency"></div>
          <div style="font-size:11px;color:var(--text2);margin-top:6px" id="patchesInfo"></div>
        </div>
      </div>
    </div>
    <div id="queryResultArea">
      <div class="empty-state">Ch&#7885;n t&#224;i li&#7879;u v&#224; nh&#7845;n <b>Truy v&#7845;n</b> &#273;&#7875; xem k&#7871;t qu&#7843;</div>
    </div>
  </div>
  <div class="card full-width">
    <div class="card-title"><span class="icon">&#128220;</span> L&#7883;ch s&#7917; phi&#234;n b&#7843;n</div>
    <div class="form-row" style="margin-bottom:16px">
      <div><label>T&#224;i li&#7879;u</label><select id="histDocSelect"></select></div>
      <button class="btn btn-secondary" onclick="loadHistory()">&#128220; Xem l&#7883;ch s&#7917;</button>
    </div>
    <div id="historyArea">
      <div class="empty-state">Ch&#7885;n t&#224;i li&#7879;u v&#224; nh&#7845;n <b>Xem l&#7883;ch s&#7917;</b></div>
    </div>
  </div>
</main>
<script>
const BASE=location.origin;
function formatJson(o){const s=JSON.stringify(o,null,2);return s.replace(/(".*?")(\\s*:\\s*)/g,'<span class="json-key">$1</span>$2').replace(/:\\s*(".*?")/g,': <span class="json-str">$1</span>').replace(/:\\s*(\\d+\\.?\\d*)/g,': <span class="json-num">$1</span>').replace(/:\\s*(true|false)/g,': <span class="json-bool">$1</span>').replace(/:\\s*(null)/g,': <span class="json-null">$1</span>')}
async function api(port,path){const r=await fetch(`http://localhost:${port}${path}`);return r.json()}
async function checkNodes(){for(const[p,d]of[['5001','dotA'],['5002','dotB']]){try{await fetch(`http://localhost:${p}/health`,{signal:AbortSignal.timeout(2000)});document.getElementById(d).classList.add('on')}catch{document.getElementById(d).classList.remove('on')}}}
async function loadStats(){try{const d=await api(5001,'/stats');document.getElementById('totalVersions').textContent=d.snapshot_versions?.toLocaleString()||'—';document.getElementById('snapSize').textContent=d.snapshot_kb+' KB';document.getElementById('snapSizeBytes').textContent=d.snapshot_bytes?.toLocaleString()+' bytes';document.getElementById('deltaSize').textContent=d.delta_kb+' KB';document.getElementById('deltaSizeDetail').textContent='Ti\\u1ebft ki\\u1ec7m '+d.savings_pct+'% so v\\u1edbi Snapshot';const max=d.snapshot_kb;document.getElementById('barSnap').style.width='100%';document.getElementById('barSnap').textContent=d.snapshot_kb+' KB';const pct=(d.delta_kb/max*100).toFixed(0);document.getElementById('barDelta').style.width=pct+'%';document.getElementById('barDelta').textContent=d.delta_kb+' KB';document.getElementById('savingsText').textContent='\\u2705 Delta Encoding ti\\u1ebft ki\\u1ec7m '+d.savings_pct+'% dung l\\u01b0\\u1ee3ng l\\u01b0u tr\\u1eef'}catch{}}
function populateDocSelects(){const o=[];for(let i=1;i<=50;i++){const id='doc_'+String(i).padStart(3,'0');o.push(`<option value="${id}">${id}</option>`)}document.getElementById('docSelect').innerHTML=o.join('');document.getElementById('histDocSelect').innerHTML=o.join('')}
async function doTimeTravelQuery(){const docId=document.getElementById('docSelect').value;const ts=document.getElementById('queryTime').value;const port=document.getElementById('nodeSelect').value;const area=document.getElementById('queryResultArea');area.innerHTML='<div class="empty-state"><div class="spinner"></div>\\u0110ang truy v\\u1ea5n...</div>';try{const d=await api(port,`/document/${docId}?at=${ts}`);document.getElementById('latencyResult').style.display='block';document.getElementById('snapLatency').textContent=(d.snapshot?.query_ms||0).toFixed(3)+' ms';document.getElementById('deltaLatency').textContent=(d.delta?.query_ms||0).toFixed(3)+' ms';document.getElementById('patchesInfo').textContent='Replay '+(d.delta?.patches_applied||0)+' patches \\u2014 O(N)';const c=d.snapshot?.content||d.delta?.content||{};area.innerHTML='<div style="margin-bottom:8px;font-size:13px;color:var(--text2)">\\ud83d\\udcc4 Phi\\u00ean b\\u1ea3n <b>'+(d.snapshot?.version||'?')+'</b> \\u00b7 Th\\u1eddi \\u0111i\\u1ec3m ghi: <b>'+(d.snapshot?.timestamp||'?')+'</b> \\u00b7 T\\u00e1c gi\\u1ea3: <b>'+(d.snapshot?.author_id||'?')+'</b></div><div class="json-viewer">'+formatJson(c)+'</div>'}catch{area.innerHTML='<div class="empty-state" style="color:var(--red)">\\u274c L\\u1ed7i k\\u1ebft n\\u1ed1i. H\\u00e3y \\u0111\\u1ea3m b\\u1ea3o Node \\u0111ang ch\\u1ea1y.</div>';document.getElementById('latencyResult').style.display='none'}}
async function loadHistory(){const docId=document.getElementById('histDocSelect').value;const area=document.getElementById('historyArea');area.innerHTML='<div class="empty-state"><div class="spinner"></div>\\u0110ang t\\u1ea3i...</div>';try{const d=await api(5001,`/document/${docId}/history`);const rows=(d.snapshot_history||[]).map((s,i)=>{const dl=d.delta_history?.[i];return`<tr><td style="font-weight:600">v${s.version}</td><td>${s.timestamp}</td><td>${s.author_id}</td><td><span class="tag tag-snap">${s.size_bytes} B</span></td><td><span class="tag tag-delta">${dl?.size_bytes||'\\u2014'} B</span></td><td>${dl?.is_base?'\\ud83d\\udcf8 Base':'\\ud83d\\udd27 Patch'}</td></tr>`}).join('');area.innerHTML=`<div style="max-height:350px;overflow-y:auto;border-radius:8px;border:1px solid var(--border)"><table class="history-table"><thead><tr><th>Version</th><th>Th\\u1eddi \\u0111i\\u1ec3m</th><th>T\\u00e1c gi\\u1ea3</th><th>Snapshot Size</th><th>Delta Size</th><th>Lo\\u1ea1i</th></tr></thead><tbody>${rows}</tbody></table></div>`}catch{area.innerHTML='<div class="empty-state" style="color:var(--red)">\\u274c L\\u1ed7i k\\u1ebft n\\u1ed1i</div>'}}
populateDocSelects();checkNodes();loadStats();setInterval(checkNodes,5000);
</script>
</body>
</html>'''


@app.route("/dashboard")
def dashboard():
    """Serve the visual demo dashboard."""
    return DASHBOARD_HTML, 200, {'Content-Type': 'text/html; charset=utf-8'}


if __name__ == "__main__":
    log.info(f"Starting {NODE_NAME.upper()} on :{PORT} | Peer: {PEER_URL}")
    log.info(f"Dashboard: http://localhost:{PORT}/dashboard")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)

