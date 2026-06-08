# Đề tài 79 — Temporal Versioning in Distributed JSON
### Phiên bản hóa theo thời gian & Truy vấn ngược thời gian trong tài liệu JSON phân tán

**Môn học:** Cơ sở Dữ liệu Phân tán  
**Nhóm:** ChronoJSON  

---

## 1. Giới thiệu

Đồ án xây dựng hệ thống cơ sở dữ liệu phân tán 2 node hỗ trợ **lưu trữ phiên bản theo thời gian** cho tài liệu JSON, mô phỏng bài toán chỉnh sửa Wiki (Wiki Edits).

**Tính năng chính:**
- **Time-Travel Query** — Truy vấn ngược thời gian: truy xuất trạng thái tài liệu tại bất kỳ thời điểm nào trong quá khứ.
- **So sánh 2 chiến lược lưu trữ:**
  - **Full Snapshot** — Lưu toàn bộ tài liệu mỗi phiên bản → truy vấn O(1), tốn dung lượng.
  - **Delta Encoding (RFC 6902)** — Chỉ lưu phần thay đổi (JSON Patch) → tiết kiệm ~40% dung lượng, truy vấn O(N).
- **Sao chép bất đồng bộ** (Asynchronous Replication) giữa Node A (Primary) và Node B (Replica).
- **Xử lý sự cố** — Hàng đợi sao chép (pending queue) đảm bảo dữ liệu không bị mất khi node gặp lỗi.

---

## 2. Kiến trúc hệ thống

```
┌─────────────────────┐                              ┌─────────────────────┐
│      NODE A         │    HTTP/REST (Đồng bộ hóa)   │      NODE B         │
│   (Chính :5001)     │ ──────────────────────────▶   │   (Dự phòng :5002)  │
│                     │    POST /sync                 │                     │
│  ┌────────────────┐ │                               │  ┌────────────────┐ │
│  │ Tầng API Flask │ │                               │  │ Tầng API Flask │ │
│  └───────┬────────┘ │                               │  └───────┬────────┘ │
│  ┌───────┴────────┐ │                               │  ┌───────┴────────┐ │
│  │ JSON Files     │ │                               │  │ JSON Files     │ │
│  │ (dữ liệu)     │ │                               │  │ (dữ liệu)     │ │
│  ├────────────────┤ │                               │  ├────────────────┤ │
│  │ SQLite Index   │ │                               │  │ SQLite Index   │ │
│  │ (chỉ mục)     │ │                               │  │ (chỉ mục)     │ │
│  └────────────────┘ │                               │  └────────────────┘ │
└─────────────────────┘                               └─────────────────────┘
```

---

## 3. Cấu trúc thư mục

```
project79/
├── node_a/
│   ├── app.py              # Flask server — Node A (Primary, port 5001)
│   └── storage.py          # Storage Engine (FullSnapshotStorage + DeltaStorage)
├── node_b/
│   ├── app.py              # Flask server — Node B (Replica, port 5002)
│   └── storage.py          # Storage Engine (bản sao giống node_a)
├── client/
│   └── benchmark.py        # Script đo hiệu năng (benchmark)
├── dashboard/
│   └── index.html          # Giao diện web demo trực quan (mở bằng trình duyệt)
├── auto_benchmark.py       # Script tự động: khởi động 2 node + chạy benchmark
├── generate_dataset.py     # Sinh dữ liệu giả lập (50 tài liệu × 20 phiên bản)
├── requirements.txt        # Danh sách thư viện Python cần cài
├── .gitignore
└── README.md
```

---

## 4. Hướng dẫn cài đặt và chạy

### 4.1. Yêu cầu hệ thống
- Python 3.10 trở lên
- pip (trình quản lý gói Python)

### 4.2. Cài đặt thư viện

```bash
pip install -r requirements.txt
```

### 4.3. Sinh dữ liệu giả lập

```bash
python generate_dataset.py
```

Kết quả: tạo 2 file trong thư mục `data/`:
- `full_snapshots.json` — 1000 bản snapshot đầy đủ
- `delta_edits.json` — 1000 bản delta patch (RFC 6902)

### 4.4. Chạy Benchmark tự động (khuyến nghị)

```bash
python auto_benchmark.py
```

Script này sẽ tự động:
1. Khởi động Node A (port 5001) và Node B (port 5002).
2. Ghi 1000 phiên bản vào hệ thống qua REST API.
3. Thực hiện 200 truy vấn Time-Travel ngẫu nhiên.
4. In bảng so sánh hiệu năng giữa Full Snapshot và Delta Encoding.
5. Tắt cả 2 node sau khi hoàn tất.

### 4.5. Chạy thủ công từng bước

**Bước 1 — Khởi động 2 node (mở 2 terminal riêng biệt):**

```bash
# Terminal 1
cd node_a
python app.py

# Terminal 2
cd node_b
python app.py
```

**Bước 2 — Ghi dữ liệu qua API:**

```bash
curl -X POST http://localhost:5001/document/doc_001 \
  -H "Content-Type: application/json" \
  -d '{"title": "Test Project", "status": "draft", "requirements": ["Feature A"]}'
```

**Bước 3 — Truy vấn ngược thời gian (Time-Travel Query):**

```bash
curl "http://localhost:5001/document/doc_001?at=2024-01-15T10:00:00"
```

---

### 4.6. Mở giao diện Dashboard Demo

Sau khi 2 node đã chạy (bằng cách chạy `auto_benchmark.py` hoặc khởi động thủ công), mở file sau bằng trình duyệt (Chrome/Edge):

```
dashboard/index.html
```

Dashboard cung cấp:
- **Thống kê trực quan** — Tổng phiên bản, dung lượng Snapshot vs Delta (biểu đồ thanh)
- **Time-Travel Query** — Chọn tài liệu, chọn thời điểm, nhấn nút → xem kết quả + so sánh tốc độ 2 chiến lược
- **Lịch sử phiên bản** — Bảng chi tiết tất cả version của một tài liệu
- **Trạng thái Node** — Hiển thị xanh/đỏ cho biết node nào đang hoạt động

---

## 5. API Endpoints

| Phương thức | Endpoint | Mô tả |
|:---|:---|:---|
| `POST` | `/document/{id}` | Ghi phiên bản mới cho tài liệu |
| `GET` | `/document/{id}` | Lấy phiên bản mới nhất |
| `GET` | `/document/{id}?at={timestamp}` | **Time-Travel Query** — lấy trạng thái tại thời điểm chỉ định |
| `GET` | `/document/{id}/history` | Xem toàn bộ lịch sử phiên bản |
| `POST` | `/sync` | Nhận dữ liệu đồng bộ từ node khác (nội bộ) |
| `GET` | `/stats` | Thống kê dung lượng lưu trữ |

---

## 6. Kết quả thực nghiệm

Benchmark trên tập dữ liệu 50 tài liệu, tổng cộng 3241 phiên bản:

| Chỉ số | Full Snapshot | Delta Encoding | Nhận xét |
|:---|:---|:---|:---|
| **Dung lượng lưu trữ** | 1351.76 KB | 814.83 KB | Delta tiết kiệm **39.7%** |
| **Độ trễ ghi** | 22.92 ms | 14.25 ms | Delta nhanh hơn **37.8%** |
| **Độ trễ Time-Travel** | **6.26 ms** | 29.31 ms | Snapshot nhanh gấp **~4.7 lần** |
| **Cơ chế tái tạo** | O(1) Lookup | O(N) Replay | Delta phải áp dụng TB 42.8 patches |

---

## 7. Công nghệ sử dụng

| Công nghệ | Phiên bản | Vai trò |
|:---|:---|:---|
| Python | 3.10+ | Ngôn ngữ lập trình chính |
| Flask | 3.x | Web framework cho REST API |
| SQLite | Built-in | Chỉ mục dữ liệu (WAL mode) |
| jsonpatch | 1.x | Tính toán JSON Patch (RFC 6902) |
| requests | 2.x | Giao tiếp HTTP giữa các node |

---

## 8. Tài liệu tham khảo

1. Özsu, M. T., & Valduriez, P. (2020). *Principles of Distributed Database Systems*, 4th ed. Springer.
2. Brewer, E. (2000). "Towards robust distributed systems." ACM PODC.
3. Bryan, P., & Nottingham, M. (2013). RFC 6902 — JSON Patch. IETF.
4. Vogels, W. (2009). "Eventual Consistency." Communications of the ACM, 52(1), pp. 40-44.
5. SQLite Documentation — Write-Ahead Logging. https://www.sqlite.org/wal.html
