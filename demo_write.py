"""
Demo Write Script — Ghi 5 phiên bản của document demo_001
Chạy: python demo_write.py
"""
import requests, json, time

BASE = "http://127.0.0.1:5001"

versions = [
    {
        "timestamp": "2024-01-10T09:00:00",
        "author_id": "alice.nguyen",
        "title": "Cloud Migration",
        "content": {
            "title": "Cloud Migration",
            "description": "This document outlines the comprehensive strategy and technical requirements for migrating our legacy monolith application to a modern, scalable cloud-native architecture. The primary objectives include improving system reliability to meet our 99.99% uptime SLA, enhancing security through end-to-end encryption, and ensuring full compliance with GDPR regulations. The architecture will leverage containerization via Docker and orchestration through Kubernetes, allowing for horizontal auto-scaling during peak loads. We will also implement a robust CI/CD pipeline to automate testing and deployment processes. This migration is critical for our long-term business goals and requires careful coordination among all engineering teams to minimize downtime.",
            "status": "draft",
            "priority": "medium",
            "requirements": ["OAuth 2.0", "99.99% uptime"],
            "assignees": ["alice.nguyen", "bob.tran"],
        },
    },
    {
        "timestamp": "2024-01-12T14:30:00",
        "author_id": "bob.tran",
        "title": "Cloud Migration",
        "content": {
            "title": "Cloud Migration",
            "description": "This document outlines the comprehensive strategy and technical requirements for migrating our legacy monolith application to a modern, scalable cloud-native architecture. The primary objectives include improving system reliability to meet our 99.99% uptime SLA, enhancing security through end-to-end encryption, and ensuring full compliance with GDPR regulations. The architecture will leverage containerization via Docker and orchestration through Kubernetes, allowing for horizontal auto-scaling during peak loads. We will also implement a robust CI/CD pipeline to automate testing and deployment processes. This migration is critical for our long-term business goals and requires careful coordination among all engineering teams to minimize downtime.",
            "status": "in_review",
            "priority": "medium",
            "requirements": ["OAuth 2.0", "99.99% uptime", "E2E encryption"],
            "assignees": ["alice.nguyen", "bob.tran"],
        },
    },
    {
        "timestamp": "2024-01-15T10:00:00",
        "author_id": "charlie.le",
        "title": "Cloud Migration",
        "content": {
            "title": "Cloud Migration",
            "description": "This document outlines the comprehensive strategy and technical requirements for migrating our legacy monolith application to a modern, scalable cloud-native architecture. The primary objectives include improving system reliability to meet our 99.99% uptime SLA, enhancing security through end-to-end encryption, and ensuring full compliance with GDPR regulations. The architecture will leverage containerization via Docker and orchestration through Kubernetes, allowing for horizontal auto-scaling during peak loads. We will also implement a robust CI/CD pipeline to automate testing and deployment processes. This migration is critical for our long-term business goals and requires careful coordination among all engineering teams to minimize downtime.",
            "status": "approved",
            "priority": "high",
            "requirements": ["OAuth 2.0", "99.99% uptime", "E2E encryption", "GDPR compliance"],
            "assignees": ["alice.nguyen", "bob.tran", "charlie.le"],
        },
    },
    {
        "timestamp": "2024-01-18T16:00:00",
        "author_id": "diana.pham",
        "title": "Cloud Migration v2",
        "content": {
            "title": "Cloud Migration v2",
            "description": "This document outlines the comprehensive strategy and technical requirements for migrating our legacy monolith application to a modern, scalable cloud-native architecture. The primary objectives include improving system reliability to meet our 99.99% uptime SLA, enhancing security through end-to-end encryption, and ensuring full compliance with GDPR regulations. The architecture will leverage containerization via Docker and orchestration through Kubernetes, allowing for horizontal auto-scaling during peak loads. We will also implement a robust CI/CD pipeline to automate testing and deployment processes. This migration is critical for our long-term business goals and requires careful coordination among all engineering teams to minimize downtime.",
            "status": "in_progress",
            "priority": "high",
            "requirements": ["OAuth 2.0", "99.99% uptime", "E2E encryption", "GDPR compliance", "Rate limiting"],
            "assignees": ["alice.nguyen", "bob.tran", "charlie.le", "diana.pham"],
        },
    },
    {
        "timestamp": "2024-01-22T11:00:00",
        "author_id": "alice.nguyen",
        "title": "Cloud Migration v2",
        "content": {
            "title": "Cloud Migration v2",
            "description": "This document outlines the comprehensive strategy and technical requirements for migrating our legacy monolith application to a modern, scalable cloud-native architecture. The primary objectives include improving system reliability to meet our 99.99% uptime SLA, enhancing security through end-to-end encryption, and ensuring full compliance with GDPR regulations. The architecture will leverage containerization via Docker and orchestration through Kubernetes, allowing for horizontal auto-scaling during peak loads. We will also implement a robust CI/CD pipeline to automate testing and deployment processes. This migration is critical for our long-term business goals and requires careful coordination among all engineering teams to minimize downtime.",
            "status": "deployed",
            "priority": "critical",
            "requirements": ["OAuth 2.0", "99.99% uptime", "E2E encryption", "GDPR compliance", "Rate limiting", "Monitoring"],
            "assignees": ["alice.nguyen", "bob.tran", "charlie.le", "diana.pham"],
        },
    },
]

doc_id = "doc_001"

print("\n" + "=" * 60)
print(f"  DEMO WRITE — 5 versions cua document {doc_id}")
print("=" * 60)

for i, v in enumerate(versions, 1):
    r = requests.post(f"{BASE}/document/{doc_id}", json=v, timeout=10)
    d = r.json()
    print(f"\n  Version {i}: {v['content']['status']}")
    print(f"    Snapshot size: {d['snapshot_size']} bytes")
    print(f"    Delta size:    {d['delta_size']} bytes")
    print(f"    Snapshot write: {d['snapshot_write_ms']:.3f} ms")
    print(f"    Delta write:    {d['delta_write_ms']:.3f} ms")
    if i > 1:
        saving = round((1 - d['delta_size'] / d['snapshot_size']) * 100, 1)
        print(f"    >>> Delta tiet kiem {saving}% so voi Snapshot")
    else:
        print(f"    >>> Base version — ca 2 bang nhau")
    time.sleep(0.5)

print(f"\n  Done! Mo Dashboard tai: http://localhost:5001/dashboard")
print(f"  Chon document '{doc_id}' de xem lich su + time-travel.\n")
