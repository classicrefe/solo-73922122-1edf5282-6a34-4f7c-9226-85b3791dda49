#!/usr/bin/env python3
"""小时放行审计端到端演示（仅标准库，可在容器外直接运行）。

流程：
1. 生成 2 站 × 3 小时的整分采样 CSV；
2. 其中 STA01 的 01 小时刻意制造“同分钟异温度重传”，00 小时缺 3 个整分，
   STA02 三个小时全部完整；
3. 上传原件 → 受理小时审计 → 轮询至完成 → 下载 JSON 报告；
4. 断言：STA02 三个小时均放行且均值正确；STA01 的 00/01 小时阻断且无均值，
   02 小时放行；冲突时刻与缺测数符合预期。

用法：
  python scripts/demo_hour_audit.py                      # 默认 http://127.0.0.1:8000
  python scripts/demo_hour_audit.py http://host:8000
"""
from __future__ import annotations

import io
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8000"
HOUR = datetime(2026, 1, 1, tzinfo=timezone.utc)
HEADER = "station,utc_time,temperature"


def request(method: str, path: str, data: bytes | None = None,
            headers: dict | None = None, raw: bool = False):
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read()
            return body if raw else json.loads(body)
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"HTTP {exc.code} {path}: {exc.read().decode()}") from exc


def ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_csv() -> bytes:
    buf = io.StringIO()
    buf.write(HEADER + "\r\n")
    # STA02：三小时完整，温度 = 分钟号 + 小时偏移
    for h in range(3):
        for m in range(60):
            buf.write(f"STA02,{ts(HOUR + timedelta(hours=h, minutes=m))},{m + h * 100}\r\n")
    # STA01 00 小时：缺 5/17/58 分
    for m in range(60):
        if m in (5, 17, 58):
            continue
        buf.write(f"STA01,{ts(HOUR + timedelta(minutes=m))},{m}\r\n")
    # STA01 01 小时：23 分重传异温度（同分钟同温度重传不应影响）
    for m in range(60):
        buf.write(f"STA01,{ts(HOUR + timedelta(hours=1, minutes=m))},{m}\r\n")
        if m == 23:
            buf.write(f"STA01,{ts(HOUR + timedelta(hours=1, minutes=23))},999\r\n")
        if m == 40:
            # 同分钟同温度重传：只计一次
            buf.write(f"STA01,{ts(HOUR + timedelta(hours=1, minutes=40))},40\r\n")
    # STA01 02 小时：完整，乱序写入（验证原件可无序）
    lines = [
        f"STA01,{ts(HOUR + timedelta(hours=2, minutes=m))},{m}"
        for m in range(60)
    ]
    for line in reversed(lines):
        buf.write(line + "\r\n")
    return buf.getvalue().encode()


def upload(csv_bytes: bytes) -> str:
    boundary = "----demo" + uuid.uuid4().hex
    body = b"".join([
        f"--{boundary}\r\n".encode(),
        f'Content-Disposition: form-data; name="file"; filename="audit-demo.csv"\r\n'.encode(),
        b"Content-Type: text/csv\r\n\r\n",
        csv_bytes, b"\r\n",
        f"--{boundary}--\r\n".encode(),
    ])
    source = request("POST", "/sources", body,
                     {"Content-Type": f"multipart/form-data; boundary={boundary}"})
    return source["source_id"]


def wait_audit(audit_id: str, timeout: float = 120.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = request("GET", f"/hour-audits/{audit_id}", None)
        if info["status"] in ("completed", "failed", "interrupted"):
            return info
        time.sleep(0.2)
    raise SystemExit(f"审计 {audit_id} 超时未结束")


def main() -> None:
    csv_bytes = make_csv()
    print(f"[1] 上传 {len(csv_bytes.splitlines()) - 1} 行示例 CSV …")
    sid = upload(csv_bytes)
    print(f"    source_id={sid}")

    print("[2] 受理小时审计：2026-01-01T00:00Z .. 03:00Z（3 小时，UTC 整点）")
    audit = request(
        "POST", "/hour-audits",
        json.dumps({
            "source_id": sid,
            "range_start": "2026-01-01T00:00:00Z",
            "range_end": "2026-01-01T03:00:00Z",
        }).encode(),
        {"Content-Type": "application/json"},
    )
    aid = audit["audit_id"]
    print(f"    受理即返回：status={audit['status']} audit_id={aid}")

    info = wait_audit(aid)
    if info["status"] != "completed":
        raise SystemExit(f"审计未成功：{info}")

    report = request("GET", f"/hour-audits/{aid}/report", None, raw=True)
    report = json.loads(report)
    stations = {s["station"]: s for s in report["stations"]}

    print("[3] 核对报告")
    sta02 = stations["STA02"]
    assert sta02["releasable_hours"] == 3 and sta02["blocked_hours"] == 0
    for h in range(3):
        detail = sta02["hours_detail"][h]
        assert detail["status"] == "releasable"
        assert abs(detail["mean_temperature"] - (h * 100 + 29.5)) < 1e-6
    print("    STA02：3 小时全部放行，均值 29.5 / 129.5 / 229.5")

    sta01 = stations["STA01"]
    h0, h1, h2 = sta01["hours_detail"]
    assert h0["status"] == "blocked" and h0["missing_minutes"] == 3
    assert h0["mean_temperature"] is None
    assert h0["block_reasons"] == ["missing_samples"]
    assert h1["status"] == "blocked" and h1["mean_temperature"] is None
    assert h1["block_reasons"] == ["conflicting_retransmission"]
    assert h1["conflict_moments"] == [{
        "time": "2026-01-01T01:23:00Z",
        "temperatures": [23, 999],
        "distinct_temperatures": 2,
    }]
    assert h2["status"] == "releasable"
    assert abs(h2["mean_temperature"] - 29.5) < 1e-6
    print("    STA01：00 小时缺 3 分钟阻断；01 小时 01:23Z 异值冲突阻断；"
          "02 小时放行（原件乱序）")
    assert sta01["missing_minutes_total"] == 3
    print("[4] 报告结构、缺测数、冲突时刻与均值全部核对通过")
    print("OK：小时放行审计端到端演示完成。")


if __name__ == "__main__":
    main()
