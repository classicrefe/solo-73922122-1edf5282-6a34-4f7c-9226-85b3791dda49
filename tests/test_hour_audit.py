"""小时放行审计的接口与关键语义测试。"""
from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone

from helpers import HEADER, create_task, make_csv, upload, wait_terminal

HOUR = datetime(2026, 1, 1, tzinfo=timezone.utc)


def ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def build_csv(lines: list[str]) -> bytes:
    return ("\n".join([HEADER] + lines) + "\n").encode()


def hourly_lines(
    station: str,
    hour: datetime,
    temps: dict[int, int],
) -> list[str]:
    return [f"{station},{ts(hour + timedelta(minutes=m))},{t}" for m, t in sorted(temps.items())]


def full_hour(station: str, hour: datetime, start_temp: int) -> list[str]:
    return hourly_lines(station, hour, {m: start_temp + m for m in range(60)})


def create_audit(client, source_id, start, end):
    return client.post(
        "/hour-audits",
        json={
            "source_id": source_id,
            "range_start": start,
            "range_end": end,
        },
    )


def wait_audit(client, audit_id, timeout=30.0):
    deadline = time.monotonic() + timeout
    info = None
    while time.monotonic() < deadline:
        info = client.get(f"/hour-audits/{audit_id}").json()
        if info["status"] in ("completed", "failed", "interrupted"):
            return info
        time.sleep(0.05)
    raise AssertionError(f"audit did not finish: {info}")


def report_of(client, audit_id):
    resp = client.get(f"/hour-audits/{audit_id}/report")
    assert resp.status_code == 200, resp.text
    return resp.json()


def station_entry(report, name):
    for entry in report["stations"]:
        if entry["station"] == name:
            return entry
    raise AssertionError(f"station {name} missing from report")


def hour_entry(entry, hour_iso):
    for detail in entry["hours_detail"]:
        if detail["hour"] == hour_iso:
            return detail
    raise AssertionError(f"hour {hour_iso} missing")


# ---------------------------------------------------------------- 正常路径
def test_full_hours_are_releasable_with_mean(client):
    hour = HOUR
    lines = (
        full_hour("STA01", hour, 0)       # 0..59 -> mean 29.5
        + full_hour("STA01", hour + timedelta(hours=1), 100)  # 100..159
        + full_hour("STA01", hour + timedelta(hours=2), -30)
    )
    sid = upload(client, build_csv(lines)).json()["source_id"]
    resp = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T03:00:00Z"
    )
    assert resp.status_code == 202
    audit_id = resp.json()["audit_id"]
    info = wait_audit(client, audit_id)
    assert info["status"] == "completed"
    assert info["report_available"] is True

    report = report_of(client, audit_id)
    assert report["range_start"] == "2026-01-01T00:00:00Z"
    assert report["range_end"] == "2026-01-01T03:00:00Z"
    assert report["hours"] == 3
    entry = station_entry(report, "STA01")
    assert entry["releasable_hours"] == 3
    assert entry["blocked_hours"] == 0
    assert entry["missing_minutes_total"] == 0
    first = hour_entry(entry, "2026-01-01T00:00:00Z")
    assert first["status"] == "releasable"
    assert first["missing_minutes"] == 0
    assert first["conflict_moments"] == []
    assert math.isclose(first["mean_temperature"], 29.5)
    second = hour_entry(entry, "2026-01-01T01:00:00Z")
    assert math.isclose(second["mean_temperature"], (100 + 159) / 2)


def test_partial_hour_missing_minutes_blocked_no_mean(client):
    hour = HOUR
    temps = {m: m for m in range(60) if m not in (5, 17, 58)}
    sid = upload(client, build_csv(hourly_lines("STA01", hour, temps))).json()["source_id"]
    audit_id = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
    ).json()["audit_id"]
    assert wait_audit(client, audit_id)["status"] == "completed"

    entry = station_entry(report_of(client, audit_id), "STA01")
    detail = hour_entry(entry, "2026-01-01T00:00:00Z")
    assert detail["status"] == "blocked"
    assert detail["missing_minutes"] == 3
    assert detail["block_reasons"] == ["missing_samples"]
    assert detail["mean_temperature"] is None
    assert entry["missing_minutes_total"] == 3
    assert entry["releasable_hours"] == 0


def test_same_minute_same_temp_retransmission_counts_once(client):
    hour = HOUR
    lines = full_hour("STA01", hour, 10)
    # 同分钟同温度重传：不得影响样本数或均值
    lines.append(f"STA01,{ts(hour + timedelta(minutes=7))},17")
    lines.append(f"STA01,{ts(hour + timedelta(minutes=7))},17")
    sid = upload(client, build_csv(lines)).json()["source_id"]
    audit_id = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
    ).json()["audit_id"]
    assert wait_audit(client, audit_id)["status"] == "completed"

    entry = station_entry(report_of(client, audit_id), "STA01")
    detail = hour_entry(entry, "2026-01-01T00:00:00Z")
    assert detail["status"] == "releasable"
    assert detail["missing_minutes"] == 0
    assert detail["conflict_moments"] == []
    assert math.isclose(detail["mean_temperature"], (10 + 69) / 2)


def test_conflicting_temperature_blocks_hour(client):
    hour = HOUR
    lines = full_hour("STA01", hour, 0)
    lines.append(f"STA01,{ts(hour + timedelta(minutes=23))},999")
    # 另一小时完全正常，仍应放行
    lines += full_hour("STA01", hour + timedelta(hours=1), 50)
    sid = upload(client, build_csv(lines)).json()["source_id"]
    audit_id = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"
    ).json()["audit_id"]
    assert wait_audit(client, audit_id)["status"] == "completed"

    entry = station_entry(report_of(client, audit_id), "STA01")
    blocked = hour_entry(entry, "2026-01-01T00:00:00Z")
    assert blocked["status"] == "blocked"
    assert blocked["missing_minutes"] == 0
    assert blocked["mean_temperature"] is None
    assert blocked["block_reasons"] == ["conflicting_retransmission"]
    assert blocked["conflict_moments"] == [
        {
            "time": "2026-01-01T00:23:00Z",
            "temperatures": [23, 999],
            "distinct_temperatures": 2,
        }
    ]
    assert hour_entry(entry, "2026-01-01T01:00:00Z")["status"] == "releasable"
    assert entry["releasable_hours"] == 1 and entry["blocked_hours"] == 1


def test_non_minute_timestamp_blocks_hour_and_is_reported(client):
    hour = HOUR
    lines = full_hour("STA01", hour, 0)
    odd_ts = "2026-01-01T00:41:30+00:00"
    lines.append(f"STA01,{odd_ts},41")
    sid = upload(client, build_csv(lines)).json()["source_id"]
    audit_id = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
    ).json()["audit_id"]
    assert wait_audit(client, audit_id)["status"] == "completed"

    detail = hour_entry(station_entry(report_of(client, audit_id), "STA01"), "2026-01-01T00:00:00Z")
    assert detail["status"] == "blocked"
    assert detail["block_reasons"] == ["non_minute_timestamp"]
    assert detail["non_minute_moments"] == [
        {"time": "2026-01-01T00:41:30Z", "count": 1}
    ]
    assert detail["non_minute_moments_total"] == 1
    assert detail["mean_temperature"] is None


def test_utc_normalization_and_utc_only_hour_boundaries(client):
    """带 +08:00 偏移的时刻按 UTC 归一化；13:00+08:00 == 05:00Z。"""
    lines = full_hour("STA01", HOUR + timedelta(hours=5), 3)
    # 把其中一部分改成 +08:00 写法（同一 UTC 时刻）
    rewritten = []
    for line in lines:
        station, time_text, temp = line.split(",")
        dt = datetime.strptime(time_text, "%Y-%m-%dT%H:%M:%S+00:00").replace(
            tzinfo=timezone.utc
        )
        rewritten.append(
            f"{station},{(dt + timedelta(hours=8)).strftime('%Y-%m-%dT%H:%M:%S+08:00')},{temp}"
        )
    sid = upload(client, build_csv(rewritten)).json()["source_id"]

    # 请求窗口也用 +08:00 表达，归一化后是 05:00Z..06:00Z
    resp = create_audit(
        client, sid, "2026-01-01T13:00:00+08:00", "2026-01-01T14:00:00+08:00"
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["range_start"] == "2026-01-01T05:00:00Z"
    assert body["range_end"] == "2026-01-01T06:00:00Z"
    assert wait_audit(client, body["audit_id"])["status"] == "completed"

    report = report_of(client, body["audit_id"])
    entry = station_entry(report, "STA01")
    detail = hour_entry(entry, "2026-01-01T05:00:00Z")
    assert detail["status"] == "releasable"
    assert math.isclose(detail["mean_temperature"], (3 + 62) / 2)


def test_window_filters_out_other_hours_and_stations(client):
    lines = (
        full_hour("STA01", HOUR, 0)                        # 窗口外小时
        + full_hour("STA02", HOUR + timedelta(hours=1), 10)
        + full_hour("STA03", HOUR - timedelta(hours=1), 20)  # 窗口外
    )
    sid = upload(client, build_csv(lines)).json()["source_id"]
    audit_id = create_audit(
        client, sid, "2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z"
    ).json()["audit_id"]
    assert wait_audit(client, audit_id)["status"] == "completed"

    report = report_of(client, audit_id)
    stations = {e["station"] for e in report["stations"]}
    # 只在窗口外出现的站号不进入报告
    assert stations == {"STA02"}
    entry = station_entry(report, "STA02")
    assert len(entry["hours_detail"]) == 1
    detail = entry["hours_detail"][0]
    assert detail["hour"] == "2026-01-01T01:00:00Z"
    assert detail["status"] == "releasable"
    assert math.isclose(detail["mean_temperature"], (10 + 69) / 2)

    # 窗口内只出现部分小时的站号，其余小时按整小时缺测阻断
    audit_id2 = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"
    ).json()["audit_id"]
    assert wait_audit(client, audit_id2)["status"] == "completed"
    report2 = report_of(client, audit_id2)
    entry2 = station_entry(report2, "STA02")
    assert [h["hour"] for h in entry2["hours_detail"]] == [
        "2026-01-01T00:00:00Z",
        "2026-01-01T01:00:00Z",
    ]
    assert entry2["hours_detail"][0]["status"] == "blocked"
    assert entry2["hours_detail"][0]["missing_minutes"] == 60
    assert entry2["hours_detail"][1]["status"] == "releasable"
    # STA03 只出现在窗口前的小时（2025-12-31T23:00Z），不进入报告
    assert "STA03" not in {e["station"] for e in report2["stations"]}


def test_unordered_input_and_full_file_scan(client):
    """原件可无序：乱序不影响结果；窗口外的非法行也导致整次审计失败。"""
    hour = HOUR
    lines = full_hour("STA01", hour, 0)
    lines.reverse()
    sid = upload(client, build_csv(lines)).json()["source_id"]
    audit_id = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
    ).json()["audit_id"]
    assert wait_audit(client, audit_id)["status"] == "completed"
    detail = hour_entry(station_entry(report_of(client, audit_id), "STA01"), "2026-01-01T00:00:00Z")
    assert detail["status"] == "releasable"
    assert math.isclose(detail["mean_temperature"], 29.5)


# ---------------------------------------------------------------- 校验与失败
def test_invalid_range_rejected(client):
    sid = upload(client, make_csv(3)).json()["source_id"]
    assert create_audit(client, sid, "2026-01-01T00:30:00Z", "2026-01-01T01:00:00Z").status_code == 400
    assert create_audit(client, sid, "2026-01-01T01:00:00Z", "2026-01-01T01:00:00Z").status_code == 400
    assert create_audit(client, sid, "2026-01-01T00:00:00Z", "2026-01-02T01:00:00Z").status_code == 400
    assert create_audit(client, sid, "not-a-time", "2026-01-01T01:00:00Z").status_code == 400
    assert create_audit(client, "nope", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z").status_code == 404
    assert client.get("/hour-audits").json() == []


def test_naive_timestamps_are_interpreted_as_utc(client):
    sid = upload(client, make_csv(3)).json()["source_id"]
    resp = create_audit(client, sid, "2026-01-01T00:00:00", "2026-01-01T01:00:00")
    assert resp.status_code == 202
    assert resp.json()["range_start"] == "2026-01-01T00:00:00Z"
    assert wait_audit(client, resp.json()["audit_id"])["status"] == "completed"


def test_max_24_hours_allowed(client):
    sid = upload(client, make_csv(3)).json()["source_id"]
    resp = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    )
    assert resp.status_code == 202
    assert resp.json()["hours"] == 24


def test_malformed_row_fails_audit_without_partial_report(client):
    hour = HOUR
    lines = full_hour("STA01", hour, 0)
    lines.insert(30, "STA01,2026-01-01T00:30:00+00:00,not-an-int")
    sid = upload(client, build_csv(lines)).json()["source_id"]
    audit_id = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
    ).json()["audit_id"]
    info = wait_audit(client, audit_id)
    assert info["status"] == "failed"
    assert "第 32 行" in info["error"]
    assert client.get(f"/hour-audits/{audit_id}/report").status_code == 409


def test_pre_1970_and_year_one_timestamps(client):
    """负时间戳与 <1000 年份：小时归属、报告时间串必须仍然正确。"""
    for start_text, year in (
        ("1969-12-31T23:00:00Z", "1969"),
        ("0001-01-01T00:00:00Z", "0001"),
    ):
        hour = datetime.strptime(start_text, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        if year == "0001":
            # strftime 对 <1000 年不补零，手工构造完整的 60 个整分
            lines = [
                f"STA01,0001-01-01T00:{minute:02d}:00+00:00,{minute}"
                for minute in range(60)
            ]
        else:
            lines = full_hour("STA01", hour, 0)
        if year == "1969":
            # 22:59:59.5 属于前一小时，不应计入本小时的非整分时刻
            lines.append("STA01,1969-12-31T22:59:59.500000+00:00,1")
        sid = upload(client, build_csv(lines)).json()["source_id"]
        end_text = (hour + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        if year == "0001":
            # strftime 对 <1000 年不补零，手工构造
            end_text = "0001-01-01T01:00:00Z"
        audit_id = create_audit(client, sid, start_text, end_text).json()["audit_id"]
        assert wait_audit(client, audit_id)["status"] == "completed", year
        entry = station_entry(report_of(client, audit_id), "STA01")
        detail = entry["hours_detail"][0]
        assert detail["hour"] == start_text
        assert detail["status"] == "releasable", (year, detail)
        assert math.isclose(detail["mean_temperature"], 29.5)


def test_audit_does_not_block_extraction_tasks(client):
    """审计期间原有提取任务的受理、查询与下载仍可用。"""
    hour = HOUR
    lines = []
    for h in range(3):
        lines += full_hour("STA01", hour + timedelta(hours=h), h)
        lines += full_hour("STA02", hour + timedelta(hours=h), h + 100)
    # 追加大量窗口外行，延长审计扫描时间
    for i in range(2000):
        lines.append(
            f"STA09,2026-02-01T{i // 60 % 24:02d}:{i % 60:02d}:00+00:00,1"
        )
    sid = upload(client, build_csv(lines)).json()["source_id"]

    audit_id = create_audit(
        client, sid, "2026-01-01T00:00:00Z", "2026-01-01T03:00:00Z"
    ).json()["audit_id"]

    started = time.monotonic()
    task = create_task(client, sid, stations=["STA01"], tmin=0, tmax=200)
    assert task.status_code == 201
    assert time.monotonic() - started < 2.0
    assert client.get("/tasks").status_code == 200
    assert client.get(f"/tasks/{task.json()['task_id']}").status_code == 200
    finished = wait_terminal(client, task.json()["task_id"])
    assert finished["status"] == "completed"

    assert wait_audit(client, audit_id)["status"] == "completed"


def test_unknown_audit_404(client):
    assert client.get("/hour-audits/nope").status_code == 404
    assert client.get("/hour-audits/nope/report").status_code == 404


def test_restart_keeps_completed_report_and_interrupts_active(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    import importlib
    import sys

    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    main = importlib.import_module("app.main")
    config = importlib.import_module("app.config")

    from fastapi.testclient import TestClient

    with TestClient(main.app) as c1:
        lines = full_hour("STA01", HOUR, 0)
        done_sid = upload(c1, build_csv(lines)).json()["source_id"]
        done_audit = create_audit(
            c1, done_sid, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
        ).json()["audit_id"]
        assert wait_audit(c1, done_audit)["status"] == "completed"

        # 让审计慢到重启时仍在途中：大批行 + 窗口外校验
        slow_lines = []
        for i in range(60000):
            slow_lines.append(
                f"STA09,2026-02-01T{i // 60 % 24:02d}:{i % 60:02d}:00+00:00,1"
            )
        slow_sid = upload(c1, build_csv(slow_lines)).json()["source_id"]
        active_audit = create_audit(
            c1, slow_sid, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
        ).json()["audit_id"]
        time.sleep(0.05)

    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    main2 = importlib.import_module("app.main")
    with TestClient(main2.app) as c2:
        kept = c2.get(f"/hour-audits/{done_audit}").json()
        assert kept["status"] == "completed"
        assert c2.get(f"/hour-audits/{done_audit}/report").status_code == 200
        broken = c2.get(f"/hour-audits/{active_audit}").json()
        assert broken["status"] == "interrupted"
        assert c2.get(f"/hour-audits/{active_audit}/report").status_code == 409
        assert not list(config.AUDITS_TMP_DIR.glob("*"))
