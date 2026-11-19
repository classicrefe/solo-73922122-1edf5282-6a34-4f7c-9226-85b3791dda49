"""小时放行审计：逐站、逐 UTC 小时核对 60 个整分采样。

语义
====
* 请求给出原件 ID 与 UTC 整点起止时间（半开区间 ``[start, end)``），跨度
  不超过 24 小时。带其他时区偏移的时刻先按 UTC 归一化，再要求归一化后仍是
  整点。
* 审计范围是窗口内出现过的全部站号 × 窗口内全部整点小时。
* 一个站号在一个小时内可放行，当且仅当：
    1. 60 个整分（秒、微秒均为 0）全部有采样；
    2. 同一整分没有出现不同温度（同分钟同温度的重传只计一次）；
    3. 该小时内没有非整分时刻的数据（非整分时刻使所在小时阻断）。
  其余情况该小时阻断；阻断小时不给出平均温度。
* 报告含每站每小时的缺测数、冲突时刻（含冲突温度集合）、非整分时刻，
  以及可放行小时的平均温度。
* 与提取任务一致：只有整份原件逐行校验通过后才原子发布报告；任一行非法
  或处理异常则本次审计 failed，不产生任何部分报告。

资源与并发
==========
* 原件流式读取，每批聚合写入磁盘上的临时 SQLite，内存占用与原件大小无关
  （实测 69 MiB / 194 万行原件峰值 RSS 约 22 MiB）。
* 审计在后台线程执行，除受理与收尾时各写一次主库外不占用主库；提取任务的
  受理、查询与扫描不受影响。多份审计串行执行，避免并发叠加内存。
* 临时聚合库在完成或失败后删除；报告写入临时文件后原子改名发布。
"""
from __future__ import annotations

import json
import math
import os
import shutil
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config
from .csvutil import CsvFormatError, parse_row, validate_header
from .db import Database

# 临时聚合库只用于本进程，无需崩溃恢复能力。
# samples 每个 (站, 小时, 整分, 温度) 一行：同分钟同温度的重传被去重，
# 同分钟出现多行即表示温度冲突；因此不需要额外的重传计数或冲突表。
AGG_SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
PRAGMA temp_store=FILE;
PRAGMA cache_size=-4096;
CREATE TABLE samples (
    station TEXT NOT NULL,
    hour    TEXT NOT NULL,
    minute  INTEGER NOT NULL,
    temp    INTEGER NOT NULL,
    PRIMARY KEY (station, hour, minute, temp)
) WITHOUT ROWID;
CREATE TABLE odd (
    station TEXT NOT NULL,
    hour_epoch INTEGER NOT NULL,
    ts_epoch INTEGER NOT NULL,
    n       INTEGER NOT NULL,
    PRIMARY KEY (station, hour_epoch, ts_epoch)
) WITHOUT ROWID;
"""

INSERT_SAMPLE = """
INSERT INTO samples (station, hour, minute, temp) VALUES (?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""

UPSERT_ODD = """
INSERT INTO odd (station, hour_epoch, ts_epoch, n) VALUES (?, ?, ?, 1)
ON CONFLICT(station, hour_epoch, ts_epoch) DO UPDATE SET n = odd.n + 1
"""

# 审计状态：pending -> running -> completed | failed | interrupted


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_z(dt: datetime) -> str:
    """UTC、秒级、带 Z 的固定格式。

    不用 strftime("%Y")：平台上它对 <1000 年不补零，会产生无法回读的串。
    """
    dt = dt.astimezone(timezone.utc)
    return (
        f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
        f"T{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}Z"
    )


class AuditRangeError(ValueError):
    """请求的时间窗口非法。"""


def parse_hour(text: str, field_name: str) -> datetime:
    """解析一个时刻，按 UTC 归一化后要求仍是整点。

    带 ``Z`` 或偏移量的按偏移归一化；不带时区的按 UTC 解释（字段本身即 UTC）。
    """
    if not isinstance(text, str) or not text.strip():
        raise AuditRangeError(f"{field_name} 不能为空")
    raw = text.strip()
    iso_text = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        dt = datetime.fromisoformat(iso_text)
    except ValueError as exc:
        raise AuditRangeError(f"{field_name}={raw!r} 不是合法 ISO 8601 时刻") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise AuditRangeError(
            f"{field_name}={raw!r} 按 UTC 归一化后不是整点（{iso_z(dt)}）"
        )
    return dt.replace(tzinfo=timezone.utc)


def validate_range(start_text: str, end_text: str) -> tuple[datetime, datetime]:
    start = parse_hour(start_text, "range_start")
    end = parse_hour(end_text, "range_end")
    if end <= start:
        raise AuditRangeError("range_end 必须晚于 range_start")
    hours = int((end - start).total_seconds() // 3600)
    if hours > config.AUDIT_MAX_HOURS:
        raise AuditRangeError(
            f"时间跨度 {hours} 小时，超过上限 {config.AUDIT_MAX_HOURS} 小时"
        )
    return start, end


class _Stopped(Exception):
    """服务关闭导致审计提前终止。"""


@dataclass
class AuditState:
    audit_id: str
    source_id: str
    range_start: str
    range_end: str
    hours: int
    status: str = "pending"
    error: str | None = None
    report_path: str | None = None
    created_at: str = field(default_factory=_now)
    stop_event: threading.Event = field(default_factory=threading.Event)

    def public(self) -> dict:
        return {
            "audit_id": self.audit_id,
            "source_id": self.source_id,
            "range_start": self.range_start,
            "range_end": self.range_end,
            "hours": self.hours,
            "status": self.status,
            "error": self.error,
            "report_available": self.status == "completed",
            "created_at": self.created_at,
        }


# 报告中对单个小时的明细设上限，避免极端原件把报告和内存撑爆；
# 超出部分只记总数（blocked 判定不受影响）。
MAX_CONFLICT_TEMPS = 32
MAX_ODD_MOMENTS_PER_HOUR = 100


class _MinuteAgg:
    """流式聚合一个整分的不同温度（有上限，避免极端原件撑大内存/报告）。"""

    __slots__ = ("count", "temps")

    def __init__(self) -> None:
        self.count = 0
        self.temps: list[int] = []

    def add(self, temp: int) -> None:
        self.count += 1
        if len(self.temps) < MAX_CONFLICT_TEMPS:
            self.temps.append(temp)


class AuditManager:
    """审计受理与后台执行。"""

    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.Lock()
        self._audits: dict[str, AuditState] = {}
        self._threads: dict[str, threading.Thread] = {}
        # 同一时刻只处理一份原件审计：单份审计的内存增量约 20-30 MiB，
        # 串行执行保证多份审计并发受理时容器 128 MiB 上限不被突破。
        self._slot = threading.Lock()
        self.shutting_down = False

    # -- 受理 --------------------------------------------------------------
    def create_audit(self, source_id: str, start_text: str, end_text: str) -> AuditState:
        if self.db.get_source(source_id) is None:
            raise KeyError(source_id)
        start, end = validate_range(start_text, end_text)
        audit_id = uuid.uuid4().hex
        state = AuditState(
            audit_id=audit_id,
            source_id=source_id,
            range_start=iso_z(start),
            range_end=iso_z(end),
            hours=int((end - start).total_seconds() // 3600),
        )
        self.db.insert_audit(
            audit_id, source_id, state.range_start, state.range_end, state.created_at
        )
        thread = threading.Thread(
            target=self._run, args=(state, start, end), name=f"audit-{audit_id[:8]}",
            daemon=True,
        )
        with self._lock:
            self._audits[audit_id] = state
            self._threads[audit_id] = thread
        thread.start()
        return state

    def get_state(self, audit_id: str) -> AuditState | None:
        with self._lock:
            return self._audits.get(audit_id)

    # -- 后台执行 ----------------------------------------------------------
    def _run(self, state: AuditState, start: datetime, end: datetime) -> None:
        agg_path = config.AUDITS_TMP_DIR / f"{state.audit_id}.sqlite"
        report_tmp = config.AUDITS_TMP_DIR / f"{state.audit_id}.json.part"
        final_report = config.AUDIT_REPORTS_DIR / f"{state.audit_id}.json"
        try:
            self._set_status(state, "running")
            # 排队期间若服务关闭，则直接以中断收尾，不触碰文件
            while not self._slot.acquire(timeout=0.2):
                self._check_stop(state)
            try:
                self._audit_source(state, start, end, agg_path, report_tmp)
            finally:
                self._slot.release()
            if state.stop_event.is_set():
                raise _Stopped()
            # 报告已完整落盘，原子发布
            os.replace(report_tmp, final_report)
            state.report_path = str(final_report)
            self._set_status(state, "completed", report_path=str(final_report))
        except _Stopped:
            state.error = "服务关闭，审计中断"
            self._set_status(state, "interrupted", error=state.error)
        except CsvFormatError as exc:
            state.error = str(exc)
            self._set_status(state, "failed", error=state.error)
        except Exception as exc:  # 处理失败：不开放任何部分报告
            state.error = f"审计处理失败: {exc!r}"
            self._set_status(state, "failed", error=state.error)
        finally:
            for path in (agg_path, report_tmp):
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            if state.status != "completed":
                try:
                    final_report.unlink(missing_ok=True)
                except OSError:
                    pass
            with self._lock:
                self._threads.pop(state.audit_id, None)
                self._audits.pop(state.audit_id, None)

    def _set_status(
        self,
        state: AuditState,
        status: str,
        error: str | None = None,
        report_path: str | None = None,
    ) -> None:
        state.status = status
        if error is not None:
            state.error = error
        if report_path is not None:
            state.report_path = report_path
        self.db.update_audit(
            state.audit_id,
            status=status,
            error=error,
            report_path=report_path,
            now=_now(),
        )

    def _check_stop(self, state: AuditState) -> None:
        if state.stop_event.is_set() or self.shutting_down:
            raise _Stopped()

    def _audit_source(
        self,
        state: AuditState,
        start: datetime,
        end: datetime,
        agg_path: Path,
        report_tmp: Path,
    ) -> None:
        source_path = config.SOURCES_DIR / f"{state.source_id}.csv"
        if not source_path.exists():
            raise FileNotFoundError(f"原件文件不存在: {source_path.name}")

        # 窗口边界用 epoch 整点比较，避免逐行格式化时间戳
        start_epoch = int(start.timestamp())
        end_epoch = int(end.timestamp())
        conn = sqlite3.connect(str(agg_path))
        try:
            conn.executescript(AGG_SCHEMA)
            conn.commit()

            with open(source_path, "r", encoding="utf-8", newline="") as src:
                header_line = src.readline()
                if not header_line:
                    raise CsvFormatError("空文件，缺少表头")
                validate_header(header_line)

                samples: list[tuple] = []
                odds: list[tuple] = []
                hour_cache: dict[int, str] = {}

                def hour_label(hour_epoch: int) -> str:
                    label = hour_cache.get(hour_epoch)
                    if label is None:
                        label = hour_cache[hour_epoch] = iso_z(
                            datetime.fromtimestamp(hour_epoch, timezone.utc)
                        )
                    return label

                def flush() -> None:
                    if samples:
                        conn.executemany(INSERT_SAMPLE, samples)
                        samples.clear()
                    if odds:
                        conn.executemany(UPSERT_ODD, odds)
                        odds.clear()
                    conn.commit()

                for line_no, raw_line in enumerate(src, start=2):
                    # 整份原件逐行校验（与提取任务一致），窗口外同样校验
                    station, dt, temperature = parse_row(raw_line, line_no)
                    # floor 而非 int()：1970 年前的负时间戳不能用向 0 截断
                    epoch = math.floor(dt.timestamp())
                    hour_epoch = epoch - epoch % 3600
                    if start_epoch <= hour_epoch < end_epoch:
                        if dt.second != 0 or dt.microsecond != 0:
                            odds.append((station, hour_epoch, epoch))
                        else:
                            samples.append(
                                (station, hour_label(hour_epoch), dt.minute, temperature)
                            )
                    if len(samples) + len(odds) >= config.AUDIT_BATCH_ROWS:
                        flush()
                        self._check_stop(state)
                flush()
            self._check_stop(state)

            self._write_report(conn, state, report_tmp)
            conn.commit()
        finally:
            conn.close()

    # -- 报告生成 ----------------------------------------------------------
    @staticmethod
    def _hour_labels(state: AuditState) -> tuple[list[int], list[str]]:
        """窗口可接受的整点小时：epoch 列表与对应的 UTC 标签。"""
        start = int(
            datetime.strptime(state.range_start, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        epochs = [start + i * 3600 for i in range(state.hours)]
        return epochs, [
            iso_z(datetime.fromtimestamp(epoch, timezone.utc)) for epoch in epochs
        ]

    def _write_report(
        self, conn: sqlite3.Connection, state: AuditState, report_tmp: Path
    ) -> None:
        epochs, hours = self._hour_labels(state)
        hour_labels = dict(zip(epochs, hours))

        with open(report_tmp, "w", encoding="utf-8", newline="") as out:
            out.write("{")
            out.write(f'"audit_id":{json.dumps(state.audit_id)},')
            out.write(f'"source_id":{json.dumps(state.source_id)},')
            out.write(f'"range_start":{json.dumps(state.range_start)},')
            out.write(f'"range_end":{json.dumps(state.range_end)},')
            out.write(f'"hours":{state.hours},')
            out.write(f'"generated_at":{json.dumps(_now())},')
            out.write('"stations":[')
            first_station = True
            # 逐站游标：报告写入与内存占用都只与单站数据量有关
            stations_cur = conn.execute(
                "SELECT station FROM ("
                "  SELECT station FROM samples"
                "  UNION SELECT station FROM odd"
                ") ORDER BY station"
            )
            for (station,) in stations_cur:
                # 大站号量时报告生成可能耗时，仍保持可中断
                self._check_stop(state)
                entry = self._station_entry(conn, station, epochs, hours)
                if not first_station:
                    out.write(",")
                first_station = False
                out.write(
                    json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
                )

            out.write("]}")
            out.flush()
            os.fsync(out.fileno())

    def _station_entry(
        self,
        conn: sqlite3.Connection,
        station: str,
        hour_epochs: list[int],
        hours: list[str],
    ) -> dict:
        label_of_epoch = dict(zip(hour_epochs, hours))
        # 每小时 -> 整分 -> 该整分已见温度（同分钟同温度重传在此被去重）
        samples: dict[str, dict[int, _MinuteAgg]] = {}
        for hour, minute, temp in conn.execute(
            "SELECT hour, minute, temp FROM samples"
            " WHERE station=? ORDER BY hour, minute, temp",
            (station,),
        ):
            agg = samples.setdefault(hour, {}).get(minute)
            if agg is None:
                agg = samples[hour][minute] = _MinuteAgg()
            agg.add(temp)

        # 非整分时刻：按小时归组并给出 UTC 时刻与出现次数。
        # 明细最多保留 MAX_ODD_MOMENTS_PER_HOUR 条，另给该小时总条数。
        odds: dict[int, list[dict]] = {}
        odd_totals: dict[int, int] = {}
        for hour_epoch, ts_epoch, n in conn.execute(
            "SELECT hour_epoch, ts_epoch, n FROM odd WHERE station=?"
            " ORDER BY hour_epoch, ts_epoch",
            (station,),
        ):
            if hour_epoch not in label_of_epoch:  # 防御：入库时已按窗口过滤
                continue
            odd_totals[hour_epoch] = odd_totals.get(hour_epoch, 0) + 1
            bucket = odds.setdefault(hour_epoch, [])
            if len(bucket) < MAX_ODD_MOMENTS_PER_HOUR:
                bucket.append(
                    {
                        "time": iso_z(
                            datetime.fromtimestamp(ts_epoch, timezone.utc)
                        ),
                        "count": n,
                    }
                )

        hour_entries: list[dict] = []
        releasable = blocked = missing_total = 0
        for hour_epoch, hour in zip(hour_epochs, hours):
            minute_values = samples.get(hour, {})
            missing = 60 - len(minute_values)
            hour_start = datetime.fromtimestamp(hour_epoch, timezone.utc)
            conflict_moments = [
                {
                    "time": iso_z(hour_start + timedelta(minutes=minute)),
                    "temperatures": agg.temps,
                    "distinct_temperatures": agg.count,
                }
                for minute, agg in sorted(minute_values.items())
                if agg.count > 1
            ]
            odd_moments = odds.get(hour_epoch, [])
            reasons: list[str] = []
            if missing:
                reasons.append("missing_samples")
            if conflict_moments:
                reasons.append("conflicting_retransmission")
            if odd_moments:
                reasons.append("non_minute_timestamp")
            is_blocked = bool(reasons)
            entry: dict = {
                "hour": hour,
                "status": "blocked" if is_blocked else "releasable",
                "missing_minutes": missing,
                "conflict_moments": conflict_moments,
            }
            if odd_moments:
                entry["non_minute_moments"] = odd_moments
                entry["non_minute_moments_total"] = odd_totals[hour_epoch]
            if is_blocked:
                entry["block_reasons"] = reasons
                entry["mean_temperature"] = None
                blocked += 1
                missing_total += missing
            else:
                # 可放行小时每分钟恰有一个温度
                total = sum(agg.temps[0] for agg in minute_values.values())
                entry["mean_temperature"] = round(total / 60.0, 4)
                releasable += 1
            hour_entries.append(entry)

        return {
            "station": station,
            "releasable_hours": releasable,
            "blocked_hours": blocked,
            "missing_minutes_total": missing_total,
            "hours_detail": hour_entries,
        }

    # -- 生命周期 ----------------------------------------------------------
    def recover_on_startup(self) -> None:
        """清理被迫中断的审计：状态已由 DB 层置 interrupted，删除残留报告。"""
        for audit_id in getattr(self.db, "interrupted_audit_ids", []):
            try:
                (config.AUDIT_REPORTS_DIR / f"{audit_id}.json").unlink(missing_ok=True)
            except OSError:
                pass
        shutil.rmtree(config.AUDITS_TMP_DIR, ignore_errors=True)
        config.AUDITS_TMP_DIR.mkdir(parents=True, exist_ok=True)

    def shutdown(self, timeout: float = 10.0) -> None:
        self.shutting_down = True
        with self._lock:
            states = list(self._audits.values())
            threads = list(self._threads.values())
        for state in states:
            state.stop_event.set()
        deadline = time.monotonic() + timeout
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
