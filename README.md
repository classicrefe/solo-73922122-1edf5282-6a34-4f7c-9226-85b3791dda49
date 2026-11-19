# 观测数据子集交付服务

面向观测数据工程师的子集交付 API：上传至多 **256 MiB** 的 CSV 原件
（`station,utc_time,temperature`），按**站号集合**与**温度闭区间**提取子集。
同原件的在途任务**共享一次源扫描**，支持途中加入补齐与即时取消；结果保留
**原表头、原行序与重复行**，且仅在整份原件校验通过后开放下载。

同时提供**小时放行审计**：提交已上传原件 ID 与 UTC 整点起止时间，后台逐站
逐小时核对 60 个整分采样，输出缺测数、冲突时刻与可放行小时的平均温度，
阻断小时不提供均值，详见下文“小时放行审计”。

## 技术栈与资源

- FastAPI + Uvicorn + SQLite（WAL），纯标准库做 CSV 流式解析；
- 全程流式处理：上传分块落盘、扫描逐行读取、结果逐行写暂存，单进程实测
  处理 72 MiB / 200 万行原件时峰值 RSS 约 **55 MiB**；小时审计在大批行原件
  上峰值进程 RSS 约 57 MiB（见“小时审计的资源与并发”）；容器内存上限
  **128 MiB**；
- 数据目录 `/data` 使用命名卷，容器重启后成功结果与元数据保留。

## 快速启动

```bash
docker compose up --build
# 接口文档（Swagger UI）
open http://localhost:8000/docs
```

本地无 Docker 时（Python 3.11+）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 可控慢扫描示例

慢扫描用于演示共享扫描、途中加入与取消。两种打开方式：

1. 上传时带表单字段 `row_delay_ms`（每行额外等待毫秒数，0–60000）：

```bash
curl -X POST http://localhost:8000/sources \
  -F "file=@obs.csv;type=text/csv" -F "row_delay_ms=20"
```

2. 容器环境变量 `SCAN_ROW_DELAY_MS` / `SCAN_DELAY_EVERY_ROWS`（全局默认）。

端到端演示脚本（生成数据 → 上传 → 任务 A → 途中加入 B → 取消 C →
下载核对，仅用标准库）：

```bash
python scripts/demo_slow_scan.py http://localhost:8000 20
```

小时放行审计端到端演示（生成样例 → 上传 → 审计 → 下载报告并核对均值、
缺测与冲突）：

```bash
python scripts/demo_hour_audit.py http://localhost:8000
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sources` | multipart 上传 CSV，可带 `row_delay_ms`；超 256 MiB 返回 413 |
| GET  | `/sources` / `/sources/{id}` | 原件列表/详情 |
| POST | `/tasks` | 受理筛选任务，**立即返回 201，不等待扫描** |
| GET  | `/tasks` / `/tasks/{id}` | 任务列表/详情（含 `scanned_rows` 扫描进度） |
| POST | `/tasks/{id}/cancel` | 取消任务 |
| GET  | `/tasks/{id}/result` | 下载结果 CSV；未完成返回 409，不供部分文件 |
| POST | `/hour-audits` | 受理**小时放行审计**，**立即返回 202** |
| GET  | `/hour-audits` / `/hour-audits/{id}` | 审计列表/详情（状态、错误） |
| GET  | `/hour-audits/{id}/report` | 下载 JSON 报告；未完成/失败返回 409 |
| GET  | `/health` | 健康检查 |

`POST /tasks` 请求体：

```json
{ "source_id": "...", "stations": ["STA01", "STA02"], "temp_min": 10, "temp_max": 30 }
```

`stations` 为 `null` 或空数组表示全部站号；温度区间为**闭区间**。

任务状态机：`pending → running → completed | failed | cancelled | interrupted`
（取消受理瞬间可见 `cancelling`）。

## 小时放行审计

分析组按“每站每小时平均温度”取数，缺测或重传异值的小时不得进入下游。

`POST /hour-audits` 请求体：

```json
{
  "source_id": "<已上传原件 ID>",
  "range_start": "2026-01-01T00:00:00Z",
  "range_end": "2026-01-01T03:00:00Z"
}
```

- **窗口语义**：UTC 整点、半开区间 `[range_start, range_end)`，跨度 ≤ 24 小时。
  带偏移量（如 `+08:00`）的时刻先按 UTC 归一化再要求整点；不带时区的按 UTC
  解释。非法窗口返回 400，原件不存在返回 404。
- **范围**：窗口内出现过的全部站号 × 窗口内全部整点小时；原件可无序。
- **逐小时判定**（站 × 小时为一个单元）：
  1. 60 个**整分**（秒/微秒均为 0）必须齐全，否则记缺测数并阻断；
  2. 同一整分出现不同温度为**重传异值**，该小时阻断，报告列出冲突时刻及
     冲突温度（同分钟同温度的重传只计一次，不影响样本与均值）；
  3. 小时内出现非整分时刻（如 `00:41:30`）同样阻断该小时。
- **阻断小时不提供平均温度**；可放行小时给出 60 个样本的算术平均。
- **完整性**：与提取任务一致，只有整份原件逐行校验通过后才发布报告；任一
  行非法或处理异常则审计 `failed`，不产生任何部分报告（报告先写 `.part`
  临时文件再原子改名）。
- **资源**：流式读取，逐批聚合进磁盘上的临时 SQLite（每 `AUDIT_BATCH_ROWS`
  行提交），内存与原件大小无关；临时聚合库在终态删除。审计后台串行执行，
  不阻塞原有提取任务的受理/查询/下载。
- **重启**：已完成报告保留可查；重启时仍在途的审计标 `interrupted`，不发布
  报告，遗留临时文件清空。

报告为 JSON，结构示例（节选）：

```json
{
  "audit_id": "...", "source_id": "...",
  "range_start": "2026-01-01T00:00:00Z", "range_end": "2026-01-01T03:00:00Z",
  "hours": 3, "generated_at": "...",
  "stations": [
    {
      "station": "STA01",
      "releasable_hours": 1, "blocked_hours": 1, "missing_minutes_total": 3,
      "hours_detail": [
        {
          "hour": "2026-01-01T00:00:00Z", "status": "blocked",
          "missing_minutes": 3,
          "conflict_moments": [
            {"time": "2026-01-01T00:23:00Z", "temperatures": [23, 999],
             "distinct_temperatures": 2}
          ],
          "block_reasons": ["missing_samples", "conflicting_retransmission"],
          "mean_temperature": null
        },
        {
          "hour": "2026-01-01T01:00:00Z", "status": "releasable",
          "missing_minutes": 0, "conflict_moments": [],
          "mean_temperature": 29.5
        }
      ]
    }
  ]
}
```

`block_reasons` 取值：`missing_samples`、`conflicting_retransmission`、
`non_minute_timestamp`（含非整分时刻时另给 `non_minute_moments` 与
`non_minute_moments_total`，明细最多 100 条/小时）。冲突温度明细最多 32 个/
时刻，`distinct_temperatures` 给出真实去重个数。

## 关键语义与实现

- **共享一次源扫描**：同一 source 同一代扫描（`scans/<source>/gen<N>/`）只由
  一个扫描线程顺序读取原件一次，所有任务作为订阅者即时筛行；已用测试验证
  5 个任务（含途中加入）原件仅被打开 1 次。
- **途中加入补齐**：新订阅者先从本代 spool 日志与扫描线程内存缓冲（同一把
  条件锁做一致性快照，不重不漏）补齐已过部分，再接收在线投递，**绝不另读
  原件**；补读期间扫描线程在检查点等待。
- **取消**：单个任务取消不影响其他任务；最后一个活跃任务取消时，扫描线程
  在下一检查点真正停止、清空本代暂存后，接口才返回 `cancelled`，此后不发布
  结果（下载返回 409）。
- **收尾期受理**：旧扫描终态清理与新扫描使用不同代目录，互不干扰；收尾窗口
  内并发受理的任务全部能完整交付（有并发测试覆盖）。
- **完整校验才开放**：任一行格式错误（列数、空站号、时刻缺时区/不可解析、
  温度非整数）或扫描异常，本代所有未完任务 `failed`，暂存删除，不提供部分
  文件；只有全文件合法才原子改名发布结果。
- **重启恢复**：成功任务的结果与 SQLite 元数据保留可下载；所有非终态任务在
  启动时标记 `interrupted`，遗留暂存目录清空。
- **查询不阻塞**：所有 GET 与任务受理均为快速返回，扫描进度经
  `GET /tasks/{id}` 的 `scanned_rows` 观测。

## 小时审计的资源与并发

- 原件 69 MiB / 194 万行、400 站实测：审计约 59 秒完成；逐行解析与聚合
  结果全部写入磁盘上的临时 SQLite，进程峰值 RSS 约 **22 MiB**。通过 FastAPI
  完整链路（上传 + 审计 + 并行的提取任务）实测进程峰值 RSS 约 **57 MiB**，
  低于 128 MiB 上限；报告 5–18 MiB（取决于冲突明细数量）。
- 多份审计同时受理时按到达顺序**串行**执行，避免并发叠加内存；任何时刻
  只有一份原件在扫描聚合。
- 审计仅使用独立的临时 SQLite 与原件文件；主库只在受理与终态各写一次，
  提取任务的受理、查询、下载全程不受影响（有并发测试覆盖）。

## 目录结构

```
app/            FastAPI 应用、SQLite 层、共享扫描调度、CSV 校验、小时放行审计
scripts/        慢扫描与小时审计的端到端演示（仅标准库）
tests/          pytest 自测（40 个用例）
data/           运行期数据（sources/results/scans/reports + app.db），已 gitignore
Dockerfile      python:3.11-slim，非 root 运行，自带 HEALTHCHECK
docker-compose.yml  128m 内存限制、命名卷持久化
```

## 自测

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/
```

覆盖：闭区间边界与原行序、重复行保留、空匹配仅表头、共享扫描只打开一次、
反复迟到加入不丢行、单任务/末任务取消与暂存清理、收尾窗口并发受理、
格式错误全量失败不供部分结果、256 MiB 上限拒收、重启保留成功结果并中断
未完任务、查询/受理不等待扫描；小时审计覆盖可放行均值、缺测阻断、同分钟
同温度重传去重、异值冲突阻断、非整分时刻阻断、UTC 归一化（含 +08:00 与
无时区输入）、窗口/站号过滤、乱序原件、非法行整份失败、审计期间提取任务
可用、重启保留完成报告并中断在途审计。
