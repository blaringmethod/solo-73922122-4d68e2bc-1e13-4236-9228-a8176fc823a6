# 样本污染追踪 API（Contamination Trace API）

实验室生物安全员依据样本转运记录划定污染追踪范围。本服务接收样本与转运批次
（起点、终点、离库/入库 UTC 时间、批次号）以及污染源样本，按时间顺序计算污染
可达样本；返回每个受影响样本的**最早到达时间**、**完整路径**和**隔离截止时间**。

更正批次时必须携带客户端所见版本（`seen_version`），服务以新版本替换旧记录，
并使依赖旧版本的已发布结果失效。查询在受理时固定快照，失败时不会发布半份结果。

实验室数据员可用**站点扫码回执**核对转运台账：登记离库/入库扫码（唯一扫码 ID、
批次号、站点、UTC 时刻），再对单个批次发起对账。对账报告列出缺回执、同方向多条、
站点或时刻不符等差异；受理时固定该批台账版本与扫码集合，计算期间任一方变化则不
发布旧结论；已发布报告遇该批新扫码或批次更正即标为过期（同内容重报与其他批次的
变化不影响它）。核对无误的可信数据再交给生物安全员用于追踪。

---

## 1. 启动

### Docker Compose（推荐）

```bash
docker compose up --build -d
open http://localhost:8000/docs      # 交互式 API 文档（Swagger UI）
curl http://localhost:8000/healthz
```

数据保存在命名卷 `trace-data`（容器内 `/data/trace.db`），重启后保留。
停止：`docker compose down`；同时删除数据：`docker compose down -v`。

### 本地运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
TRACE_DB_PATH=./data/trace.db .venv/bin/uvicorn app.main:app --port 8000
```

`TRACE_DB_PATH` 默认为 `./data/trace.db`；系统自动创建目录与 SQLite 表。

### 本地示例

```bash
docker compose up --build -d
./examples/run.sh                 # 使用 http://localhost:8000
API=http://127.0.0.1:9000 ./examples/run.sh
```

示例数据在 `examples/intake.json`。脚本每次运行使用唯一的 ID 前缀，因此可在同一
数据库上反复执行。

---

## 2. 追踪语义（必须明确的部分）

一次转运批次是一条**边**：`origin → destination`，在
`[departed_at, arrived_at]` 之间发生（离库/入库均为 UTC）。一个批次可以装载多个
样本（共享批次）。

**污染批次**的定义（二者之一）：

1. 该批次装载了被标记为污染源的样本；
2. 该批次从某站点**离库**的时间 `t_depart` 落在某污染批次在该站**入库**时间
   `t_arrive` 的窗口内：

   ```
   t_arrive <= t_depart <= t_arrive + 24h
   ```

   即“后续转运须在上一站入库后 24 小时内离库”：恰好 24 小时算作可达，早于污染
   入库则不可达。

这是一条**站点级**的传播规则（上一条链的终点必须等于下一条链的起点）。

**受影响样本**：装载于任一污染批次上、且自身不是污染源的样本。

对每个受影响样本返回：

| 字段 | 含义 |
| --- | --- |
| `first_arrival_at` | 与污染材料中该样本的**最早**入库时间 |
| `quarantine_until` | `first_arrival_at + 24h`（隔离截止时间） |
| `path` | 从污染源批次到该样本的**完整批次链**（每条腿含批次号与版本） |

结果按 `first_arrival_at`、再按 `sample_id` 排序。路径的确定性规则：优先最短路径，
其次按 `(arrived_at, batch_no)` 的处理顺序（结果可复现）。

实现为按 `arrived_at` 单遍扫描的闭包（污染入库必然早于后续离库），每个站点用区间
最小值结构查询 24 小时窗口，因此大规模、含热点枢纽的数据集也能在可接受时间内完成。

---

## 3. API 摘要

完整契约与示例见 `http://localhost:8000/docs`（OpenAPI 3）。

| 方法与路径 | 用途 |
| --- | --- |
| `POST /intakes` | 注册样本、转运批次与污染源；单事务原子写入 |
| `GET /dataset` | 查看当前数据集（含每个批次当前版本） |
| `POST /batches/{batch_no}/corrections` | 以新版本替换批次，并使旧结果失效 |
| `POST /traces` | 在固定快照上计算污染可达样本 |
| `GET /traces` | 列出所有已受理查询 |
| `GET /traces/{trace_id}` | 重读已发布的快照结果 |
| `GET /traces/{trace_id}/invalidations` | 查询失效记录 |
| `POST /scans` | 登记站点扫码回执（按扫码 ID 幂等） |
| `GET /batches/{batch_no}/scans` | 查看某批次已登记的扫码回执 |
| `POST /batches/{batch_no}/reconciliations` | 对单个批次发起扫码-台账对账 |
| `GET /reconciliations` | 列出对账报告（含过期标记） |
| `GET /reconciliations/{id}` | 读取报告差异与过期状态 |
| `GET /healthz` | 就绪探针 |

### 3.1 `POST /intakes`

```jsonc
{
  "source_sample_ids": ["S-001"],
  "samples": [
    {"sample_id": "S-001", "batch_nos": ["B-1001"]},
    {"sample_id": "S-002", "batch_nos": ["B-1001", "B-1002"]},
    {"sample_id": "S-003", "batch_nos": ["B-1003"]}
  ],
  "batches": [
    {"batch_no": "B-1001", "origin": "Collection-Lab", "destination": "Hub-North",
     "departed_at": "2026-01-01T08:00:00Z", "arrived_at": "2026-01-01T10:00:00Z"},
    {"batch_no": "B-1002", "origin": "Hub-North", "destination": "Reference-Lab",
     "departed_at": "2026-01-01T20:00:00Z", "arrived_at": "2026-01-01T22:00:00Z"},
    {"batch_no": "B-1003", "origin": "Reference-Lab", "destination": "Archive",
     "departed_at": "2026-01-02T10:00:00Z", "arrived_at": "2026-01-02T12:00:00Z"}
  ]
}
```

- `batches` 也可内联在样本的 `batches` 字段中；两种写法可混用。
- 污染源可用 `source_sample_ids` 或样本上的 `"contaminated": true` 标记。
- 批次号全局唯一，标识一次实体转运；多个样本通过 `batch_nos` 共享同一批次。
- 时间必须带时区（`Z` 或偏移）；朴素时间按 UTC 解释；响应统一输出 `Z`。
- 提交是**幂等且增量**的：完全相同的重复提交不会产生重复记录；同批次号但取值
  不同则返回 `409`，必须走更正接口。

### 3.2 `POST /traces`

```jsonc
{}                                                  // 使用入库时标记的全部污染源
{"source_sample_ids": ["S-001"]}                    // 或本次查询显式指定污染源
```

受理时在**单个写事务**内读取数据集并固定快照（`intake_id` + 每个批次的版本），
之后的计算只使用该快照。返回：

```jsonc
{
  "trace_id": 1,
  "status": "complete",
  "has_sources": true,
  "source_sample_ids": ["S-001"],
  "affected_samples": [
    {
      "sample_id": "S-003",
      "first_arrival_at": "2026-01-02T12:00:00Z",
      "quarantine_until": "2026-01-03T12:00:00Z",
      "path": [
        {"batch_no": "B-1001", "origin": "Collection-Lab", "destination": "Hub-North",
         "departed_at": "...", "arrived_at": "...", "version": 1},
        {"batch_no": "B-1002", "...": "...", "version": 1},
        {"batch_no": "B-1003", "...": "...", "version": 1}
      ]
    }
  ],
  "snapshot": {"intake_id": 1, "accepted_at": "...", "batch_versions": {...}, "batches": [...]}
}
```

**失败不发布半份结果**：所有结果行与 `status=complete` 在同一事务中提交；任何异常
都会回滚事务、删除可能的部分行，并把该查询标记为 `failed`。读取失败查询返回 `500`
且不含任何结果行。进程在发布前崩溃时，遗留超过 10 分钟的 `pending` 查询会在下次
启动时标记为 `failed`（宽限期避免多进程部署时误杀其他 worker 正在计算的任务）。

### 3.3 `POST /batches/{batch_no}/corrections`

```jsonc
{
  "seen_version": 1,
  "origin": "Hub-North",
  "destination": "Cold-Room",
  "departed_at": "2026-01-01T20:00:00Z",
  "arrived_at": "2026-01-01T21:00:00Z"
}
```

- `seen_version` 必须等于当前版本，否则 `409`（乐观锁，防止覆盖他人更正）。
- 更正会插入 `version + 1` 的新记录并将旧记录标记为已取代；新值原样返回并附带
  `invalidated_trace_ids` 与 `invalidated_reconciliation_ids`。
- 固定了旧版本的已发布查询会被记录失效，随后 `GET /traces/{id}` 返回 `409`
  `snapshot_invalidated`（附失效明细），绝不返回过期数据。
- 没有实际变化的更正返回 `409`。

### 3.4 `POST /scans`（扫码回执登记）

```jsonc
{
  "scans": [
    {"scan_id": "SC-0001", "batch_no": "B-1001", "direction": "departure",
     "site": "Collection-Lab", "scanned_at": "2026-01-01T08:00:00Z"},
    {"scan_id": "SC-0002", "batch_no": "B-1001", "direction": "arrival",
     "site": "Hub-North", "scanned_at": "2026-01-01T10:00:00Z"}
  ]
}
```

- 每条回执包含**唯一扫码 ID**（`scan_id`，由扫码站点分配）、**批次号**、**方向**
  （`departure` 离库 / `arrival` 入库）、**站点**与 **UTC 时刻**。
- **幂等**：同一 `scan_id` 且内容完全相同的重报不重复入账（响应中计为
  `duplicate`）；同一 `scan_id` 但内容不同则整批拒收（`409`）。
- 批次必须已登记，否则 `422`；尚无数据集时 `404`。
- 一次提交是**原子**的：任一条冲突或非法，整批回滚，不留半份。
- 新入账的回执会把该批次**已发布对账报告**标为过期，响应的
  `invalidated_reconciliation_ids` 列出受影响的报告；纯重复重报不影响任何报告。
- `GET /batches/{batch_no}/scans` 可按批次查看已登记回执（按登记顺序）。

### 3.5 `POST /batches/{batch_no}/reconciliations`（单批次对账）

无请求体。受理时在**单个写事务**内固定该批次的台账版本与扫码集合（高水位标记），
随后的对照计算只使用这组固定输入；发布前在同一事务内再次校验两者未变，若计算
期间有新扫码入账或批次被更正，则拒绝发布（`409 reconciliation_changed`，该次对账
记为 `failed`），不产出旧结论。

对照当前批次版本，报告列出四类差异（`findings`）：

| `kind` | 含义 |
| --- | --- |
| `missing_receipt` | 缺回执：离库或入库方向没有任何扫码 |
| `duplicate_direction` | 同方向多条：同一方向登记了不止一条扫码 |
| `site_mismatch` | 扫码站点与台账起点（离库）/终点（入库）不符 |
| `time_mismatch` | 扫码时刻与台账离库/入库时刻不符 |

`findings` 为空即 `matched: true`（回执与台账完全一致）。响应同时回显固定使用的
台账记录（`ledger`，含版本）与扫码集合（`scans`），报告可据此复核。

**过期**：已发布报告遇该批**新扫码入账**或**批次更正**即被标为过期
（`stale: true`，`invalidations` 记录原因与时刻）。同内容重报、其他批次的扫码或
更正都不影响它。过期报告仍可读取（`GET /reconciliations/{id}` 返回 `200`，差异与
过期状态一并给出），但不应再作为核对依据；重新对账会基于最新台账与扫码集合生成
新报告。

**失败不留半份结果**：所有差异行与 `status=complete` 在同一事务提交；任何异常都会
回滚并记为 `failed`（读取返回 `500`，不含任何差异行）。进程崩溃遗留超过 10 分钟的
`pending` 对账会在下次启动时标记为 `failed`。

### 3.6 错误格式

所有错误使用统一结构，不使用 FastAPI 默认的 `detail` 字符串：

```json
{"code": "validation_error", "message": "request validation failed",
 "details": {"errors": [{"loc": ["body", "batches", 0], "msg": "..."}]}}
```

状态码约定：`404` 未知样本/批次/查询/报告或无数据集；`409` 版本冲突、快照失效、批次取值
冲突、扫码 ID 异内容重报、对账期间输入变化（`reconciliation_changed`）、查询或对账
尚未发布（`*_pending`，不会以空结果冒充"无影响"/"无差异"）；`422` 载荷校验失败
或引用不存在的批次/样本；`500` 追踪或对账失败（含对应 id，且无结果行）。

`POST /traces` 与 `POST /batches/{batch_no}/reconciliations` 的 `500`/`409` 响应体是
统一错误结构（`code` 为 `trace_failed` / `snapshot_changed` /
`reconciliation_failed` / `reconciliation_changed`），`details` 中的 id 可用于在列表
接口中定位该次失败记录。

---

## 4. 数据一致性模型

- SQLite WAL 模式；所有写操作使用 `BEGIN IMMEDIATE` 串行化。
- 批次只追加：更正产生新版本，旧版本保留用于审计。
- 查询快照固定 `intake_id` 与每个批次的版本；发布前会再次校验这些版本未变化，
  若已变化则拒绝发布（`409 snapshot_changed`，查询标记为 `failed`）。
- 扫码回执只追加：`scan_id` 唯一，同内容重报幂等，异内容拒收；回执不更新、不删除，
  因此某批次的回执集合可由最大行号（高水位）精确标识。
- 对账受理时固定该批台账版本与回执高水位；发布前在同一事务内复查，任一方变化则
  拒绝发布并记为 `failed`。新回执入账或批次更正会在**同一事务**内把该批已发布报告
  标记为过期——要么报告已包含新事实，要么被标过期，不存在"半新半旧"。
- 快照读取、结果读取、失效标记读取都在单个读事务内完成，不会返回"半新半旧"的组合。

## 5. 自测

```bash
.venv/bin/python -m pytest -q
```

覆盖范围（78 个用例）：

- 24 小时窗口边界：恰好 24 小时可达、超出 1 微秒不可达、污染入库前离库不可达；
- 多跳链、共享批次、多个污染源、污染源重写；
- 最早到达时间与完整路径（含确定性并列规则）、隔离截止时间；
- 更正替换、连续多版本、陈旧 `seen_version`、无变化更正、未知批次；
- 更正使旧查询失效、旧查询返回 `409`、新查询反映新值、未被引用的批次更正；
- 发布失败/计算异常时不产生任何结果行，且不影响其他查询；
- 受理与更正并发时不会返回半份结果；
- 扫码登记：同 ID 同内容幂等、同 ID 异内容 `409` 且整批回滚、未知批次 `422`、
  载荷内重复 ID `422`、朴素时间按 UTC 解释；
- 对账差异：缺回执（离库/入库）、同方向多条、站点不符、时刻不符、多差异确定
  性排序、无差异即 `matched`；
- 对账固定：报告回显固定的台账版本与扫码集合；计算期间新扫码或批次更正则拒绝
  发布（`409`）且不留半份结果；
- 报告过期：该批新扫码或批次更正使报告过期（仍可读，附失效明细），同内容重报与
  其他批次变化不影响；过期后重新对账反映最新状态；
- 对账与扫码并发：要么包含新扫码、要么拒发、要么标过期，绝不半份；
- 400 跳深链、5 万批次宽图、2 万批次热点枢纽的性能回归；
- 400 组随机图与独立暴力参考实现逐一比对（同一 `first_arrival` 与路径）；
- 真实 uvicorn HTTP 端到端（`/docs`、入库、追踪、更正、失效、扫码、对账、过期、
  错误结构）；
- 重启持久化、崩溃遗留 `pending` 查询/对账的恢复。

Docker 构建的验证情况见下方“已知限制/未验证项”。

## 6. 已知限制与未验证项

- 本环境**未安装 Docker**，无法执行 `docker build` / `docker compose up`。Dockerfile
  与 compose 文件按标准用法编写，并已用相同的 `uvicorn app.main:app` 命令在本机
  验证；容器内 `pip install` 与健康检查命令未实测。
- 隔离截止时间按“首次到达 + 24 小时”计算，未接入具体病原体的潜伏期参数。
- 对账的时刻比对是**严格相等**：扫码时刻与台账时刻相差 1 秒也会记为
  `time_mismatch`。业务上若需要容差（如 ±5 分钟），需在 `app/reconcile.py` 中调整。
- 对账按批次独立进行，不校验跨批次的站点衔接（上一批终点是否等于下一批起点）；
  该一致性由追踪链本身表达。
- 扫码回执只追加、不可更正；登记错误的回执无法收回，只能通过新报告体现（回执
  集合不变时报告结论不变）。若业务需要撤销回执，需要新增作废接口。
- 未实现认证/授权、速率限制与多租户隔离；默认部署应置于受信网络之后。
- 更正只针对活跃数据集的批次；数据集的整批替换接口未开放（提交是增量式的）。
- 路径选择：优先最短链，其次 `(arrived_at, batch_no)` 处理顺序；这是可复现的确定性
  规则，但不是唯一的合理规则（例如按“最早离库”选路径会得到不同但同样合法的链）。
  若业务要求另一种并列规则，需要在 `app/tracing.py` 中调整。
- 同一批次号在两次入库提交中被定义成不同取值会返回 `409`；必须先更正，不能静默覆盖。
- SQLite 适合单机部署；多副本/高并发写入需要换成服务端数据库。
