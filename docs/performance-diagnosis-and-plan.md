# fnos-dupclean 性能诊断与架构演进方案

> 针对两类卡顿的系统性分析：① 查询完成后写入数据卡顿；② 批量删除数据卡顿/阻塞。
> 并给出 Rust 迁移方案与存储引擎替换评估。
> 版本基线：v0.5.3（SQLite 存储 + Python 服务层 + Rust `dupscan` 引擎）。

---

## 0. 一句话结论

- **删除卡顿的主因不是「移动文件」而是「逐文件 `commit` 触发 fsync」**——`quarantine.apply_removals` 每删一个文件就 `db.add_quarantine()` 一次，每次都加全局锁 + 开连接 + `INSERT` + `commit`（fsync）。16 万组 ≈ 32 万文件时这会变成数十分钟级阻塞，且跑在 HTTP 请求线程里，前端只能干等。
- **写入卡顿相对次要**，但当前实现把 Rust 已经算好的结果 JSON 又用 Python 逐组 `INSERT` 回写 SQLite（16 万次 `INSERT INTO dup_groups`），且常驻 48 万行元组在内存；`init_db` 还**未开启 WAL / `synchronous=NORMAL`**，默认 DELETE 日志 + FULL 同步，HDD 下写库与删库都吃亏。
- **不建议整体替换 SQLite**：瓶颈是「用法」，不是「引擎」。160 万级行数 SQLite + WAL 完全够用。真正该做的是：① 修用法（WAL + 批量事务）；② 把两个 I/O 热点（结果回写、批量删除）用 Rust 子命令重做并异步化。

---

## 1. 实证基准（本次实测，非空谈）

测试环境：macOS / Apple SSD / tmpfs，`DUP_VAR` 指向临时库。按比例外推到用户 16 万组 × ~2 副本 ≈ 32 万待删文件。

| 路径 | 实现 | 600 文件耗时 | 外推 32 万文件 | 提速 |
|---|---|---|---|---|
| 删除-当前 | 逐文件 `move` + 逐文件 `commit`(fsync) | 0.200s | **≈107s（快盘）** / 机械盘可达 30–40 min | 1× |
| 删除-优化A | 逐文件 `move` + 批量 `commit`（每 500 一事务） | 0.047s | ≈25s | 4× |
| 删除-优化B | 批量 `commit` + **WAL / `synchronous=NORMAL`** | 0.043s | ≈23s | 5× |
| 写库 | `add_groups_bulk`（20k 组） | 0.052s | 16 万组 ≈0.4s（SSD） | — |

> 关键：优化 A/B 的 `move` 次数与当前**完全相同**，耗时却骤降——证明**瓶颈是每次 commit 的 fsync，不是文件移动本身**。机械盘 fsync ~8ms，32 万 × 8ms ≈ 2560s，正是用户"批量删除卡死"的真实量级。

---

## 2. 逐路径诊断

### 2.1 写入路径（查询完成后写库卡顿）
调用链：`rust_scan.run_scan` → Rust 引擎写出 `result_<tid>.json`（原子写）→ Python 读 JSON → `db.add_groups_bulk(tid, groups)`。

`db.add_groups_bulk`（`db.py:167`）的问题：
1. **`dup_groups` 逐条 INSERT**（16 万次 `c.execute("INSERT INTO dup_groups...")`，未用 `executemany`）。仅 `files` 表用了 `executemany`。
2. **内存峰值**：先在 Python 侧拼出 `file_rows`（48 万元组）再一次性 `executemany`——48 万元组常驻内存（GB 级对象开销）。
3. **未开 WAL + 默认同步**：单个大事务虽只提交一次，但默认 DELETE 日志 + `synchronous=FULL`，提交时需 fsync 整个 DB 文件；HDD 上明显变慢。
4. **整段持 `_DB_LOCK`**：虽只读请求不走该锁（读取不被阻塞），但任何并发写（如日志）会在此期间排队。
5. **架构冗余**：Rust 已经把完整结果写成了 JSON（`--out`），Python 又把它拆行回写 SQLite——结果 JSON 才是事实源，SQLite 里这份是重复存储，且还要再被 `handle_clean` 用 `get_groups(tid)` 整表读回（同样 48 万行）。

### 2.2 删除路径（批量删除卡顿/阻塞）★主因
调用链：`handle_clean`（`app.py:317`）→ `db.get_groups(tid)` 全量读回 → `strategies.select_to_delete` 逐组选文件 → `quarantine.apply_removals(tid, to_delete, config)`。

`quarantine.apply_removals`（`quarantine.py:24`）的问题：
1. **逐文件 `db.add_quarantine()` 且每次 `commit`**（每个文件一次 `INSERT` + fsync）→ 见 §1 基准，**这是 32 万文件卡死的根因**。
2. **逐文件 `os.makedirs(os.path.dirname(dest))`**：每个文件都做目录存在性检查 + 可能建目录，是浪费的 syscall；应按目录批量预建。
3. **整个循环在 HTTP 请求线程内同步执行**：`handle_clean` 直接 `return self.send_json(...)` 之前，32 万文件已经全删完了——请求线程被占用数十分钟，前端无进度、易超时；且 `ThreadingHTTPServer` 其他请求也受影响。
4. **跨文件系统 `shutil.move` = copy+delete**：若隔离区与扫描目录不在同卷，大文件复制极慢（此点需确认部署中 `DUP_VAR`/隔离区与数据是否同卷）。
5. **删后不回写 `dup_groups`/`files`**：删完 UI 不刷新，二次"清理全部"会重复尝试已删文件、大量落入 `failed`。

### 2.3 锁竞争
- `_DB_LOCK` 是**单一全局锁**，所有写操作（`add_group/add_groups_bulk/add_log/add_quarantine/set_config/update_task/delete_task`）串行化。
- 读操作（`get_groups/get_groups_page/get_task/...`）**不走该锁**，因此查询界面不因写锁而卡——这与"查看时卡死"是另一处已修的 N+1 问题，二者无关。
- 删除期每文件加/放锁 32 万次，锁本身开销小，真正贵的是锁内 commit 的 fsync（§1）。
- **风险点**：删除长事务若与"写库/日志"并发，会互相等待；当前是串行 HTTP 所以暂未爆发，但异步化后必须保证同一 DB 的写入串行或都用 WAL。

### 2.4 I/O 瓶颈
- 写库：48 万行 INSERT 的随机/顺序页写 + 事务日志。
- 删除：32 万次 `rename`（同卷）更新目录元数据；跨卷则 32 万次「读-写-删」大文件。
- **两个路径的 I/O 都被「每次操作的 fsync」放大**——这是比 I/O 总量更致命的因素。

### 2.5 内存
- 写库：`file_rows` 48 万元组 + `groups` 全量 list；`add_groups_bulk` 期间峰值可达 GB 级。
- 删除前：`handle_clean` 的 `db.get_groups(tid)` 把 48 万文件字典全载入内存，再逐组 `select_to_delete` 生成 `to_delete`（可能 32 万路径字符串）——又是 GB 级临时占用。
- 百万文件规模下，这两处内存峰值必须消除（见 §4 流式/分页改造）。

---

## 3. 根因排序（按影响）

| 排名 | 根因 | 影响路径 | 严重度 |
|---|---|---|---|
| 1 | 删除逐文件 `commit`(fsync) | 批量删除 | 致命（数十分钟阻塞） |
| 2 | 删除在 HTTP 线程同步执行，无进度/易超时 | 批量删除 | 高 |
| 3 | SQLite 未开 WAL + `synchronous=NORMAL` | 写库 + 删除 + 全部写 | 高（HDD 下放大数倍） |
| 4 | 写库 `dup_groups` 逐条 INSERT + 全量内存拼装 | 写库 | 中（SSD 可接受，HDD 慢） |
| 5 | 结果 JSON 被冗余回写 SQLite 又整表读回 | 写库 + 删除 | 中（浪费 I/O 与内存） |
| 6 | 删除后不更新分组表，二次清理失效 | 删除正确性 | 中 |

---

## 4. Rust 迁移方案

### 4.1 哪些模块该用 Rust 重做
I/O 绑定、需并行、需批量事务的路径 → 用 Rust 子命令重做；纯逻辑/HTTP/UI 留在 Python。

| 模块 | 现状 | 迁移建议 |
|---|---|---|
| 扫描引擎 `dupscan` | 已是 Rust | 维持；新增子命令 |
| **结果回写** | Python `add_groups_bulk` | → `dupscan write-db`（Rust + rusqlite，WAL，双表 `executemany`，零 Python 大列表） |
| **批量删除** | Python `apply_removals` | → `dupscan clean`（Rust，并行 `rename` + 批量事务写隔离记录 + 进度上报） |
| 分组选择策略 | Python `strategies.py` | 保留 Python（轻量 CPU，非瓶颈）；Rust 只接收「待删路径清单」 |
| HTTP/SSE/UI | Python | 保留；改为**异步派发**子命令 + 订阅进度 |

### 4.2 新增子命令设计

**`dupscan write-db --db <path> --result <result_tid.json> --task-id <tid>`**
- 直接 `rusqlite` 打开 SQLite，先 `PRAGMA journal_mode=WAL; synchronous=NORMAL; cache_size=-64000`；
- 删除旧结果 → `executemany` 写 `dup_groups`（一次），按自增 id 区间算出 `gid` → `executemany` 写 `files`（一次）；
- 通过 `--out` 写一行 JSON 进度（`{type:"write_progress", done, total}`）供 SSE 转发；
- 引擎本就静态链接 musl，重加 rusqlite（bundled）不破坏交叉编译。

**`dupscan clean --db <path> --task-id <tid> --quarantine-root <dir> --manifest <to_delete.json>`**
- 入参：待删路径清单（由 Python 先按策略算好，或 Rust 直接读 DB 算——见 §4.3）；
- 按**目标目录分组**，预建目录树（`makedirs` 一次/目录，而非每文件）；
- `rayon` 并行 `rename` 到隔离区（同卷 rename 是 O(1) 元数据操作；跨卷走 copy+delete 并限速）；
- 每 N（默认 1000）个文件批量 `INSERT` + 单次 `commit`；
- 每批结束输出 `{type:"clean_progress", removed, failed, total}` → Python 经 SSE 推前端；
- 完成后回写 DB：把已删文件从 `files` 表移除/`dup_groups` 标记为「已部分清理」，避免二次清理重复尝试。

### 4.3 协议与生命周期
- Python 服务 `handle_clean` 改为：① 算 `to_delete` 清单（可流式，不必全载内存）；② 派发 `dupscan clean` 子进程，task_id 登记为「cleaning」状态；③ 子进程 stdout 的进度事件由现有 EventBus 转发到前端 SSE；④ 立即返回 `{ok:true, job_id, async:true}`，前端显示进度条。
- 取消：子进程监听 `SIGTERM`，Python 收到取消请求时 `kill` 之，已移动文件保留在隔离区（安全）。

---

## 5. 存储引擎替换评估

| 方案 | 适用度 | 评估 | 结论 |
|---|---|---|---|
| **SQLite + WAL**（现状修了用法） | ★★★★ | 160 万行内读写轻松；WAL 下并发读不阻塞写；零额外依赖、交叉编译简单 | **采用**：元数据/隔离/日志继续用 |
| **不把结果回写 SQLite，直接服务 Rust 的 JSON 结果** | ★★★★★ | 结果 JSON 已是事实源；`/groups` 分页直接从 JSON（或 MessagePack/Parquet）流式切片；消除 48 万行冗余写读与内存峰值 | **强烈建议**：写入卡顿从根上消失 |
| **LMDB**（mmap KV） | ★★ | 读多写少极快、无 fsync 负担、并发读好；但需原生依赖 + musl 交叉编译，且要重写全部 `db.py` | 仅当「多浏览器并发读导致锁竞争」出现时再考虑 |
| **RocksDB** | ★ | 写入吞吐高但原生依赖重、编译复杂、对当前规模过度工程 | 不推荐 |
| **Parquet/Arrow IPC + mmap 索引**（仅结果数据） | ★★★ | 列存 + 谓词下推适合「按 size 排序分页」；但增加依赖与序列化复杂度 | 可选：若结果集 >500 万组再上 |

**存储结论**：
- **保留 SQLite（开启 WAL）** 用于 tasks / quarantine / logs / config（小表、写少读多、需事务）。
- **扫描结果（dup_groups/files）改为由 Rust 引擎产物直接服务**，SQLite 不再存这份冗余大表——这是消除「写入卡顿」与「删除前全量读回」的最干净办法。
- **不整体替换 SQLite 为 LMDB/RocksDB**：当前瓶颈是用法而非引擎，替换带来的交叉编译/重写成本远高于收益；仅把 I/O 热点交给 Rust。

---

## 6. 分阶段实施计划

### Phase 0 — Python 快赢（低风险，数小时，立即缓解用户两个痛点）
1. `init_db` 加 `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000`（消除所有逐次 commit 的 fsync 放大）。
2. `apply_removals` 改为**批量 commit**（每 500–1000 一事务）+ 按目录预建 `makedirs`。
3. `add_groups_bulk` 的 `dup_groups` 改 `executemany`（按自增 id 区间回填 `gid`）。
4. `handle_clean` 改为**异步**：起后台线程跑删除并写日志/进度，`send_json` 立即返回；前端加轮询/复用 SSE 显示进度。
5. 删除完成后回写 `files`/`dup_groups`，避免二次清理失效。
> 预期：删除 32 万文件从「数十分钟阻塞」降到「约 20–30s、且前端有进度」；写库小幅提速。**Phase 0 即可单独交付一个补丁版本（建议 v0.5.4）**。

### Phase 1 — Rust 热点迁移（数天）✅ 已于 v0.5.5 落地
6. 引擎加 `dupscan write-db` 子命令（rusqlite WAL + 单事务预编译 INSERT + sha256 sidecar 校验 + 进度事件）。✅
7. 引擎加 `dupscan clean` 子命令（目录预建 + rayon 并行 rename/EXDEV 回退 + 每批单事务 + 进度 + SIGTERM 优雅取消 + 结束回写分组）。✅
8. Python 层改造为异步派发 + SSE 进度：`--out` 模式下 done 事件不再携带全量 groups_detail（Python 不再解析百 MB JSON）；`_clean_worker` 分页迭代生成待删清单（移除全量 `get_groups` 内存峰值）；两条路径均保留 Python 回退。✅
> 实测（macOS SSD）：5000 文件隔离删除 Rust 0.32s ≈ Python(0.5.4) 0.33s——快盘上 rename 本身是瓶颈、两者持平；Rust 版收益在 NAS 机械盘（并行 rename 掩盖寻道延迟）、独立进程隔离（不占 HTTP 服务线程/内存）、取消语义与 DB 一致性（每批事务同步删 files 行）。
> 踩坑记录：rusqlite bundled SQLite 默认 `SQLITE_DEFAULT_FOREIGN_KEYS=1`（Python sqlite3 默认关闭），需显式 `PRAGMA foreign_keys=OFF` 对齐，否则回写时 DELETE dup_groups 触发 FK 违规。

### Phase 2 — 结果存储去 SQL 化（按规模决定）
9. 评估是否让 `/groups` 直接服务 Rust 结果文件（JSON/MessagePack/Parquet），SQLite 仅留元数据——彻底去掉 48 万行冗余写读。
10. 若百万级并发读出现锁竞争，再评估 LMDB。

---

## 7. 风险与待确认
- **隔离区与数据是否同卷**：跨卷 `rename` 退化为 copy，Rust 需对大文件限速并区分策略。需确认部署拓扑。
- **取消语义**：异步删除中途中止后，已移入隔离区的文件应保留（安全优先），DB 记录「部分清理」。
- **WAL 文件**：开启 WAL 后会产生 `-wal`/`-shm` 伴随文件，`DUP_VAR` 备份/迁移时需一并处理。
- **回归**：`get_groups` 被清理逻辑与 UI 共用，去 SQL 化时要保证分页接口契约不变。

---

## 8. 建议下一步
- 若求**最快止血**：先落地 Phase 0（v0.5.4），仅 Python 改动、无需重编引擎，几小时内可交付并显著改善你的两个卡顿。
- 若求**面向百万文件的长期架构**：Phase 0 + Phase 1 一起做，把删除与回写彻底 Rust 化、异步化。
- 存储引擎替换**暂不必要**；保持 SQLite(WAL) + Rust 产物直接服务结果 的组合即可。
