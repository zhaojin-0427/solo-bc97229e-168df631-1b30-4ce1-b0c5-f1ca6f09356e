# Citation Corpus Collector

沿论文站的参考文献构建引用语料的采集系统：**FastAPI + SQLite** 采集 API、双采集槽并发、
全量状态持久化、中断自动续采，附带一个含循环引用 / 迟到短路径 / 一次请求失败的本地论文站。
在已采集语料之上还提供**后台寻链 API**：沿已存引用方向查找两篇已登记论文间边数最少的
引用链（含各页题名），支持进度查询与取消。

## 快速开始

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| crawler | http://localhost:8000/docs | 交互式 API 文档（Swagger UI），全部演示在此完成 |
| papersite | http://localhost:8080/papers/p0.html | 本地论文站（静态 HTML + 程序化大图） |

采集状态与寻链结果保存在命名卷 `crawler_data` 的 SQLite 中，`docker compose restart crawler`
甚至 `down`/`up` 之后仍可续采、完整寻链结果不丢失。

## 演示流程（在 Swagger UI 中操作，或用 curl）

**1. 提交采集作业** — `POST /jobs`：

```json
{"seed_url": "http://papersite:8080/papers/p0.html", "max_depth": 4}
```

种子深度为 0，`max_depth` 取值 0～4。

**2. 观察采集** — `GET /jobs/1`。论文站里 `slow.html` 会延迟 2.5s 响应：
另一个采集槽不会等同层慢页，会先把长链 `c1→c2→c3→X` 抓完（X 先以深度 4 登记）；
慢页到达后揭示短路径 `slow→X`，X 的深度被下调为 2，并沿已存引用边传播，
使原本超深的 `Y`（5→3）、`Z`（4）进入深度范围被**补抓**。最终：

```
status: failed          # broken.html 首次请求返回 500，仍有失败 -> 不得报完成
counts: {done: 13, failed: 1, external: 1, non_html: 1}
by_depth: {0:1, 1:3, 2:6, 3:3, 4:3}     # X 已在深度 2，Y 在 3，Z 在 4
```

**3. 查看失败** — `GET /jobs/1/failures` → `broken.html / HTTP 500`。

**4. 重试续采** — `POST /jobs/1/retry`：失败页重新入队（已采内容保留、不重复登记），
`broken.html` 第二次请求成功，作业转为 `completed`。

**5. 中断续采** — 再建一个作业（`POST /jobs`），在 2.5s 慢页窗口内执行
`docker compose restart crawler`：进程重启后自动把 `fetching` 重置为 `pending`
并恢复全部 running 作业，不漏抓、不重复登记（`papers` 表 `UNIQUE(job_id,url)`、
`citations` 表主键去重）。

**6. 查询语料**：

- `GET /jobs/1/papers`（可按 `status`、`depth` 过滤）— 论文 URL、深度、题名、状态
- `GET /jobs/1/citations`（可按 `src` 过滤）— 引用边

**7. 寻链（小图）** — `POST /jobs/1/paths`：

```json
{"start_url": "http://papersite:8080/papers/p0.html",
 "end_url":   "http://papersite:8080/papers/Z.html"}
```

返回 201 与搜索号（后台执行）。`GET /paths/{id}` 查看进度与结果：最短路
`p0 → slow → X → Y → Z`（4 条边，同长任选），`result.chain` 带每页题名。
反向 `Z → p0` 则为 `not_found`，`note` 注明"仅针对搜索开始执行时的语料快照"。

**8. 寻链 + 取消（大图）** — 先用大图源播种第二个作业：

```json
{"seed_url": "http://papersite:8080/papers/big/index.html", "max_depth": 4}
```

程序化生成 1 + 1500 + 3000 共 4501 页（hub → mids → leaves，leaves 回链 hub 构成大环），
采集约需 20～30 秒。完成后提交一个会扫遍全图的寻链：

```json
{"start_url": "http://papersite:8080/papers/big/l/00_00.html",
 "end_url":   "http://papersite:8080/papers/big/l/999_00.html"}
```

`GET /paths/{id}` 可看到 `progress`（当前层深、已展开节点数、已访问数、前沿大小）
持续增长；演示配置下搜索约 3～4 秒，期间 `POST /paths/{id}/cancel` 立即受理并
中止实际遍历（不等遍历结束），状态变为 `cancelled` 且之后不会再出现结果。
搜索期间采集、重试与查询接口照常可用。连续提交 4 个寻链可观察到最多 2 个
`running`、其余 `queued`（后台执行数量上限）。

**9. 重启语义** — 在大寻链运行中 `docker compose restart crawler`：已完成的寻链结果
完整保留，未完成的搜索标记为 `interrupted`，不提供部分链。

## 演示语料站结构

```
p0 ─┬─> p1 ─┬─> p2 ─┬─> p3 ─> p1          循环引用（登记一次，不重复展开）
    │       │       └─> p4 ─> p5 ─> mailto:（非 http，忽略）
    │       │              └─> slides.pdf  非 HTML，记 non_html
    │       ├─> p3 (p2.html#related 片段已去掉)
    │       ├─> broken.html                首次 500，重试成功（一次请求失败）
    │       └─> http://example.org/...     非同源，记 external 不抓
    ├─> slow ──────────────> X             迟到短路径（延迟 2.5s）：X 深度 4→2
    └─> c1 ─> c2 ─> c3 ─> X ─> Y ─> Z      长链；Y 深度 5→3 补抓，Z 深度 4 补抓

big/index.html ─┬─> big/m/00..1499（1500 个中间页，各链 2 个叶子）
                │       └─> big/l/NN_00..01（3000 个叶子，各回链 index 构成大环）
                └─> http://example.org/...  外部链接（登记不抓）
```

## 采集规则实现

- **仅同源、无重定向、静态 HTML**：`httpx` 关闭重定向跟随，3xx 记为失败；
  非 `text/html` 记 `non_html`；非同源 URL 登记为 `external` 不抓取。
- **解析**：`<title>` 为题名，`a.citation` 的 `href` 为引用；相对地址按当前页
  `urljoin` 解析，`urldefrag` 去掉片段。
- **双采集槽**：每作业 2 个 worker 各自认领下一待抓页（按深度、登记序），
  槽空且有待抓页即发起请求，无同层等待。
- **深度即最短距离**：发现更短路径时下调深度并沿已存引用边传播（严格递减、
  有下界，引用环安全），新进入深度范围的论文自动补抓，已抓页面不重复抓取。
- **共同持久化**：页面状态、引用边、待抓进度在同一 SQLite 事务中提交；
  重启时 `fetching → pending` 并恢复所有 running 作业。
- **失败语义**：请求失败仅标记该页 `failed`，已采内容保留；作业存在失败时
  状态为 `failed` 而非 `completed`；`POST /jobs/{id}/retry` 重新入队失败页续采。

## 寻链实现

- **后台执行、数量有上限**：固定工作线程池（`SEARCH_WORKERS`，默认 2）逐个执行，
  其余搜索排队（`queued`）。
- **语料快照**：搜索在开始执行时开启业务库的**只读快照事务**（WAL 模式），
  只看到那一刻已完整提交的语料；之后的补抓、重试、新提交对其不可见。
  未找到时 `note` 注明结论仅针对该快照（含快照时间）。
- **不占内存、不占业务库写事务**：BFS 的已访问集（含父指针）与前沿存放在
  每个搜索独立的暂存 SQLite 文件中，按块（`SEARCH_CHUNK_SIZE`）处理，
  整图与搜索状态都不进内存；业务库只承担提交/认领/完成/取消等极短的元数据写，
  搜索期间采集、重试、查询照常进行。搜索结束后暂存文件即删除。
- **最短链**：按层 BFS，首次发现终点即为边数最少（同长任选）；已访问集保证
  引用环不会被重复展开。题名取自同一快照。
- **取消**：取消请求立即受理（CAS 把 `queued|running` 置为 `cancelled`）并置位
  取消标志，遍历循环在节点粒度检查该标志，实际搜索随即中止——不等遍历结束。
  取消先受理则工作线程发布结果时的 CAS 必然失败，迟到结果被丢弃；
  完成先生效则取消返回 409。
- **重启**：`completed`/`not_found`/`cancelled` 结果持久保留；重启时
  `queued|running` 标记为 `interrupted`，暂存文件清除，不提供部分链。
- **演示节拍**：`SEARCH_CHUNK_DELAY_MS`（默认 200）在每块之间加入可中断的停顿，
  让小语料上的取消也能在 Swagger UI 中演示；生产置 0。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/jobs` | 创建作业 `{seed_url, max_depth(0-4)}` |
| GET | `/jobs` / `/jobs/{id}` | 作业列表 / 状态与进度（按状态、深度统计） |
| POST | `/jobs/{id}/retry` | 失败页重新入队并续采 |
| GET | `/jobs/{id}/failures` | 失败 URL 及错误 |
| GET | `/jobs/{id}/papers` | 论文查询（`status`、`depth` 过滤） |
| GET | `/jobs/{id}/citations` | 引用边查询（`src` 过滤） |
| POST | `/jobs/{id}/paths` | 提交后台寻链 `{start_url, end_url}`（须已登记于该作业） |
| GET | `/paths` | 寻链列表（`job_id` 过滤） |
| GET | `/paths/{id}` | 寻链状态、实时进度与结果（链含各页题名） |
| POST | `/paths/{id}/cancel` | 取消排队/运行中的寻链（已完成则 409） |

寻链状态机：`queued → running → completed | not_found | cancelled | interrupted`
（另有余留的 `failed` 表示内部错误）。`result.chain` 仅在 `completed` 时提供。

## 配置（docker-compose.yml 中可调）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SEARCH_WORKERS` | 2 | 同时执行的后台寻链数量上限 |
| `SEARCH_CHUNK_SIZE` | 100 | 每块处理的前沿节点数（内存上界） |
| `SEARCH_CHUNK_DELAY_MS` | 200 | 块间演示停顿；0 关闭 |
| `SEARCH_STATE_DIR` | `<db目录>/search_state` | 寻链暂存文件目录 |
| `BIG_MIDS` / `BIG_LEAVES` | 1500 / 2 | 论文站程序化大图规模 |

## 本地开发（不用 Docker）

```bash
pip install -r crawler/requirements.txt
( cd papersite && python server.py )                       # :8080
( cd crawler && DATABASE_PATH=./crawler.db \
    uvicorn app.main:app --port 8000 )                     # :8000/docs
```
