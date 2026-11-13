# LIMS 去标识出数 API

单实例 FastAPI + SQLite 服务：读取固定的 LIMS 检验明细 CSV 与初始同意名单，异步生成去标识批次 CSV 供统计员下载。**受试者号被替换为跨批次稳定的随机研究代号，其余所有值与行序保持不变。**

## 快速开始

```bash
docker compose up --build
```

- Swagger UI: <http://localhost:8000/docs>
- OpenAPI JSON: <http://localhost:8000/openapi.json>
- 健康检查: <http://localhost:8000/healthz>

数据卷 `deid-data` 挂载到容器 `/data`，保存 SQLite 库（同意状态、代号注册表、批次登记）、批次清单与生成的 CSV。镜像以非 root 用户运行。

### 端到端演示（生成中撤回 + 重新同意后旧批仍拒绝）

```bash
docker compose up --build -d
python3 scripts/demo.py            # 默认 http://localhost:8000
```

脚本依次演示：

1. 发起批次，生成中途撤回 `S003` → 批次 `invalidated`，下载返回 **410**；
2. 重新同意 `S003` → 旧批次**仍是 410**（永久失效）；
3. 重新同意后新建批次 → 正常 `completed`，可下载；
4. 再次撤回 → 该**已完成**批次同样转为 `invalidated`，下载 **410**。

`docker-compose.yml` 设置 `ROW_DELAY_SECONDS=0.02` 用于模拟逐行处理耗时，使第 1 步可靠地发生在生成期间；生产环境可去掉该变量（默认 0）。

## 样例数据

| 文件 | 内容 |
| --- | --- |
| `samples/inputs/lims.csv` | 固定检验明细：`subject_id,test_date,test_item,value`，10 名受试者 × 3 次检验 × 5 个项目，共 150 行 |
| `samples/inputs/initial_consent.csv` | 初始同意名单：`S001`–`S008`（每人一行，可带 `subject_id` 表头） |

输出 CSV 保留原表头与原列序，仅把 `subject_id` 列的值换成研究代号。未同意（或已撤回）者的行不会出现；因此输出行数 = 发起时已同意且在明细中出现的人数 × 其行数。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/subjects` | 列出受试者同意状态与撤回序号 |
| `GET` | `/subjects/{subject_id}/consent` | 查询单人状态 |
| `PUT` | `/subjects/{subject_id}/consent` | 请求体 `{"consented": true/false}`；授予或撤回 |
| `POST` | `/batches` | 立即受理**此刻已同意**者，后台异步生成，返回 202 与批次状态 |
| `GET` | `/batches` | 批次列表 |
| `GET` | `/batches/{batch_id}` | 批次详情（含事件时间线、失败原因） |
| `GET` | `/batches/{batch_id}/download` | 下载 CSV；校验摘要后才流式返回 |

下载状态码：

- `200` — 已完成且文件完整有效；
- `404` — 批次不存在；
- `409` — 仍在生成（`batch_running`）、已失败（`batch_failed`）或文件缺失/被改动（`file_unavailable`）；
- `410` — 因同意撤回被**永久**失效（`batch_invalidated`）。

## 核心语义

- **受理快照**：`POST /batches` 在事务内读取当时 `active=1` 的受试者并逐个登记入库，写入代号。之后新同意的人不加入本批；生成期间撤回的人使本批失效。
- **稳定代号**：代号由持久随机密钥的 HMAC-SHA256 映射所得（8 位、去掉易混字符），同一受试者跨批次恒定，且无法仅凭受试者号推导。代号只在受理事务中铸造。
- **撤回永久失效**：撤回会把该受试者登记过的**所有**批次（生成中或已完成）置为 `invalidated` 并物理删除 CSV；后续下载一律 410。该状态不可逆——重新同意也不会恢复旧批（批次受理时记录的撤回序号与会话时比对，且状态机不允许 `invalidated → completed`）。
- **生成中撤回**：工作线程每处理若干行重读状态；完成提交时在撤回锁内再次校验，确保不存在“撤回与完成”竞态。
- **可更改、可查询**：生成期间同意变更照常保存；批次与事件可随时查询。
- **已在途下载可结束**：下载在状态校验通过后即以已打开的文件句柄流式输出；期间撤回会取消后续请求，但不中断已开始的响应（POSIX 下 unlink 不影响已打开句柄）。
- **失败不供数**：生成异常、文件写入或摘要登记失败 → 批次 `failed`，且不保留可下载文件。下载前总是校验大小与 SHA-256，缺失或被篡改一律 409。
- **重启恢复**：进程启动时，把上一进程遗留的 `running` 批次全部标为 `failed`（原因含 `restarted`），并校验所有 `completed` 批次文件，损坏者标失败；完好批次仍可下载。临时文件被清理。
- **持久化**：同意状态、代号、批次登记、清单与文件全部位于数据卷；初始名单只在首次启动时播种（`meta.consent_seeded`），之后以 API 变更结果为准。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data` | 持久化目录（容器内 `/data`） |
| `INPUT_CSV` | `samples/inputs/lims.csv` | 固定明细 CSV 路径 |
| `INITIAL_CONSENT_CSV` | `samples/inputs/initial_consent.csv` | 初始同意名单路径 |
| `ROW_DELAY_SECONDS` | `0` | 每行模拟延迟，仅用于演示生成中撤回 |
| `CHECK_INTERVAL_ROWS` | `50` | 工作线程每隔多少行复查批次有效性 |

## 直接运行（不经 Docker）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DATA_DIR=./data uvicorn app.main:create_app --factory --port 8000
pytest -q                                        # 9 个测试
```

## 测试覆盖

`tests/test_api.py` 覆盖：初始同意、代号替换与跨批稳定、行序与其余值不变、生成中撤回并失效、重新同意不恢复旧批、已完成批撤回失效、生成期间同意变更照常保存且不影响受理、重启后未完成批失败而完好批可下载、文件被篡改不供下载、404/409/410 契约、已在途下载可结束而后继请求 410。
