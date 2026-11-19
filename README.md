# 数据质量执行器 · Pointwise Data Quality Executor

一个可运行的数据质量执行系统：规则版本化、失败记录隔离、仅回放失败数据、多轮修复历史保留、发布门禁。

- **后端**：FastAPI + SQLite（标准库 `sqlite3`，隔离分区用表实现）
- **前端**：原生 HTML/CSS/JS 单页控制台（无构建步骤）
- **表达式规则**：基于 AST 的受限求值器，不调用 `eval`，无属性访问/导入/lambda

## 快速开始

```bash
./run.sh                      # 首次会自动建 venv 并装依赖，默认 http://127.0.0.1:8000
# 或手动：
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DQ_DB_PATH=dq.db .venv/bin/uvicorn app.api:app --port 8000
```

打开浏览器访问根路径，点击右上角 **「载入演示数据」** 即可获得：参照表 `customers`、
`orders` 上的五类规则、以及一个含失败记录的接入批次。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v          # 14 个端到端/单元测试
.venv/bin/python tests/smoke_e2e.py           # 15 项完整业务流冒烟检查
```

## 五类规则

| 类型 | config 示例 | 失败条件 |
| --- | --- | --- |
| `primary_key` | `{"fields": ["order_id"]}` | 键为空、批内重复、与**其他修复血缘**中已持久化的键冲突 |
| `unique` | `{"fields": ["email"]}` | 批内组合值重复（NULL 不触发，遵循 SQL 语义） |
| `range` | `{"field":"amount","min":0,"max":10000,"allow_null":false}` | 非数值、越界（含开闭区间）、null 策略 |
| `cross_table` | `{"field":"customer_id","ref_dataset":"customers","ref_field":"id"}` | 参照表中无匹配（隔离中的参照记录不计为有效） |
| `expression` | `{"expr":"quantity >= 1 and amount >= 0"}` | AST 求值为假或出错 |

表达式仅允许：字面量、记录字段、比较/布尔/算术运算、容器，以及白名单函数
（`len/abs/min/max/round/str/int/float/bool/sum/any/all/lower/upper/is_null`）。
`open`、`__import__`、属性访问、lambda 等在**规则创建时**即被拒绝。

## 核心数据模型

- `rules` / `rule_versions`：规则与不可变版本（`draft → published → archived`）。
  升级即新增版本，历史版本全部保留；回放始终按**当前已发布版本**重新检查。
- `batches`：接入批次（`ingest`）与回放批次（`replay`）。回放批次以
  `parent_batch_id` / `root_batch_id` 串成修复血缘。
- `records`：不可变记录，修复从不就地更新，而是在子批次中写入新版本。
- `quarantine_entries`：每个「数据集 + 记录 + 规则」一行的**当前**隔离状态
  （`open` / `fixed`）。
- `quarantine_events`：**append-only** 事件流，保留每次评估结果
  （`quarantined / still_failing / fixed / re_quarantined`）及当时的规则版本与载荷。
  同一记录多轮修复的完整历史因此永不丢失。

### 回放语义（关键设计）

1. 只选取根批次血缘下仍 `open` 的记录；无 open 记录时回放被拒绝（空回放 409）。
2. 一条记录可能违反多条规则而有多条 open 条目。回放按 **record_key 归一**，
   取最近更新的载荷作为权威版本，因此每条记录在回放批次中只出现一次。
3. 同根血缘内的同键记录是同一逻辑记录的修复版本，**不算主键冲突**；
   只有不同血缘间的同键才算真实碰撞。

## 发布门禁

`GET /api/gate/{root_batch_id}` 覆盖整条修复血缘：

- `block`：仍有 open 隔离记录 → 禁止发布（`POST .../publish` 返回 409）。
- `pass`：所有隔离条目已解决 → 可一次性发布整棵血缘。
- `warn`：没有任何规则被评估（数据未校验），不阻断但显式提示。

## 主要 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/rules` | 创建草稿规则（config 与表达式即时校验） |
| POST | `/api/rules/{id}/publish` · `/upgrade` | 发布 / 升级出新版本 |
| GET | `/api/rules?dataset=` | 规则与全部版本 |
| POST | `/api/ingest` | 接入批次并立即执行质量检查（空批次 400） |
| GET | `/api/batches` · `/api/batches/{id}/failures` | 批次与失败样本 |
| PUT | `/api/quarantine/{ds}/{key}/repair` | 暂存修复载荷 |
| POST | `/api/batches/{id}/replay` | 仅回放 open 失败数据，生成子批次（空回放 409） |
| GET | `/api/quarantine` · `/api/quarantine/{id}/history` | 隔离区与多轮修复历史 |
| GET/POST | `/api/gate/{root}` · `/api/gate/{root}/publish` | 门禁评估 / 发布 |

## 已验证的场景

测试与冒烟脚本明确覆盖：

- **规则升级**：v1 上限 100 → v2 上限 60，回放按 v2 重判，历史同时保留 v1/v2 快照。
- **空批次**：空接入返回 400；全部修复后的空回放返回 409（`NOTHING_TO_REPLAY`）。
- **部分失败**：一批中部分记录通过、部分隔离；门禁 block，修复回放后逐步收敛到 pass。
- **多轮修复历史**：`quarantined → still_failing → fixed`，以及修好后再坏的
  `re_quarantined`，事件与规则版本轨迹完整保留。
- 全部五类规则、沙箱安全拦截、跨血缘主键冲突、参照表隔离记录失效等边界。
