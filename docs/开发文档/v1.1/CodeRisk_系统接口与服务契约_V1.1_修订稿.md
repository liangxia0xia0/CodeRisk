# 系统接口与服务契约

**项目名称：** CodeRisk——代码变更风险评估与任务难度量化平台  
**文档状态：** V1.1 修订稿；待与其余专项文档交叉核验，接口尚未实现  
**关联文档：**
- 《项目总纲.md》
- 《产品需求与业务规则.md》
- 《总体架构与技术决策.md》
- 《Agent 工作流与证据规范.md》
- 《认证权限与安全设计.md》
- 《数据库与任务状态设计.md》

## 一、设计目标

本文件定义系统对外 API、Go 与 Python 的内部通信、Kafka 事件、Celery 任务参数及统一错误处理规则。

设计目标：

- 所有服务使用明确、稳定的契约通信。
- 业务状态由 Go 后端统一管理。
- Kafka 负责跨服务事件，Celery 负责 Python 内部任务调度。
- 同一任务重复投递、重复回调时，不产生重复业务结果。
- 接口能够进行自动化测试、日志关联和故障排查。
- 接口优先满足 MVP，不提前建设复杂的通用 API 网关或服务治理平台。

具体 URL、字段类型、鉴权方式和错误码以本文件为基线；如与代码仓库现状冲突，先报告差异，不允许静默修改契约。

## 二、服务通信关系

| 调用方 | 接收方 | 通信方式 | 用途 |
|---|---|---|---|
| 浏览器 | Nginx | HTTP/HTTPS | 统一访问入口 |
| 前端 | Go 后端 | REST API | 认证、任务、报告 |
| Go 后端 | PostgreSQL | 数据库连接 | 业务状态和持久化 |
| Go 后端 | Kafka | 事件发布 | 发布分析任务及业务事件 |
| Python 消费者 | Kafka | 消费协议 | 接收跨服务事件 |
| Python 消费者 | Celery/Redis | 内部任务投递 | 调度后台分析 |
| Celery Worker | Go 后端 | 内部 HTTP API | 查询必要任务信息、提交分析结果 |
| Python Worker | 模型供应商 | 模型 API | 分析代码与生成结论 |

Python 不直接写入 Go 所管理的业务表。前端不直接访问 PostgreSQL、Kafka、Redis 或模型供应商的内部凭据。

## 三、通用 API 约定

### 请求规范

- API 前缀：`/api/v1`
- 数据格式：JSON，文件上传接口除外。
- 字符编码：UTF-8。
- 时间格式：RFC 3339，统一使用 UTC 存储。
- 标识符：使用稳定的任务 ID、报告 ID 等业务标识。
- 请求关联：支持 `X-Request-ID`；服务间使用 `trace_id` 关联一次处理链路。
- 分页：采用统一的 `page`、`page_size` 参数，具体上限在接口实现规范中确定。
- 敏感信息：密码、完整令牌、模型密钥不得出现在普通日志中。

### 统一成功响应

```json
{
  "data": {},
  "request_id": "req_example"
}
```

列表接口的 `data` 中应包含记录列表和分页信息，具体字段由各接口定义。

### 统一错误响应

```json
{
  "error": {
    "code": "TASK_NOT_FOUND",
    "message": "任务不存在或不可访问",
    "details": {}
  },
  "request_id": "req_example"
}
```

`message` 用于用户理解问题，`code` 用于前端分支处理，`details` 只允许包含必要且不敏感的信息。

建议使用的 HTTP 状态码：

| 状态码 | 场景 |
|---|---|
| 400 | 参数格式错误 |
| 401 | 未认证或令牌无效 |
| 403 | 已认证但无权限 |
| 404 | 资源不存在或不允许访问 |
| 409 | 状态冲突、重复操作或幂等冲突 |
| 413 | 请求体超过限制 |
| 422 | 输入内容不满足业务规则 |
| 429 | 请求频率超限 |
| 500 | 未预期的服务端错误 |
| 502/503/504 | 依赖服务异常、暂不可用或超时 |

不向用户暴露堆栈、数据库细节、内部主机地址和密钥。

## 四、对外业务 API

以下是建议的 MVP 契约。具体请求字段和响应字段在 API 实现前，需结合 PRD 及数据库设计完成最终确认。

### 认证接口

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/v1/auth/register` | 邮箱注册 |
| POST | `/api/v1/auth/login` | 登录 |
| POST | `/api/v1/auth/refresh` | 刷新令牌 |
| POST | `/api/v1/auth/logout` | 退出登录 |
| GET | `/api/v1/users/me` | 获取当前用户信息 |

注册、登录及刷新接口必须限制滥用。邮箱验证、密码重置、Refresh Token 轮换及撤销策略应在认证专项设计中确定。

### 分析任务接口

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/v1/analysis-tasks` | 提交 Diff，创建线程和第一个分析任务 |
| GET | `/api/v1/analysis-tasks` | 查询当前用户的任务列表 |
| GET | `/api/v1/analysis-tasks/{task_id}` | 查询任务状态与摘要 |
| POST | `/api/v1/analysis-tasks/{task_id}/retry` | 手动重试符合条件的失败任务 |
| POST | `/api/v1/analysis-tasks/{task_id}/reanalysis` | 主动重新分析，创建新任务 |

任务创建采用异步模式。请求成功表示任务已被系统接受，不代表分析已经完成。

建议响应：

```json
{
  "data": {
    "thread_id": "thread_example",
    "task_id": "task_example",
    "status": "PENDING",
    "created_at": "2026-10-17T10:00:00Z"
  },
  "request_id": "req_example"
}
```

任务状态只返回 `PENDING`、`QUEUED`、`RUNNING`、`SUCCEEDED`、`PARTIAL_SUCCESS`、`FAILED`。

任务列表和任务详情都必须返回前端判断终态操作所需的字段。字段类型按 JSON 约定冻结如下：

| 字段 | 含义 |
|---|---|
| `status` | 六种任务状态之一 |
| `failure_code` | 任务为 `FAILED` 时的稳定错误码。未失败时不返回可当作失败原因的值 |
| `failure_summary` | 面向用户的简要失败说明。未失败时不作为失败原因返回 |
| `attempt_count` | Go 已为该任务分配的业务执行次数，等于当前最大 `attempt_no` |
| `attempt_limit` | 固定为 4 |
| `retry_allowed` | 与 `POST .../retry` 使用同一套资格规则的计算结果 |
| `reanalysis_allowed` | 仅当状态为 `SUCCEEDED`、`PARTIAL_SUCCESS` 或 `FAILED` 时为真 |

`retry_allowed` 同时满足以下条件时为真：状态为 `FAILED`；`failure_code` 属于可手动重试的错误码；`attempt_count` 小于 4；当前没有未结束尝试。前端只读取该字段，不自行推断。并发请求仍以重试接口的事务结果为准。

创建、手动重试、重新分析和发送对话消息都支持 `Idempotency-Key`。这是 HTTP 幂等，与 Kafka 的 `event_id` 消费去重分开存储和判断。相同用户、相同幂等键、相同请求内容返回原先的结果；相同幂等键但内容不同，返回 `409` 和 `IDEMPOTENCY_CONFLICT`。

手动重试的规则：

- 路径是 `POST /api/v1/analysis-tasks/{task_id}/retry`。它不创建新的 `task_id`。请求体不得包含 `attempt_no`。
- 这是开始下一次业务执行的唯一入口。失败回调、恢复扫描、执行超时和基础设施故障都不得分配下一个 `attempt_no`。
- 仅当 `retry_allowed` 的四项条件全部成立时接受。Go 在同一个 PostgreSQL 事务中重新检查状态、失败码、未结束尝试和次数，再分配 `attempt_no`、写入新的 `event_id` 和 Outbox，并把任务改为 `QUEUED`。
- 两个手动重试并发时，只有一个事务能分配编号。失败的一方返回 `409` 与 `TASK_STATE_CONFLICT`，不写新的尝试或 Outbox。
- `SUCCEEDED`、`PARTIAL_SUCCESS`，以及仍处于 `PENDING`、`QUEUED` 或 `RUNNING` 的任务，返回 `409` 与 `TASK_STATE_CONFLICT`。
- 确定性输入错误，以及任务失败码为 `RESULT_INVALID` 时，返回 `409` 与 `RETRY_NOT_ALLOWED`。
- 四次业务执行已经用尽时，任务保持 `FAILED`，返回 `409` 与 `ATTEMPT_LIMIT_REACHED`。此时 `retry_allowed` 为否，`reanalysis_allowed` 仍可为真。
- 可手动重试的任务失败码为：`EXECUTION_TIMEOUT`、`EXECUTION_INTERRUPTED`、`MODEL_TIMEOUT`、`MODEL_UNAVAILABLE`、`RATE_LIMITED`、`ANALYSIS_FAILED`、`INTERNAL_ERROR`。
- 历史尝试和失败原因保留。重复提交同一幂等键且内容相同，返回同一次已接受的重试，不额外占用次数。

成功时返回原 `task_id`、`status`（`QUEUED`）、更新后的 `attempt_count` 和 `attempt_limit`。响应不要求调用方回传 `attempt_no`。

重新分析的规则：

- 路径仍是 `POST /api/v1/analysis-tasks/{task_id}/reanalysis`。不新增路径。
- 请求体可选 `user_note`，表示用户补充说明。省略或空字符串表示没有补充说明；非空时最多 2,000 个 Unicode 字符，超限返回 `422` 与 `INVALID_INPUT`。
- 请求体不得指定 `attempt_no`、`source_task_id` 或 `source_report_id`，也不得把本次操作改写成 `/retry`。前序任务取路径中的 `task_id`。
- 仅当源任务处于 `SUCCEEDED`、`PARTIAL_SUCCESS` 或 `FAILED` 时，Go 在同一线程创建新任务，返回新的 `task_id`、原来的 `thread_id` 和 `source_task_id`。新任务的 `attempt_no` 从 1 开始，状态为 `PENDING`。
- 创建新任务的同一个 PostgreSQL 事务必须保存不可变 Diff 副本、可选 `user_note`、前序 `task_id`，以及源任务已有当前有效报告时的前序 `report_id`。源任务没有当前有效报告时不编造 `report_id`。同一事务写入新任务的 Outbox。
- 源任务处于 `PENDING`、`QUEUED` 或 `RUNNING` 时，返回 `409` 与 `TASK_STATE_CONFLICT`，不创建新任务，也不修改源任务。
- 创建成功后，源任务保持原终态。旧报告和旧 Diff 不修改。
- 新任务分析时重新读取自己的 Diff 并重新提取证据。`user_note` 和前序报告只作为待核查参考，不能登记为本次事实。不得自动装入该线程的全部历史消息。
- 相同幂等键和相同请求体（含相同 `user_note`）返回原先创建的新任务。同一幂等键但 `user_note` 不同，返回 `409` 与 `IDEMPOTENCY_CONFLICT`。

`POST /api/v1/analysis-tasks/{task_id}/cancel` 不是 MVP 接口。首版不实现、不登记为可用契约，调用时按不存在处理。

### 对话接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/v1/threads/{thread_id}/messages` | 按时间查询当前用户在该线程的对话历史 |
| POST | `/api/v1/threads/{thread_id}/messages` | 发送追问或质疑 |

发送消息时，Go 在同一个 PostgreSQL 事务中保存用户消息和 Outbox，并确定目标 `task_id`、`report_id`。请求可以携带本线程内的 `report_id`；否则绑定该线程已有当前有效报告中、所属任务创建时间最新的一份。提交后 Worker 不得改选目标。该操作不创建新的 `task_id`，不增加 `attempt_no`，不修改报告。

没有可绑定报告时返回 `409` 与 `REPORT_NOT_READY`。已保存的历史消息仍可查询。

本次回复的上下文只包括这条用户消息、绑定同一报告的历史对话、该报告及其证据。Agent 回复沿用相同绑定，并通过历史接口可见。回复结果为等待、完成或失败。失败只表示这次回复没有生成，不改变任务和报告。

用户在普通文本中写下“重新分析”时，本接口只把它当作对话，不自动创建任务。重新分析必须调用上面的重新分析接口。

### 报告接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/v1/reports/{report_id}` | 获取分析报告 |
| GET | `/api/v1/analysis-tasks/{task_id}/report` | 根据任务查询报告 |

报告必须关联所属用户和分析任务。即使报告 ID 不可预测，也必须进行服务端权限校验。

### 内部接口

内部接口只允许经过认证的服务访问，不应直接开放给普通浏览器用户。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/internal/v1/analysis-tasks/{task_id}` | 获取执行所需的任务和 Diff。调用方必须同时提供 `attempt_no` |
| POST | `/internal/v1/analysis-tasks/{task_id}/start` | 请求把当前尝试标记为运行中 |
| POST | `/internal/v1/analysis-tasks/{task_id}/complete` | 提交 `SUCCEEDED` 或 `PARTIAL_SUCCESS` |
| POST | `/internal/v1/analysis-tasks/{task_id}/fail` | 提交当前尝试的失败信息 |
| POST | `/internal/v1/threads/{thread_id}/replies` | 提交 Agent 的对话回复 |

分析类回调的请求体必须包含 `task_id`、`attempt_no`、`event_id`。`complete` 还必须给出结果类别：`SUCCEEDED` 或 `PARTIAL_SUCCESS`。核心风险未完成时不能提交 `SUCCEEDED`。非核心失败且核心风险已完成时提交 `PARTIAL_SUCCESS`，不能改记为完整成功。

Go 只接受当前尝试、预期状态匹配且 `event_id` 未被用于另一结果的回调。过期尝试返回 `409` 与 `STALE_CALLBACK`。同一 `event_id` 重复提交已接受的结果时，返回原结果，不新建报告。

对话回复回调包含 `thread_id`、被回复的用户消息标识和原 `event_id`，不包含 `attempt_no`。Go 只在该消息仍处于等待回复时追加一条 Agent 回复并标记完成。同一 `event_id` 的重复回调返回已保存回复。

模型内部重试耗尽后，回调将这次回复标记为失败，不改变任务状态、`attempt_no` 或报告。检查点已经完成但数据库还没有回复时，只能用原 `event_id` 再次提交；已有回复则不再执行，也不追加第二条。

`fail` 的请求体除三个标识外，还必须包含稳定的 `failure_code`。Go 只接受已知错误码。该回调在同一事务中结束当前尝试并把任务记为 `FAILED`，不分配下一个 `attempt_no`，也不写入下一次 `analysis.requested`。`EXECUTION_TIMEOUT` 和 `EXECUTION_INTERRUPTED` 只由 Go 的恢复扫描写入。Python 若传入这两个码，Go 不采用，改记为 `ANALYSIS_FAILED` 后收口当前尝试。

同一当前尝试的重复失败回调返回已接受的失败结果。尝试已经结束，或任务已不再处于可接受该回调的状态时，返回 `409` 与 `STALE_CALLBACK`，不分配新编号。恢复扫描确认无法恢复时，走这同一套失败事务，不调用新的内部路径。已经进入 `RUNNING` 且执行期限已到、并经恢复判断确认无法继续时写入 `EXECUTION_TIMEOUT`；发布或排队等待超期且无法恢复，或期限未到但执行已中断且无法恢复时写入 `EXECUTION_INTERRUPTED`。两种原因同时存在且运行阶段执行期限已到时使用 `EXECUTION_TIMEOUT`。判定顺序以数据库设计为准。

内部接口的认证机制和字段类型在实现前冻结；上述标识、绑定和回复结果已经确认。

## 五、分析任务输入契约

MVP 只接受 Diff。JSON 请求提交 Diff 文本，`multipart/form-data` 上传 Diff 文件。二者进入同一个创建接口，并都规范化为该任务的一份 Diff。

文本请求的最小结构：

```json
{
  "input_type": "diff",
  "external_model_confirmed": true,
  "language": "java",
  "source": {
    "diff": "..."
  },
  "analysis_options": {
    "risk": true,
    "hardness": true,
    "test_suggestions": true
  }
}
```

`input_type` 只允许 `diff`。`commit`、`pull_request`、`repository` 返回 `422` 与 `INVALID_INPUT`。文件上传使用字段 `diff_file`，并同样要求 `external_model_confirmed=true`。未确认时返回 `422` 与 `EXTERNAL_MODEL_CONFIRMATION_REQUIRED`。确认事实由 Go 留存为确认记录或审计事件。

`language` 允许 `AUTO`、`GO`、`JAVA`、`PYTHON`、`JAVASCRIPT`、`TYPESCRIPT`、`OTHER`；默认 `AUTO`。`analysis_options` 只允许已定义布尔字段 `risk`、`hardness`、`test_suggestions`，MVP 三项均为 `true`，不支持未知字段。Diff 最大 1 MiB、最多 500 个变更文件、增删行合计最多 20,000 行；超限返回 `413` 与 `INPUT_TOO_LARGE`，且不创建任务。

实现时必须校验：

- 输入类型是否为 Diff 文本或 Diff 文件。
- 内容大小、编码和处理时限是否符合当时已配置的限制。
- 语言是否在当前支持范围内。
- 分析选项是否为系统支持的选项。
- 用户是否已登录，并且只能访问自己的线程和任务。

不得接受仓库地址、Commit、PR 编号、未经校验的本地文件路径、任意 Shell 命令或任意内部 URL。

## 六、Kafka 事件契约

### 事件信封

所有跨服务事件采用统一信封：

```json
{
  "event_id": "evt_example",
  "event_type": "analysis.requested",
  "event_version": 1,
  "occurred_at": "2026-10-17T10:00:00Z",
  "trace_id": "trace_example",
  "aggregate_id": "task_example",
  "payload": {}
}
```

字段含义：

- `event_id`：事件唯一标识，用于去重和排查。
- `event_type`：事件类型。
- `event_version`：事件结构版本。
- `occurred_at`：事件产生时间。
- `trace_id`：关联一次业务处理链路。
- `aggregate_id`：业务聚合标识，本项目主要为任务 ID。
- `payload`：事件业务内容。

事件字段新增应尽量保持向后兼容。破坏性变更必须通过版本升级处理，不能直接改变已有字段含义。

### 核心事件

| 事件 | 发布方 | 消费方 | 用途 |
|---|---|---|---|
| `analysis.requested` | Go Outbox 发布器 | Python 消费者 | 通知一次业务执行。每次新的 `attempt_no` 使用新的 `event_id` |
| `conversation.reply_requested` | Go Outbox 发布器 | Python 消费者 | 通知回复一条追问。不分配 `attempt_no` |
| `analysis.completed` | Go Outbox 发布器 | 其他订阅方（需要时） | 通知 `SUCCEEDED` 或 `PARTIAL_SUCCESS` 已持久化 |
| `analysis.failed` | Go Outbox 发布器 | 其他订阅方（需要时） | 通知任务已失败 |

MVP 必须实现 `analysis.requested` 和 `conversation.reply_requested`。完成和失败通知可以在有真实订阅方之后再实现。不实现用户取消事件。RAG 检索、Embedding 生成和 Agent Memory 的内部实现不作为新的业务任务事件，不应通过 Kafka 传递原始 Diff、对话正文、知识全文或报告正文。

### 分析请求事件

```json
{
  "event_id": "evt_example",
  "event_type": "analysis.requested",
  "event_version": 1,
  "occurred_at": "2026-10-17T10:00:00Z",
  "trace_id": "trace_example",
  "aggregate_id": "task_example",
  "payload": {
    "task_id": "task_example",
    "attempt_no": 1
  }
}
```

对话回复事件的 `aggregate_id` 使用 `thread_id`。载荷包含 `thread_id`、用户消息标识、被引用的 `task_id` 和 `report_id`，不包含 `attempt_no`。

事件只传递标识和调度信息。Diff、对话正文、知识全文、向量数据和分析报告不进入 Kafka。Python 通过内部接口读取自己有权处理的数据；RAG 通过知识检索模块访问已启用的知识切片，Agent Memory 读取必须强制执行用户归属、有效状态和相关性过滤。

分析执行如果使用 LangGraph 检查点，键必须包含 `task_id` 与 `attempt_no`。对话回复的检查点必须包含 `thread_id` 与该次 `event_id`。两类检查点不能混用，重复投递不能写入另一次执行。

### 事件发布规则

- Go 在同一 PostgreSQL 事务中写入任务或对话消息，以及对应的 Outbox 记录，然后再由发布器发送 Kafka。
- 发布器可以重复发布。消费方按 `event_id` 去重，这个去重与 HTTP `Idempotency-Key` 无关。
- 消费者先把工作可靠交给 Celery，然后才提交 Kafka Offset。Celery 没有接收成功时不得提交 Offset。
- 提交 Offset 前崩溃导致的再次投递，必须回到原来的 `task_id` 与 `attempt_no`，或原来的对话回复，不能新建任务或增加 `attempt_no`。
- 投递长期失败时，按 PostgreSQL 中尚未完成的 Outbox 和任务记录恢复发布。任务不能因为一次投递失败永久停在 `PENDING` 或 `QUEUED` 且无人接手。
- 基础设施层的重试和死信只处理事件投递，不表现为用户可见的业务重试，也不占用 4 次业务执行。
- 分区键使用 `task_id` 或对话事件中的 `thread_id`。业务是否接受结果仍由 Go 按状态和 `attempt_no` 判断。

## 七、Celery 任务契约

Celery 任务只负责 Python 内部调度，不是独立的业务任务记录。

建议的任务参数：

分析执行：

```json
{
  "task_id": "task_example",
  "attempt_no": 1,
  "trace_id": "trace_example",
  "event_id": "evt_example"
}
```

对话回复：

```json
{
  "thread_id": "thread_example",
  "task_id": "task_example",
  "report_id": "report_example",
  "message_id": "msg_example",
  "trace_id": "trace_example",
  "event_id": "evt_example"
}
```

Celery 不分配 `attempt_no`，也不创建业务任务。Worker 崩溃后的恢复继续使用原参数。模型内部重试和 LangGraph 纠错同样不改变 `attempt_no`。

分析任务只有在 Go 接受回调并完成持久化后，才能成为 `SUCCEEDED` 或 `PARTIAL_SUCCESS`。Celery 结果后端不是报告或对话历史的权威存储。输入错误和不支持的语言由 Go 记为不可重试的 `FAILED`，不能在 Celery 中无限重试。

## 八、分析结果契约

Python 成功回调携带结构化报告。Risk/Hardness 枚举、证据结构、分区状态、最低核心成功条件与部分成功判定，以《Agent 工作流与证据规范》为唯一细节来源；本节定义跨服务传输边界。

```json
{
  "task_id": "task_example",
  "attempt_no": 1,
  "event_id": "evt_example",
  "outcome": "SUCCEEDED",
  "result_version": 1,
  "summary": "在已分析范围内的变更总结",
  "change_overview": {
    "files_changed": 1,
    "insertions": 4,
    "deletions": 1,
    "languages": ["java"],
    "parse_status": "COMPLETE",
    "changed_files": ["service/order.go"]
  },
  "risk": {"status": "SUCCEEDED", "overall_level": "MEDIUM", "findings": []},
  "impact": {"status": "SUCCEEDED", "items": []},
  "hardness": {"status": "SUCCEEDED", "level": "MEDIUM", "score": null, "reasons": []},
  "test_suggestions": {"status": "SUCCEEDED", "items": []},
  "evidence": [],
  "knowledge_references": [],
  "memory_references": [],
  "limitations": [],
  "model_metadata": {"provider": "deepseek", "model": "configured-model", "prompt_version": "risk-v1", "duration_ms": 1200}
}
```

### 8.1 成功结果校验

Go 在接受成功回调前至少校验：

- `task_id`、`attempt_no`、`event_id` 与当前任务和尝试一致；
- `outcome` 仅允许 `SUCCEEDED` 或 `PARTIAL_SUCCESS`；
- 顶层字段、枚举和数据类型符合契约版本；
- 每个证据引用存在，路径属于本次 Diff，行号合法；
- 报告大小与证据数量不超过服务端限制；
- `PARTIAL_SUCCESS` 明确列出失败或跳过的非核心部分；
- `model_metadata` 不包含密钥、完整提示词或原始 Diff。
- `knowledge_references`（若存在）必须包含可追溯的知识来源标识，不得伪装成 Diff 代码证据。
- `memory_references`（若存在）必须是当前用户有权使用的记忆标识；Go 或受信任的内部服务应能校验归属，不能仅接受模型任意生成的引用。

Go 不仅依赖 Python 声称“校验通过”，还必须执行服务端可确定的契约与归属校验。校验失败不得覆盖当前报告；按错误契约拒绝回调或将当前尝试按失败规则收口。

### 8.2 失败回调

失败回调至少包含 `task_id`、`attempt_no`、`event_id`、`failure_code` 和经过脱敏的 `failure_summary`。Python 不得提交 `EXECUTION_TIMEOUT` 或 `EXECUTION_INTERRUPTED` 作为自己判定的最终失败原因；这两个代码由 Go 恢复扫描在确认无法恢复后生成。Python 的模型或工作流失败使用已定义错误码，例如 `MODEL_TIMEOUT`、`MODEL_UNAVAILABLE`、`RESULT_INVALID` 或 `ANALYSIS_FAILED`。

重复、过期或不匹配的回调必须幂等拒绝，不能覆盖当前报告，也不能分配新的 `attempt_no`。

## 九、错误处理与重试约定

下列错误码是已确认的设计约束。模型内部错误是否映射到其中某一个码，实现前可以细化，但不能另造同义码，也不能因此分配下一次 `attempt_no`。

| 错误码 | 含义 | 默认处理 |
|---|---|---|
| `INVALID_INPUT` | 输入格式、非 Diff 类型或业务参数错误 | 创建前发现则不创建任务。任务已创建后才确认时记为 `FAILED`，不允许手动重试，不分配下一次 `attempt_no` |
| `INPUT_TOO_LARGE` | 输入超过限制 | 不创建任务 |
| `UNSUPPORTED_LANGUAGE` | 语言不受支持 | 创建前发现则不创建任务。任务已创建后才确认时与 `INVALID_INPUT` 的失败处理相同 |
| `EXTERNAL_MODEL_CONFIRMATION_REQUIRED` | 提交前未确认 Diff 会发送给外部模型 | 不创建任务 |
| `TASK_NOT_FOUND` | 任务、线程或报告不存在，或不属于当前用户 | 终止并排查 |
| `TASK_STATE_CONFLICT` | 当前状态不允许手动重试、重新分析或生成分析性回复，或并发手动重试未抢到事务 | 不增加 `attempt_no` |
| `RETRY_NOT_ALLOWED` | 失败类型不允许手动重试 | 不增加 `attempt_no` |
| `ATTEMPT_LIMIT_REACHED` | 该任务 4 次业务执行已用尽 | 拒绝手动重试，任务保持 `FAILED` |
| `IDEMPOTENCY_CONFLICT` | 相同幂等键对应了不同请求内容 | 拒绝，不创建新结果 |
| `REPORT_NOT_READY` | 追问没有可引用的当前有效报告 | 不创建任务 |
| `SOURCE_INPUT_EXPIRED` | 原始 Diff 已按保留策略清理，不能再用旧输入重新分析 | 不创建新任务；提示用户重新提交 Diff |
| `STALE_CALLBACK` | 回调的尝试或状态已过期 | 拒绝覆盖，不分配下一次业务执行 |
| `EXECUTION_TIMEOUT` | 当前尝试超过已配置执行期限，且确认无法继续恢复 | 失败事务收口为 `FAILED`。不分配下一次 `attempt_no`。次数未用尽时允许手动重试 |
| `EXECUTION_INTERRUPTED` | 发布/排队等待超期且无法恢复，或执行期限未到但当前尝试已中断且无法按原 `attempt_no` 恢复 | 失败事务收口为 `FAILED`；不分配下一次 `attempt_no`。次数未用尽且该失败码允许重试时，可由用户手动重试 |
| `RATE_LIMITED` | 请求或模型调用被限流 | 先在当前执行内退避。耗尽后记为 `FAILED`，不自动开始下一次业务执行 |
| `MODEL_TIMEOUT` | 模型调用超时 | 当前执行内部有限重试。耗尽后记为 `FAILED`，不分配新的 `attempt_no` |
| `MODEL_UNAVAILABLE` | 模型服务不可用 | 同上 |
| `ANALYSIS_FAILED` | 分析流程失败 | 记为 `FAILED`。不自动分配下一次业务执行；次数未用尽时允许手动重试 |
| `RESULT_INVALID` | 分析结果未通过校验 | 当前执行内有限修正。作为任务失败码时不允许手动重试，用户仍可重新分析 |
| `INTERNAL_ERROR` | 未分类的内部异常 | 记为 `FAILED` 并保留诊断。不由 Python 新建任务或增加 `attempt_no` |

错误响应不能包含密码、令牌、模型密钥、原始 Diff、内部堆栈或其他用户的数据。

业务执行最多 4 次，由 Go 计数。只有创建任务和手动重试分配 `attempt_no`。Kafka、Celery、模型客户端、LangGraph 和恢复扫描必须受各自预算限制，并且不得乘成新的业务执行。具体退避秒数、模型请求超时、执行期限和恢复判定窗口由服务配置给出，并通过故障测试校准；不能由请求体传入。原始 Diff 到期清理后，重新分析返回 `SOURCE_INPUT_EXPIRED`，提示用户重新提交输入。

## 十、接口安全与服务鉴权

### 用户接口

- 通过服务端校验用户身份。
- 每次读取或修改线程、对话消息、任务和报告时检查所属用户。
- 对注册、登录、刷新、任务创建、手动重试、重新分析和发送消息实施必要的限流。
- 校验请求体大小、字段类型和枚举值。
- 对 Diff 文本和 Diff 文件设置资源限制。MVP 不抓取仓库，不接受压缩包形式的仓库快照。

### 内部接口

内部接口不能仅依靠路径名称或“只有内部调用”的假设保护。

至少采用一种明确的服务身份认证方案，并结合网络访问限制。认证方式应避免把长期静态密钥散落在代码仓库中。

内部接口还必须具备：

- 请求签名或其他可信身份校验。
- 请求超时和大小限制。
- `task_id`、`attempt_no`、`event_id` 与预期状态校验。对话回复则校验 `thread_id` 与 `event_id`。
- 幂等回调。过期回调不得覆盖报告或对话。
- 关联日志和异常审计。

### 外部模型服务

模型供应商的 API Key 必须通过服务端安全配置注入。提交 Diff 前的告知、确认和留存已经是 MVP 业务规则。是否支持用户自带 API Key，以及凭据轮换细节，仍在安全设计中确定。普通日志不记录 API Key、Token、原始 Diff 和完整对话正文。

## 十一、RAG 与 Agent Memory 契约边界

RAG 和 Agent Memory 是 Python 分析流程中的上下文能力，不改变 Go 的业务状态权威，也不创建新的 `task_id` 或 `attempt_no`。

- BGE-M3 负责将知识片段和查询编码为向量；PostgreSQL/pgvector 负责向量存储与检索。接口层不得把 Embedding 模型当成数据库或把向量相似度当成事实证明。
- 检索结果应保留知识文档 ID、版本、片段 ID 等来源信息，并通过 `knowledge_references` 或报告内部约定字段追溯。若本次未使用知识检索，引用列表可为空。
- Agent Memory 只能读取当前用户有权访问且状态有效的记忆；禁止跨用户召回。引用记忆必须可追溯，且不得替代当前 Diff 的证据。
- Kafka 事件和 Celery 参数只携带任务调度所需标识，不传送知识正文、Embedding 向量、记忆正文或完整提示词。Worker 通过受控内部接口或模块读取所需上下文。
- 对话追问只绑定接受时确定的 `task_id` 和 `report_id`。如果使用 Agent Memory，也必须按当前用户过滤；不得自动拼接所有历史对话。
- 知识检索失败或记忆不可用时，按 Agent 工作流契约决定是否降级为无该上下文的分析，并在报告中记录限制；不得伪造检索成功或来源引用。

以上是跨服务契约边界。知识导入、切分、Embedding 重建、记忆写入审批和数据删除的物理实现，分别由 Agent 工作流、数据库、安全和实施文档定义。

## 十二、契约测试

所有关键接口和事件都应具备自动化契约测试。

### API 测试

- 合法请求能够得到符合契约的响应。
- 错误请求返回正确状态码和稳定错误码。
- 未认证请求不能访问受保护接口。
- 用户 A 无法读取或修改用户 B 的线程、消息、任务和报告。
- 相同幂等键和相同内容不产生第二个任务；相同幂等键但内容不同会被拒绝。
- 重复回调和过期回调不覆盖当前报告。
- 手动重试、失败收口和重新分析分别遵守任务、次数、失败码和历史规则。失败回调不增加 `attempt_no`。
- 重新分析保存可选 `user_note` 与前序任务、前序报告的关联；新任务重新读取 Diff。
- 非法任务状态转换会被拒绝。取消接口不可用。

### Kafka 测试

- Outbox 发布失败后可恢复。
- 重复事件不会重复创建业务结果。
- 消费者在 Celery 尚未可靠接收时不会提交 Offset。
- 投递失败后的重新发布不会让任务永久停留在未执行状态，也不会新增 `attempt_no`。
- 事件版本不兼容时能够被识别并告警。
- 消息积压和死信能够观测与排查。

### Celery 测试

- Worker 重启后任务能够按设计恢复。
- Celery 恢复和模型内部重试不增加 `attempt_no`。
- 重复执行不会重复提交当前有效报告或对话回复。
- 永久性错误不会无限重试。
- Celery 任务成功但结果回调失败时，能够安全恢复提交过程。

### 端到端测试

至少验证：

用户确认并提交 Diff → Go 创建线程、任务和 Outbox → Kafka → Celery → 分析 → Go 保存报告 → 用户追问 → 独立的对话事件 → Go 追加回复。

另测失败后的手动重试、次数用尽、并发重试只有一方成功、重新分析保留旧报告并带上可选 `user_note`，以及模型不可用、重复事件、Worker 崩溃、执行超时和执行中断。超时与中断都必须得到 `FAILED` 和对应错误码，且 `attempt_count` 不增加。

## 十三、已冻结契约与可配置默认值

在正式实现前，需要结合数据库设计和安全设计明确：

- 注册是否需要邮箱验证。
- Access Token 与 Refresh Token 的有效期、轮换和撤销方式。
- Java 与 Python 的首批语法支持范围，以及 Diff 大小上限。
- Agent 契约以《Agent 工作流与证据规范》为准，若契约升级须同步版本。
- 内部服务鉴权的具体方案。
- Kafka Topic、分区数、保留策略及消费者组配置。
- 退避时间、模型单次超时、业务尝试执行期限、发布或排队等待期限、恢复判定窗口，以及这些项在 Go 服务配置中的键名。判断责任和超时后的状态处理已经确认。
- 数据保留期限以《认证权限与安全设计》为准；上线前须确认清理作业已实现。
- MVP 由平台服务端统一管理模型凭据，不接收用户自带 Key。

以下契约均已冻结，实施时不得自行改变：MVP 只接受 Diff 文本和 Diff 文件；取消不是 MVP；下一次业务执行只由手动重试触发；失败回调和恢复扫描不分配 `attempt_no`；`EXECUTION_TIMEOUT` 与 `EXECUTION_INTERRUPTED` 的区分；重新分析沿用原路径并可选接收 `user_note`；同一任务只有一个未结束尝试；追问在接受时绑定报告；回复的等待、完成和失败；回调三字段；HTTP 幂等与 Kafka 去重分离；Offset 在 Celery 可靠接收后提交。非终态任务不能重新分析。这些是设计约束，接口尚未实现。V1.1 同时明确：分析范围为 Java/Python 同语言变更；BGE-M3 + PostgreSQL/pgvector 是当前 RAG 首选方案；RAG 来源和 Agent Memory 引用必须可追溯并受用户隔离约束。

默认超时参数：模型单次请求 30 秒；发布/排队等待 120 秒；任务执行 180 秒；恢复判定窗口 60 秒；结构修正最多一次；基础设施自动重投最多三次并使用约 1、2、4 秒指数退避加抖动。它们均由服务端配置控制，调参须同步测试与文档。

## 十四、完成标准

本文件对应的设计工作完成，需要满足：

- [ ] 对外 API、内部 API、Kafka 事件和 Celery 任务之间的边界清晰。
- [ ] 每个核心字段有明确含义。
- [ ] 任务 ID 能够贯穿完整链路。
- [ ] 业务任务状态只由 Go 后端统一管理。
- [ ] Kafka 重复投递、Celery 重复执行和内部回调重复请求均有幂等策略。
- [ ] Outbox、重试、超时和失败恢复有可验证规则。
- [ ] 关键错误能够映射到稳定错误码。
- [ ] 安全边界、用户数据隔离和服务鉴权已经纳入契约。
- [ ] 关键接口及完整异步链路具备契约测试计划。
- [ ] 未决事项已经明确列出，没有把草案字段误认为已冻结的最终 API。

任务状态和执行次数以《数据库与任务状态设计.md》为准；Agent 节点、证据结构和报告业务语义以《Agent工作流与证据规范.md》为准。本文件只定义接口、事件和错误码如何表达那些规则。物理字段类型、索引和 SQL 约束以数据库迁移实现为准；报告 JSON 契约则以《Agent 工作流与证据规范》为准。
