# Passive Memory V1 架构

## 目标与不变量

Passive Memory V1 为被动对话提供长期画像、事件记忆和滑出短期窗口的摘要，同时保持以下边界：

1. `AgentRuntime` 不读写 Markdown，不调用 embedding，不依赖 SQLAlchemy，也不决定何时提取记忆。
2. Persona 是静态 Prompt 资产，Memory 无权修改 Persona、Profile 或工具权限。
3. Markdown 是内容事实源；PostgreSQL 是可重建索引、任务、证据和审计层。
4. 只有成功送达的 passive 回复触发维护；发送失败不提取，发送重试不重跑 Runtime。
5. 用户事实必须引用同一 memory scope 内的 user message；assistant 只能提供上下文。
6. Memory 读取、检索和审计故障不能阻断当前 passive 回复。
7. 同一 memory scope 的维护串行，不同 scope 可以并发。
8. 用户原语是检索 anchor，不允许 Query Rewrite 覆盖或替代原始细节。

## 模块边界

```text
Telegram / future Channel
          |
          v
PassiveIngress -> durable Work -> AgentWorkHandler
                                      |
                                      v
                                AgentRuntime
                                      |
                         TurnContextProviderPort
                                      |
                         immutable TurnContextSnapshot
                                      ^
                                      |
                            MemoryContextPort
                       / Markdown     \ PostgreSQL index

WorkFinalizer -> Message Outbox -> ChannelPort
                                      |
                         final passive part sent
                                      |
                         transactional MemoryJob
                                      |
                              MemoryWorker
                  extract -> Markdown -> index -> checkpoint
```

Application 层只依赖以下 Port：

- `MemoryDocumentStorePort`：权威文档快照、compare-and-swap 写入。
- `MemoryIndexRepositoryPort`：scope、文档状态、索引、混合搜索和检索审计。
- `MemoryJobRepositoryPort`：durable job、lease、窗口和 checkpoint。
- `EmbeddingPort`：可选批量向量能力。
- `MemoryMaintenanceReasonerPort` / `MemoryRetrievalReasonerPort`：受约束的模型推理。

具体实现分别位于 Markdown、PostgreSQL 和 OpenAI-compatible adapter；Runtime 主循环不导入这些类型。

## 模型路由

用户可见的 passive/scheduled/proactive/drift Runtime 继续使用 `JASI_OPENAI_*` 主模型。以下无用户直接
输出的后台推理统一使用 `JASI_LIGHT_MODEL_*`：

- memory candidate extraction；
- stable reconciliation；
- recent context summary；
- retrieval Gate、Query Rewrite、HyDE、Rerank 和 Sufficiency。

当前本地配置沿用 Akashic 的 `qwen-flash` fast endpoint；embedding 沿用
`text-embedding-v3`。三项 light endpoint 配置必须一起提供；全部省略时才整体回退主模型。这样 Runtime
的最低层模型/工具循环不因后台成本路由而改变。

## 权威文档

每个 scope 使用数据库保存的路径安全 UUID5 目录名，目录中固定有四个文件：

| 文件 | 所有权 | 用途 |
| --- | --- | --- |
| `MEMORY.md` | 人工区段 + Jasi 受管区段 | 稳定事实、偏好、承诺和显式 remember 内容 |
| `HISTORY.md` | 人工内容 + 自动 append-only | 有意义的 episodic 事件 |
| `RECENT_CONTEXT.md` | 按 conversation 标记的受管区段 | 已滑出最近 30 条原始历史的压缩摘要 |
| `PENDING.md` | Jasi 受管暂存区段 | consolidation 进行中的候选，便于崩溃检查 |

自动记录使用可解析 marker：

```html
<!-- jasi-memory {"id":"auto:...","tier":"stable","tags":["preference"],"evidence":[42]} -->
- The user prefers concise answers.
```

Jasi 只替换 `MEMORY.md` 和 `PENDING.md` 的 `jasi-managed` marker 区段。普通人工 Markdown 会生成
`manual:*` 索引键，稳定 reconciliation 不得 replace 或 reinforce 它。每个文件使用临时文件、`fsync`
和原子 `os.replace` 写入；`expected_hash` 防止覆盖同时发生的人工编辑。

四个文件不是一个跨文件事务。`PENDING.md` 和 deterministic record key 使重试可观察且幂等：稳定记录
按决策合并，episodic 记录只在 key 不存在时 append。数据库绝不反向覆盖文件。

## 写入链路

### 1. 送达后创建任务

Outbox 最后一分段标记 `sent` 的事务同时完成 assistant message 的 delivery 状态。只有当对应 Work 是
`kind=passive` 且 message `origin=model` 时，事务才以 `delivery:<message_id>` 唯一键创建 consolidation
job。重复 Telegram update、重复 `mark_outbox_sent` 或进程恢复都不会创建第二个 job。

默认目标是每 6 条 passive 消息，即三个已送达 user/assistant turn；配置只允许 4-8。窗口还会包含两个
checkpoint 之间已送达的 proactive/scheduled/drift assistant 作为上下文，因此实际 reasoner 输入可能略大于
目标值。这些 assistant 不能成为事实证据。

### 2. Claim 与恢复

`MemoryWorker` 使用 scope 行锁、`FOR UPDATE SKIP LOCKED` 和共享 lease token 领取一个批次。过期 lease
在未耗尽次数时回到 pending；达到五次后进入 failed。失败使用 2、10、30、120、300 秒退避。单轮 worker
异常会被记录并继续下一轮，不会终止 passive 服务。

共享 scope 下不同渠道、不同 conversation 的文件维护也按 scope 串行。每个 conversation 保留独立
consolidated/summarized checkpoint，避免交叉跳过消息。

### 3. Grounded extraction

维护 reasoner 把消息当作数据而非指令，并返回 strict JSON。候选只固定两个技术层级：

- `stable`：长期画像、偏好、承诺、明确 remember 请求。
- `episodic`：有后续意义的用户事件。

具体类别使用开放 tags，不建立封闭记忆类型枚举。候选必须至少引用一个当前窗口 user message ID；模型
返回 assistant ID、未知 ID 或其他 scope 的 ID 会分别在 reasoner 和 repository 两层被拒绝。

### 4. Consolidation 顺序

```text
extract candidates
-> stage all candidates in PENDING.md
-> reconcile stable candidates against MEMORY.md
-> append new episodic candidates to HISTORY.md
-> summarize messages that left the raw 30-message window
-> clear managed PENDING section
-> parse and index changed Markdown
-> atomically complete jobs and advance checkpoint
```

如果文件已写入但索引或 checkpoint 提交前崩溃，lease 到期后会重跑。Markdown 仍是现状事实源，索引
重建不会从旧数据库内容覆盖文件。

## 读取链路

Runtime 在 Turn 已创建后请求 `TurnContextProvider`。Provider 并发边界外只返回冻结的
`TurnContextSnapshot`，其中包含：

1. 当前入站消息之前最近 30 条真实 user 和已送达、非 system-error assistant 历史；
2. `MEMORY.md` 稳定画像全文；
3. `RECENT_CONTEXT.md` 中的历史摘要；
4. 与当前 query 相关的 episodic recall。

Episodic recall 顺序：

```text
Memory Gate
-> Query Rewrite
-> optional HyDE
-> original + rewritten + optional HyDE searches
-> merge and score threshold
-> model Rerank
-> Sufficiency Check
-> count/character budget
-> ContextItem injection + retrieval audit
```

Stable records不再参加 episodic 搜索，因为其权威 Markdown 已全文注入。这样不会重复占用 recall 预算。
Gate 只控制 episodic 搜索，不会移除稳定画像和最近摘要。所有 Memory Context 的 trust 都是 `derived`，
Prompt 将其包在 `data-reference-only` frame 中；当前用户明确陈述优先于可能过期的记忆。

原始用户表达与改写查询是两条独立检索路径：两者都做 trigram；配置 embedding 后也各自生成 vector。
HyDE 只提供第三条增强路径，不能替代 original 检索路径和审计。合并按 record ID 去重并保留各路径
最高分，再交给轻量模型 rerank。

检索审计以 Turn 唯一记录原 query、rewrite、HyDE、gate、sufficiency、轻量模型名、阶段错误、候选分数
和实际注入标记。结构化 `query_variants` 对每条路径保存完整文本、是否执行 semantic/lexical，以及该路径
的 hit record IDs。即使 Gate skip，也会保存未搜索的完整 original variant。审计写入失败只记录日志。

## Embedding 降级

Embedding 是显式可选能力：

- 未配置 `JASI_MEMORY_EMBEDDING_BASE_URL`：不发送 embedding 请求，使用 rewrite + trigram + rerank。
- 已配置 endpoint：索引 1024 维向量，original、query rewrite 与 HyDE 各生成向量并和 trigram 结果融合。
- query embedding 临时失败：当前 Turn 降级 lexical search。
- 文档 embedding 失败：maintenance job 重试，不伪造向量，也不把失败索引当作成功。

`JASI_MEMORY_EMBEDDING_BASE_URL` 和 API key 必须一起配置，避免把 DeepSeek 等主模型 key 误发给另一个
provider。`0011_akashic_model_routing` 会清空旧 1536 维模型空间中的向量、把列迁移为 1024 维，并使
`MEMORY.md`/`HISTORY.md` 索引 hash 失效；worker 随后从权威 Markdown 使用新模型重建。

## 跨渠道 Scope

默认 scope key 是 `channel:external_user_id`。Ingress 可通过 `JASI_MEMORY_SCOPE_MAP` 在进入 Work 前把多个
渠道身份映射为同一 owner key。数据库用 `conversation_memory_scopes` 固化 conversation 到 scope 的映射，
文件目录使用 owner key 的 UUID5 派生值，不直接暴露渠道 ID。

Scope 映射属于身份配置，不属于 Runtime。首版不会自动迁移已经绑定的 conversation；改变 owner 映射前
应先设计显式数据迁移，避免两个用户的记忆被静默合并。

## Proactive 污染规则

Proactive source text 只存在 Work/Turn input，不伪装成 user message。成功送达的 proactive 输出是普通
sent assistant history，保证系统知道自己发过什么，但它不会创建 MemoryJob。

只有用户后续回复并累计出 passive batch 时，这条 assistant 消息才可能作为 extraction 上下文出现；任何
长期用户事实仍必须引用用户回复。用户不互动时，推送不会进入 `MEMORY.md` 或 `HISTORY.md`，从而避免把
模型自行生成的兴趣判断写成用户画像。原始送达事实仍永久保存在 Message/Outbox 审计中。

## 运维要求

- `workspace/memory` 已从 Git 排除，但生产环境必须挂载持久卷并纳入备份。
- 应用不会自动迁移；当前要求 Alembic revision 为 `0012_memory_query_variants`。
- 人工编辑会在定期 reconcile 时按 hash 创建 reindex job，不需要重启。
- `memory_jobs.last_error`、attempts、lease 和状态聚合查询用于诊断；错误文本会截断且不包含应用密钥。
- pgvector 是索引能力，不是 Markdown 备份。禁止实现隐式 DB -> Markdown 恢复。

## V1 非目标

- 不提供 memory tool 或模型直接增删记忆的能力。
- 不自动推断或合并用户身份，不支持 scope 在线迁移。
- 不让 proactive、schedule 或 drift 单独触发长期记忆。
- 不实现 Memory 类型本体、Skill、MCP 或动态 Prompt 插件。
- 不声称 Channel exactly-once；Memory job 幂等不能消除 Telegram 在发送成功、提交前崩溃的重复窗口。
