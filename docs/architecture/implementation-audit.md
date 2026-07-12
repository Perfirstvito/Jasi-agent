# 主动链路实施审计

## 当前执行面

```text
Telegram Update ----> PassiveIngressService -----------+
Schedule Clock -----> ScheduleWorker -> Occurrence ----+
Source Clock --------> SourceWorker -> SourceItem ------+--> durable Work
Drift Producer ------> DriftOpportunity ---------------+         |
                                                               WorkWorker
                                                        direct / agent handler
                                                                  |
                                                        WorkFinalizer transaction
                                                                  |
                                            assistant message + Message Outbox
                                                                  |
                                               OutboxWorker -> ChannelPort

Source poll transaction -> Effect Outbox -> EffectWorker -> EffectPort
```

AgentRuntime 只接收通用 `TurnRequest`，负责 Profile、历史、模型/工具循环、Hook、Turn 和
ToolExecution。它不导入 Trigger、Telegram、Outbox、Schedule、Source、Drift 或 SQLAlchemy。

## 分阶段结果

| 阶段 | 结果 |
| --- | --- |
| Phase 0 | 完成 Akashic passive/proactive/drift/schedule 审计、竞态矩阵和目标边界 |
| Phase 1 | PostgreSQL durable Work、priority、session serialization、lease fencing |
| Phase 2 | Passive 入站原子 enqueue；Work ID Turn 恢复；统一 Message Outbox |
| Phase 3 | 独立 at/interval/cron schedule、唯一 occurrence、direct/agent action |
| Phase 4 | SourcePort、持久化 cursor/items、proactive cooldown planner |
| Phase 5 | DriftOpportunity、idle/cooldown gate、共享 InitiativePlanner/Runtime |
| Phase 6 | Work heartbeat、后台配额、Effect Outbox、运维聚合查询 |
| Phase 7 | 静态 Self/Profile、不可变上下文快照、Markdown 权威 passive memory 与可重建检索索引 |
| Phase 8 | Akashic fast/embedding 模型路由、原语锚定双查询和结构化 retrieval audit |

Prompt 采用静态 `self.md` 和四个 Profile Markdown，由 `PromptAssembler` 显式组装。`self.md` 定义
Jasi 的稳定身份、关系边界、表达习惯和条件化情绪反应，不由 Memory Worker 自动改写。Memory 只作为
`TurnContextSnapshot` 中的 derived reference data 进入模型；仍不引入 Prompt DSL、动态插件或依赖图。

## 不变量证据

### 1. 只有 OutboxWorker 调用用户 Channel

生产代码中唯一的 `channel.send(...)` 位于 `application/outbox.py`。Schedule direct、模型回复、
proactive 和 drift 全部先由 WorkFinalizer 写 Message Outbox。EffectWorker 使用独立 EffectPort，
不能获得 ChannelPort。

### 2. 同一 dedupe key 最多一个 Work

`work_items.dedupe_key` 有唯一约束；enqueue 使用 PostgreSQL `ON CONFLICT DO NOTHING`。单元与
PostgreSQL 集成测试覆盖重复 enqueue。

### 3. 同一 schedule occurrence 最多一个 Work

`(job_id, scheduled_for)` 和 `work_item_id` 在 `schedule_occurrences` 上均唯一。Scheduler 在锁住
job 的同一事务中创建 Work、Occurrence 并推进 `next_run_at`。

### 4. 同一 session 不并发执行两个 Work

claim 使用 session rank、running 排除和 `status='running'` 的 session 唯一部分索引。领取事务
还使用 PostgreSQL advisory transaction lock 来原子计算后台配额。并发 claimant 测试验证不会
重复或跨类型并发领取。

### 5. Passive 优先于尚未开始的主动 Work

claim 先选择 priority-100 passive，再在剩余容量和后台配额内选择 scheduled/proactive/drift。
WorkWorker 不等待整批后台任务结束才继续 claim。测试覆盖两个后台任务运行时新 passive 仍可执行，
以及同一 session proactive pending 时 passive 先被领取。Passive 入站还会取消 pending drift。

### 6. Runtime 成功、Finalizer 失败后不重复请求模型

Turn 以 `work_item_id` 唯一恢复。Runtime 结果提交后，Finalizer 的 assistant/Outbox 事务即使失败，
Work 重试会读取 cached TurnResult。单元测试模拟 Finalizer 失败并断言模型请求数保持为 1。

### 7. 内部输入不污染用户历史

Schedule/proactive/drift 的 input 只存在 Work 和 TurnRequest，不插入 user Message。集成测试验证
模型能看到 source/drift input，但数据库 conversation history 只包含真实 user 和已送达 assistant。

### 8. 只有 sent assistant 进入 Runtime 历史

Repository 只读取 user Message，以及 `delivery_status='sent'` 且非 `system_error` 的 assistant。
分段集成测试验证第一段送达后仍不可见，最后一段送达后才进入历史；failed/pending assistant 不可见。

### 9. 重启恢复 pending Work、Outbox 和 Effect

三类队列都以 PostgreSQL 状态为准。Work 使用可续租 lease；Message Outbox 和 Effect Outbox 会回收
stale executing/delivering 记录。集成测试用新 Repository/Worker 实例恢复 pending effect，并验证
Work/Outbox 并发领取不重复。

### 10. 新 Source 或 Channel 不修改 Runtime 主循环

Source 通过显式 `SourcePort` registry 注册，Channel 通过 OutboxDispatcher registry 注册。Runtime
只按 request.profile 从 Profile mapping 选择配置；新增 scheduled/proactive/drift 未增加模型循环分支。

### 11. Markdown 是唯一 Memory 内容事实源

`MemoryConsolidator` 只修改 Markdown 的受管区段；人工区段保留。PostgreSQL 只保存可重建记录、向量、
证据、检索审计、job 和 checkpoint，不提供 DB -> Markdown 回写。人工编辑由 `MemoryWorker` 检测 hash
变化后重建索引。Memory 根目录必须使用持久化存储。

### 12. Proactive 不直接写长期记忆

只有成功送达的 passive model reply 会创建 consolidation job。已送达 proactive assistant 可在用户后来
回复形成的 passive 窗口中提供上下文，但不能单独作为用户事实证据；没有后续互动时不会污染长期记忆。

### 13. Query Rewrite 不替代用户原语

Original utterance 和 rewritten query 分别进行 lexical/semantic search，HyDE 只做第三路增强。审计的
`query_variants` 保存每路完整文本、搜索模式和命中 ID；Gate skip 也保留 original。提取、摘要和检索判断
走独立 `JASI_LIGHT_MODEL_*`，用户可见 Runtime 仍走主模型。

## 竞态处理

| 竞态 | 当前处理 |
| --- | --- |
| 同 chat 并发 Telegram Update 争用 message sequence | conversation upsert 行锁 + 唯一 sequence |
| 重复 Telegram Update | inbound event 唯一约束 + 同事务 passive Work |
| Work lease 到期后旧执行仍运行 | heartbeat；续租失败取消旧 task；Finalizer token fencing |
| 多 worker 超过后台模型配额 | advisory claim lock + running background count |
| passive 到达 pending drift 之后 | 入站事务将该 session pending drift 标为 cancelled |
| 两个 scheduler tick 同时看到 due job | job `FOR UPDATE SKIP LOCKED` + occurrence unique |
| source poll 重放 | subscription cursor 与 source item upsert 同事务 |
| 两个 initiative planner 选同候选 | candidate row lock + session rank + initiative state row lock |
| source 标记完成但 ACK 尚未执行 | ACK 先持久化 Effect Outbox，Worker 后执行 |
| 第一分段失败、后续分段先发送 | Outbox 只领取所有前序分段已 sent 的记录；终止失败取消后续分段 |
| Memory 文件写入后、checkpoint 前崩溃 | lease 到期后重跑；稳定记录合并与 episodic key append 幂等 |
| 人工编辑 Markdown 与 worker 同时写 | expected hash 冲突使 job 重试，不覆盖人工新内容 |
| 两个 Memory worker 领取同一 scope | scope 行锁 + lease token；同一 scope 串行、不同 scope 可并发 |

## 明确语义与剩余边界

- Telegram 和一般外部 Effect 只能保证 at-least-once。发送/执行成功后、状态提交前崩溃仍可能重复；
  支持幂等键的 adapter 应使用 `outbox_id` 或 `effect.dedupe_key`。
- Interval 和 cron misfire 采用 coalesce：恢复后创建一个 occurrence，下一次推进到当前时间之后。
- 取消 schedule 只阻止未来 occurrence，不撤销已经 materialize 的 Work。
- 项目提供 SourcePort、EffectPort 和 DriftOpportunityProducer 边界，但默认不注册具体外部 connector。
- Schedule 创建当前通过 `ScheduleService`，尚未暴露 Telegram 命令或模型工具。
- 首版仍是单应用进程；数据库约束、lease 和 SKIP LOCKED 已允许未来多 worker，但没有分布式限流服务。
- Memory 的四个 Markdown 文件不是数据库备份的投影；文件系统丢失后不能从 PostgreSQL 反向恢复。
- Embedding 未配置时使用 trigram lexical recall；配置兼容 endpoint 后启用 pgvector + HyDE 混合检索。
- 当前 Akashic-compatible embedding 空间固定为 1024 维；切换模型必须显式迁移并重建 Markdown 索引。
