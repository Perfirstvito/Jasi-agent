# 主动链路审计与迁移设计

## 目标

在 Jasi 现有被动链路基础上增加 schedule、proactive 和 drift，同时满足：

- AgentRuntime 只负责模型和工具执行，不感知 Trigger、Channel 或投递方式。
- 四类入口共享同一个持久化 Work 执行面，不各自实现模型循环。
- 所有用户可见消息都先写 PostgreSQL Outbox，业务代码不直接调用 Channel。
- 同一会话跨 passive、schedule、proactive、drift 串行，不同会话并发。
- 每个 Trigger、Work、Turn、Outbox 都有幂等键和可恢复状态。
- 内部任务输入与用户真实消息分离，避免污染聊天历史。

首版仍保持单应用实例，但数据模型和领取协议不能依赖进程内状态才能正确恢复。

## Akashic 审计

### Passive

当前链路已经拆出 ContextStore、Reasoner 和生命周期 Phase，但仍存在以下边界问题：

- Passive Pipeline 直接调用 OutboundPort，发送没有持久化 Outbox。
- 会话 busy 状态是进程内计数器，只能降低冲突概率，不能形成跨链路原子互斥。
- Prompt、Memory retrieval、工具发现、模型循环和 session 修剪集中在 Reasoner 中。
- session 消息与发送结果不是一个可靠提交单元。

可复用思想：上下文准备和模型执行应分开；不可复用部分：直接 dispatch 和进程内 busy 判断。

### Proactive

`ProactiveTurnPipeline` 同时承担：

1. presence/cooldown Gate；
2. alerts/content/context 拉取；
3. Prompt 和工具 schema 组装；
4. 独立模型工具循环；
5. reply/skip 判断；
6. 去重、ACK 和发送。

其依赖数量和状态字段反映了职责聚合。`TurnOrchestrator` 统一了一部分结果处理，但仍先写
session、再直接 dispatch，并使用不可持久化 callback 表达副作用。

### Drift

Drift 被放在 Proactive 的“没有 feed”分支中，并拥有另一套模型/工具循环。这样会导致：

- Drift 的调度、冷却和生命周期依赖 Proactive。
- 工具执行、步骤记录和终止规则与主 Runtime 重复。
- `message_push` 可以在 Drift 尚未完成、运行状态尚未落库时直接发送。

Drift 的业务本质不是另一种 Runtime，而是“空闲时是否创建一次 initiative work”的 Planner。
它可以使用 proactive 已采集的兴趣候选，也可以使用 Skill/Memory 产生候选；差异应体现在
候选选择和 Runtime Profile，而不是独立执行循环。

### Schedule

`SchedulerService` 同时负责时间解析、JSON Store、tick、AI 执行、直接推送和重排。

- `instant` 任务直接调用 push tool。
- `soft` 任务调用 AgentLoop 后再次直接 push。
- `_in_flight` 只在内存中。
- job 执行和下一次 `fire_at` 持久化之间存在崩溃窗口。

Schedule 的领域职责应止于“在某个逻辑时间产生一次唯一 Work”。它不应知道 Runtime 或 Channel。

## 已识别竞态

| 窗口 | Akashic 当前后果 | Jasi 目标处理 |
| --- | --- | --- |
| proactive Gate 通过后、发送记录前并发第二个 tick | 两次主动发送 | 会话级 Work 领取互斥 + delivery reservation |
| passive 在 proactive Gate 后开始 | 同一会话同时回复 | 所有 Work 共用 session 串行规则，passive 高优先级 |
| schedule 已发送但尚未删除/重排 job 时崩溃 | 重启后重复发送 | due occurrence 唯一键 + Work/Outbox 状态恢复 |
| schedule 重排完成但 Work 未落库时崩溃 | 本次任务永久丢失 | enqueue occurrence 与推进 next_run 同一事务 |
| source 拉取成功但 ACK/seen 未持久化时崩溃 | 重复候选 | `(source, external_id)` 唯一 upsert |
| source 标记 seen 后、Work 创建前崩溃 | 候选丢失 | source item 状态与 Work enqueue 同一事务 |
| drift message_push 成功但 drift run 未记录 | 冷却失效并可能重复 | Drift 只产生 Work；发送由 Outbox，运行状态持久化 |
| session 先写 assistant、Channel 后发送失败 | 未送达内容进入历史 | 仅 sent assistant 进入历史 |
| Channel 发送成功、Outbox 标记前崩溃 | 可能重复发送 | at-least-once；记录外部 ID，支持渠道幂等键时使用 |
| Work lease 到期但旧执行仍在运行 | 两个 Runtime 并发 | lease heartbeat + 同会话数据库互斥 |
| 多类后台 Work 同时占满模型并发 | passive 延迟 | priority + 全局/后台并发配额 |

Telegram 不提供通用发送幂等键，因此 Outbox 最后一项只能做到 at-least-once。系统应消除业务层
重复执行，不能声称 Channel 层 exactly-once。

## 目标边界

```text
Trigger Adapter
  Telegram | Schedule Clock | Source Poller | Drift Clock
                         |
                         v
Application Ingress / Planner
  PassiveIngress | SchedulePlanner | InitiativePlanner
                         |
                         v
                  durable work_items
                         |
                         v
                    WorkWorker
                         |
                  session serialization
                         |
                         v
                 WorkHandler Registry
  DirectDeliveryHandler | AgentWorkHandler
                         |
                         v
                    AgentRuntime
                         |
                         v
       assistant message + work complete + Outbox
                  (single transaction)
                         |
                         v
              OutboxWorker -> ChannelPort
```

### Trigger

Trigger 只说明“为什么现在产生工作”，不执行工作：

- Telegram Update 创建 passive Work。
- Schedule due occurrence 创建 direct 或 agent Work。
- Source Poller upsert SourceItem，InitiativePlanner 创建 proactive Work。
- Drift Clock 在满足空闲策略时创建 drift Work。

Trigger 成功的定义是 Work 已持久化，而不是 Runtime 已完成。

### Work

Work 是跨链路共享的可恢复执行单元。建议字段：

```text
id
kind                 passive | scheduled | proactive | drift
action               agent | direct
dedupe_key           全局唯一
session_id
conversation_id
profile              agent action 必填
input_text
payload              非核心扩展数据
priority
status               pending | running | succeeded | failed | cancelled
available_at
lease_until
attempts / max_attempts
last_error
created_at / started_at / completed_at
```

Payload 在持久化层可以使用 JSONB，但进入 Application 后必须转换成强类型 command，不能让 Handler
直接读取任意 metadata 字典。

优先级建议：

```text
passive           100
scheduled direct   90
scheduled agent    70
proactive           40
drift               20
```

### Runtime

Runtime 继续只负责：

- 加载允许进入上下文的历史；
- 根据 Profile 调用模型和工具；
- 超时、步骤、Hook、usage 和 Turn/Tool 记录；
- 返回通用执行结果。

Runtime 不负责：

- 判断当前是否适合主动触达；
- 订阅或 ACK 消息源；
- 计算 schedule 下次运行时间；
- 选择 Channel；
- 直接发送；
- 修改 source/schedule 状态。

首版 initiative Planner 只在确定要发送时创建 agent Work，因此 Runtime 暂时仍可保持“成功必须产生
文本”的契约。等真实需求证明需要模型内 `reply/skip` 终止后，再引入强类型 CompletionPolicy。

### Schedule

Schedule 保留独立领域模型，因为时间 recurrence 与 Agent 执行没有共同语义。

```text
scheduled_jobs
  id / target conversation
  action: direct | agent
  schedule_kind: at | interval | cron
  timezone / next_run_at
  direct_text 或 agent_input/profile
  enabled / version

schedule_occurrences
  job_id / scheduled_for (unique)
  work_item_id
```

Scheduler 领取 due job 后，在同一事务中：

1. 插入唯一 occurrence；
2. 创建 Work；
3. 推进或关闭 job。

Direct Work 跳过 Runtime，但仍创建 assistant message 和 Outbox。

### Proactive Source

Proactive 是 source ingestion + initiative planning，不是独立 Runtime。

```text
SourcePort.poll(subscription, cursor) -> SourceBatch
source_subscriptions
source_items (subscription_id, external_id unique)
```

Source Poller 只持久化候选。InitiativePlanner 根据显式订阅、优先级、过期时间、冷却和会话状态选择
候选，再创建 `profile=proactive` 的 Work。外部 ACK 使用持久化 Effect，不使用 callback。

### Drift

Drift 与 proactive 共享 InitiativePlanner 的后半段：候选、Gate、Work、Runtime、Outbox。

区别：

- proactive 候选来自用户订阅的外部 SourceItem；
- drift 候选来自空闲机会、Memory/Skill，或未被即时推送但仍适合聊天的兴趣 SourceItem；
- drift 使用更严格的冷却和最低优先级；
- drift Profile 负责把候选组织成自然的聊天开场，而不是通知摘要。

Drift 不在 Proactive Pipeline 内执行。两者只共享 Candidate 和 EngagementPolicy。

### 会话与历史

必须区分：

```text
execution work/session     Runtime 审计和串行化
delivery conversation     最终消息发往哪里
conversation history      后续模型允许看到什么
```

规则：

- passive 用户消息进入历史。
- schedule/proactive/drift 的内部 input 只存在 Work/Turn，不伪装为 user message。
- 最终成功送达的 assistant 消息可以进入目标 conversation 历史。
- pending/failed assistant 永不进入历史。
- drift 未发送的内部执行只进入审计记录。

## Jasi 迁移阶段

### Phase 0：审计和约束

- 本文档和竞态测试清单。
- 保持线上 passive 行为不变。

### Phase 1：Durable Work 基础

- 新增 work_items、WorkRepositoryPort、WorkWorker、Handler Registry。
- Turn 改为通过 work item 幂等恢复，允许没有 inbound user message。
- 增加跨类型会话串行和优先级测试。

### Phase 2：Passive Cutover

- Telegram 入站事务改为：inbound event + user message + passive Work。
- Telegram 在 Work 持久化后即可确认 Update。
- Passive Handler 调 Runtime，并原子完成 Work + assistant + Outbox + inbound event。
- 删除 Service 内长时间持有的模型调用路径。

### Phase 3：Schedule

- PostgreSQL scheduled_jobs/occurrences。
- direct 与 agent 两种 action。
- 重启恢复、misfire、recurrence、并发领取和取消测试。

### Phase 4：Proactive Sources

- SourcePort、subscription/source item Repository。
- Source Poller 与 InitiativePlanner。
- 冷却、过期、去重、passive 优先和 Outbox 投递测试。

### Phase 5：Drift

- Drift opportunity producer。
- 与 proactive 共用 Candidate/Gate/Work Handler。
- drift Profile 和候选上下文；不复制 Runtime 循环。
- 空闲、冷却、竞争和“无发送不进历史”测试。

### Phase 6：Hardening

- Work lease heartbeat、stale reclaim、优雅停机。
- 后台模型并发配额和 passive 优先。
- Effect Outbox、指标和运维查询。
- 根据三种真实 Profile 再决定 Prompt/CompletionPolicy 抽象。

## 验收不变量

1. 代码中只有 OutboxWorker 可以调用用户消息 ChannelPort。
2. 同一 `dedupe_key` 最多创建一个 Work。
3. 同一 schedule occurrence 最多创建一个 Work。
4. 同一会话不会并发执行两个 Work。
5. passive 到达时，尚未开始的 proactive/drift 不得抢占。
6. Runtime 成功后、Outbox 创建前崩溃，重试不得再次请求模型。
7. schedule/proactive/drift 内部输入不进入用户聊天历史。
8. 只有 sent assistant 进入 Runtime 历史。
9. 进程重启后 pending Work 和 pending Outbox 都能恢复。
10. 新增 Source 或 Channel 不修改 AgentRuntime 主循环。
