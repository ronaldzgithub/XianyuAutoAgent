# Foundry—Huaxiaobao 接入说明

## 基线与边界

- 上游：`shaxiu/XianyuAutoAgent`
- 基线提交：`540bbc26cf02ee6348d997843942776a9be9460b`
- 改造分支：`codex/foundry-huaxiaobao-integration`
- 许可证：GPL-3.0。README 同时写有“仅供学习与交流”及可能停止维护的提示，商业使用和平台准入必须单独复核，不能仅凭存在 LICENSE 判定。

Huaxiaobao 是闲鱼连接、账号/Cookie、登录和风控 HITL、能力审核、执行权限与工具回执的唯一所有者。Foundry 不复制 Cookie，不重建闲鱼 WebSocket/API 执行器。

Foundry 拥有服务包、线索/商机、供应能力与成本、报价边界、客户订单、外部动作批准、独立验收和会计裁决。闲鱼消息、工具发送成功或平台订单状态不能直接成为客户验收、收入或净值证据。

## 能力拆分与副作用

当前上游把接收、意图识别、LLM 回复生成、议价和发送放在同一进程。接入时必须拆成独立能力：

| 能力 | 副作用 | 接入约束 |
| --- | --- | --- |
| 咨询读取 | 读取及协议 ACK | 返回稳定消息 ID；不得把协议 ACK 当作业务结果 ACK |
| 回复草稿生成 | 内部计算 | 默认只生成草稿，不发送；内容是不可信候选 |
| 报价建议 | 内部计算 | 必须受 Foundry 的成本、底价、期限、验收、转包/AI 条件约束 |
| 回复发送 | 外部客户消息 | 单独能力，要求有效批准、幂等 operation ID 和结果验证 |
| 人工接管 | 暂停自动化 | 状态必须持久化并绑定会话/目标；重启后保持暂停 |
| 订单状态读取 | 读取 | 仅作为候选事实，需后续验收和 Accounting receipt |

本 Fork 新增 `ENABLE_AI_AUTO_SEND` 安全开关。默认、空值及未知值均关闭 AI 直发；仅 `true`、`1`、`yes` 或 `on` 显式开启。该开关只是兼容旧版直发行为，不是 Foundry 批准，生产环境仍应通过 Huaxiaobao 的独立发送能力。

## 安全 adapter surface

`python xianyu_adapter.py describe` 输出机器可读能力描述；`python xianyu_adapter.py execute` 从标准输入读取 `foundry.huaxiaobao.tool-request.v1` JSON，并返回 `foundry.huaxiaobao.tool-result.v1`。当前仅提供：

- `account.status`：离线判断凭据是否配置；即使已配置也返回 `UNKNOWN`，不会把“存在 Cookie”冒充登录有效；
- `inquiry.read`：只接受已由本进程原生监听器原子写入 journal 的咨询，以账号、会话、message ref 和 message revision 做精确复验；事件缺失返回 `UNKNOWN`，同 identity 不同内容 fail closed；
- `reply.draft.generate`：调用原生 `XianyuReplyBot.generate_reply`，只产生草稿；上游默认模型端点属于外部模型数据传输，调用前仍需核验客户与平台的 AI/数据条件；
- `quote.constrain`：只对 Foundry 冻结的服务包、供应核验、验收引用、七类成本、产能/期限和 AI/转包条件做确定性边界检查；它不授予商业批准、不创建订单；
- `reply.send`：固定返回并持久化 `PAUSED / EXTERNAL_ACTION_APPROVAL_REQUIRED`，adapter 不导入或调用 WebSocket 发送路径；
- `operation.query`：按原 operation ID 查询持久结果，支持服务重启后的 UNKNOWN/PAUSED 恢复判断。

默认 SQLite journal 是 `data/huaxiaobao_adapter.db`，可用 `XIANYU_ADAPTER_STATE_PATH` 指向 Huaxiaobao 管理的持久卷。原生监听器与 adapter 共用此 journal：入站咨询在模型调用前写入，同一账号/会话/message ref/revision 的重复事件直接跳过，不同内容冲突停止自动处理；人工接管状态也写入同库，只有显式操作才能恢复自动模式，进程重启不会清除暂停。同一 operation ID 与同一内容重放返回缓存结果；同 ID 不同内容 fail closed。请求内禁止携带 Cookie、Token、密码等凭据材料。该 journal 是工具执行状态，不是商业账本或人工验收记录。

部署时应由 Huaxiaobao 设置 `XIANYU_ACCOUNT_REF` 为不透明账号引用。未设置时，原生进程只在工具边界内从当前原生账号 ID 生成不可逆引用；该 fallback 不能替代 Huaxiaobao 对账号和租户的正式绑定。

## 账号与人工入口

Cookie 缺失、过期、滑块或风控应生成持久化账号所有者/工具管理员待办：

- 账号所有者在 Huaxiaobao 管理的受限现场完成本人或组织账号登录；
- 工具管理员处理连接配置和能力激活；
- 普通工作人员只能处理获准的咨询接管或资料工作，不能取得 Cookie、账号所有权、管理员权或批准权；
- 完成后适配器必须重新验证账号、租户、权限和阻塞动作，再产生绑定原任务的 typed outcome。

禁止把完整 Cookie、Token、密码、可复用会话链接或风控现场写入任务正文、普通日志、提交或 Foundry ledger。当前上游的终端粘贴 Cookie 路径只能保留在隔离的 Huaxiaobao 管理现场，不能暴露给普通工作人员。

## ACK 与恢复

WebSocket `code: 200` ACK 只是协议接收确认。生产 adapter 仍需实现：

- 入站消息 ID/revision 已在原生 journal 唯一约束；出站 operation ID 仍须由独立发送 adapter 实现；
- 发送前批准有效性与任务版本检查；
- 发送后平台可复查结果，而不是只信 `websocket.send`；
- 人工结果完成、取消、失败、超时、重复和迟到处理；
- 原组件显式 ACK 消费 typed outcome 后从原检查点恢复；
- 等待人工时停止自动回复、持续重试和预算消耗。

## 部署与升级

VolvenceDeploy 负责获批服务的固定版本部署、SQLite 持久化、健康、升级和恢复。不得直接使用浮动 `latest` 作为可审查生产版本，也不得擅自重启或替换现有 Foundry/Huaxiaobao 实例。

升级时记录 upstream commit、Fork commit、Python/依赖版本和镜像 digest；在隔离环境运行单元测试、重复消息、重启恢复、权限撤销和无外发检查后，再通过独立发布门。

## 当前验收状态

- 源码基线和安全默认：已记录并有离线单元测试。
- 本地服务/容器：未运行。
- Foundry—Huaxiaobao 能力/回执合同：已增加草稿、账号离线状态、原生咨询复验、持久暂停与 operation 查询的最小 adapter；独立获批发送仍未实现。
- 真实账号、平台准入和获批发送：未验证。
- 持久化人工任务、工具复查和业务 ACK：无真实证据；原生咨询 journal、暂停重启、adapter 查询、重复结果与幂等冲突仅有离线单元测试证据。

不得把单元测试、SQLite 历史或 WebSocket 连通性声称为真实咨询、订单、客户验收或收入。
