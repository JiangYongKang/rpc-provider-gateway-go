# rpc-provider-gateway-go

一个本地可运行的 **JSON-RPC 2.0 网关**：把请求分发到多个后端 provider，在节点
超时、出错、不可用时自动故障转移，并对非幂等方法保证“最多投递一次”；同时提供
并发/QPS 配额、统一错误码与按参数的只读结果缓存。

- 无第三方依赖，仅用 Go 标准库。
- JSON-RPC 2.0：支持单请求、批量请求、通知（notification）。
- 每个请求与响应严格按 `id` 一一对应，批量与并发下都不会串包。

---

## 1. 构建与运行

```bash
go build ./...
go run ./cmd/gateway -config config.example.json
# 或
go build -o gateway ./cmd/gateway && ./gateway -config config.example.json
```

启动后监听 `http://127.0.0.1:8080/`，仅接受 `POST` 且 `Content-Type: application/json`。

```bash
curl -s -H 'Content-Type: application/json' -X POST localhost:8080 \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}'
```

---

## 2. 处理流程与路由策略

单个请求的处理顺序（顺序很重要，决定了错误码的优先级）：

1. **协议解析**：JSON 语法错误 → `-32700`；不是合法 JSON-RPC 2.0 对象 → `-32600`。
2. **方法路由**：只有在配置 `methods` 中显式登记的方法才会被转发；
   未登记的方法直接返回 `-32601 Method not found`，**不会触碰任何 provider**。
   这样“方法不支持”“provider 全部不可用”“被限流”三种情况从错误码上严格区分。
3. **配额控制**：先过 QPS 令牌桶，再占用并发名额。任一不足 → `-32004`，
   立即拒绝（不排队），同样不触碰 provider。
4. **缓存**：只读且可缓存的方法先查缓存（命中直接返回，不回源）。
5. **选 provider 与故障转移**：见下。

### 2.1 Provider 选择顺序与健康冷却

- provider 按配置文件中的**顺序作为优先级**依次尝试（`weight` 字段当前预留）。
- 请求开始时，节点被分为“健康”和“冷却中”两组：健康节点在前、冷却节点兜底在后。
- 一次调用失败（可重试类故障）会让该节点进入 `health_cooldown` 冷却，
  冷却期内的新请求优先选择其它健康节点，避免反复打到故障节点。
- **所有节点都冷却时不会静默丢请求**：只读请求仍会按顺序尝试冷却节点；
  非幂等请求则直接拒绝（见 2.3），绝不试探。
- 一次调用成功会清除该节点的冷却标记。

### 2.2 只读（幂等）方法：自动故障转移

配置 `read_only: true` 的方法（如 `eth_blockNumber`、`eth_getBalance`、`eth_call`）
按选择顺序逐个尝试，直到成功或耗尽全部 provider，对外只返回**一次**最终结果。

是否切换到下一个 provider，取决于**归一化后的故障类别**：

| 上游故障 | 是否故障转移 |
| --- | --- |
| 传输层错误（连接拒绝/重置/EOF/DNS/非法响应体） | 是 |
| 超时（`context deadline exceeded`、HTTP client 超时） | 是 |
| HTTP `408` / `429` / `5xx` | 是 |
| HTTP 其它非 2xx（如 `400`） | 否 |
| 正常收到的 **JSON-RPC error 对象** | 否（确定性业务/协议应答，直接透传） |

设计依据：JSON-RPC error 是后端成功收到请求后给出的确定性应答，换节点重试没有
意义且可能造成不一致；而连接错误/超时意味着请求很可能没被处理，切换是安全的。

### 2.3 非幂等方法：最多投递一次（绝不因切换重复执行）

未声明 `read_only` 的方法（如 `eth_sendRawTransaction`）一律视为非幂等：

- **只选择一个健康节点发送一次**，成功就返回结果。
- 唯一一次调用无论是传输错误、超时、HTTP 状态还是 JSON-RPC error，
  **都不会切换到其它 provider 重试**，统一返回 `-32003`，
  并在 `data` 中如实给出真实故障类别（`kind`）、`http_status`、
  `upstream_code` 等，调用方据此判断是否可以安全地自己重发。
- 如果此刻没有任何健康节点，网关**直接拒绝且完全不投递**（同样 `-32003`，
  message 含 “not attempted”），避免“可能已发、可能没发”的不确定状态。

因此对任何非幂等方法，调用方都能确定：**请求要么发了恰好一次，要么一次都没发**。

---

## 3. 错误码映射规则

网关把上游的各种错误归一为统一错误类型 `gateway.Error`，对外暴露**稳定错误码**。

| 错误码 | 含义 | 触发情形 |
| --- | --- | --- |
| `-32700` | Parse error | 请求体不是合法 JSON |
| `-32600` | Invalid Request | 不是 JSON-RPC 2.0 对象 / 缺 method / id 类型非法 / 空批量 |
| `-32601` | Method not found | 方法未在 `methods` 中登记（不转发） |
| `-32603` | Internal error | 网关内部错误 |
| `-32000` | Upstream JSON-RPC error | 上游返回 JSON-RPC `error` 对象；`data.upstream_code` 保留其原始码 |
| `-32001` | No provider available | 没有任何 provider，或只读请求尝试完所有节点仍失败（非全超时） |
| `-32002` | Upstream timeout | 所有尝试的 provider 都超时 |
| `-32003` | Non-idempotent failure | 非幂等方法唯一一次调用失败 / 无健康节点未投递；拒绝重试 |
| `-32004` | Rate limited | 超过 QPS 或最大并发配额 |
| `-32005` | Gateway error | 配置错误等网关自身问题 |

要点：

- `-32601`（方法不支持）、`-32004`（被限流）、`-32001/-32002/-32003`（上游问题）
  三类互不相同，调用方可以据代码做不同决策。
- 错误详情放在 JSON-RPC `error.data` 中，常见字段：
  - `provider`：发生故障的节点名；
  - `kind`：`transport` / `timeout` / `http_status` / `rpc_error`；
  - `http_status`：HTTP 状态码（适用时）；
  - `upstream_code` / `upstream_message` / `upstream_data`：上游 JSON-RPC error 原文；
  - `attempts`：只读请求在多个节点上的尝试明细；
  - `reason`：限流时为 `qps_exceeded` 或 `max_concurrent_exceeded`；
  - `note`：非幂等失败时固定提示“最多发送一次、网关未重试”。

HTTP 状态码仅表达传输层语义：正常处理一律 `200`（错误体现在 JSON 中）、
通知返回 `204`、解析/非法请求 `400`、非 POST `405`、错误 Content-Type `415`。

---

## 4. 缓存：哪些方法可缓存

- 只有 `read_only: true` **且** `cacheable: true` **且** `ttl > 0` 的方法才缓存
  （配置中若给非只读方法开启缓存会在启动校验时直接报错）。
- 缓存键 = `方法名 + 规范化后的参数`：对象参数的键顺序不同也会命中同一结果；
  不同方法、不同参数互不共享。
- **只缓存成功结果**：错误、超时、限流等绝不写缓存。
- 到期（`ttl`）后下次请求自动回源并刷新；过期项在读取时被惰性清理。
- 即使命中缓存，响应 `id` 仍然使用当前请求自己的 `id`。

| 方法类型 | 可故障转移（重试） | 可缓存 |
| --- | --- | --- |
| 只读 `read_only:true` | 是（仅对可重试类故障） | 仅当 `cacheable:true` 且有 `ttl` |
| 非只读（默认，如发交易） | **否，最多发一次** | 否 |

---

## 5. 配额与限流

- `qps`：令牌桶，容量等于 QPS（允许等量瞬时突发），按秒补充；
  令牌不足的请求**立即**得到 `-32004`（`reason=qps_exceeded`），不等待。
- `max_concurrent`：在途请求并发上限（信号量）；占满时新请求立即得到
  `-32004`（`reason=max_concurrent_exceeded`）。请求结束释放名额。
- 两者都设为 `0` 表示不限制。
- 被限流的请求不会被静默丢弃：要么成功，要么收到明确的错误响应。

---

## 6. 配置项

见 `config.example.json`。

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `listen_addr` | 监听地址 | `:8080` |
| `path` | HTTP 路径 | `/` |
| `default_timeout` | 单次上游调用超时（如 `"2s"`） | `10s` |
| `max_concurrent` | 最大并发在途请求，`0` 不限 | 无限制 |
| `qps` | 每秒请求数，`0` 不限 | 无限制 |
| `providers[].name` | 节点唯一名（日志/错误详情使用） | 必填 |
| `providers[].url` | 节点 HTTP JSON-RPC 地址 | 必填 |
| `providers[].timeout` | 该节点专属单次调用超时 | 取 `default_timeout` |
| `providers[].health_cooldown` | 失败后健康冷却时长（如 `"2s"`） | 不冷却 |
| `providers[].weight` | 预留字段 | - |
| `methods.<name>.read_only` | 是否只读（可故障转移） | `false` |
| `methods.<name>.cacheable` | 是否缓存（要求只读） | `false` |
| `methods.<name>.ttl` | 缓存有效期（如 `"500ms"`） | 不缓存 |

时长字段统一用字符串：`"500ms"`、`"2s"`、`"1m"`。

---

## 7. 日志：请求 ID、选中的 provider、判定依据

每个判定点都会打印一行结构化日志，便于核对“为什么选这个节点、为什么失败/切换”：

```
req_id=10 method=eth_getBalance decision=failover_attempt provider=primary attempt=1
req_id=10 method=eth_getBalance provider=primary decision=failed kind=http_status retryable=true
req_id=10 method=eth_getBalance decision=failover_attempt provider=backup attempt=2
req_id=10 method=eth_getBalance provider=backup decision=success attempt=2
req_id=13 method=eth_sendRawTransaction decision=single_attempt provider=primary reason=non_idempotent
req_id=13 method=eth_sendRawTransaction provider=primary decision=fail_once reason=http_status retry=refused
req_id=3  method=eth_frobnicate decision=reject reason=method_not_supported
req_id=5  method=eth_blockNumber decision=reject reason=quota_exceeded ...
```

单元测试通过 `testing.T.Logf` 输出同样内容，`go test -v` 时可见每个请求的 id、
provider 与判定依据。

---

## 8. 本地验证

### 8.1 单元测试（内置本地可控假 provider，无需外部依赖）

```bash
go test ./...                 # 全部测试
go test -race ./...           # 带竞态检测
go test -v -run TestFailover  # 仅看故障转移
go test -v -run TestNonIdempotent
go test -v -run 'TestQuota|TestServe_Concurrent'
go test -v -run TestCache
go test -v -run TestE2E       # 真实 HTTP 链路（httptest 假上游）
```

覆盖的场景：

- 故障转移：传输错误 / 超时 / HTTP 503 切换；HTTP 400、JSON-RPC error 不切换；
  全部失败 `-32001`、全部超时 `-32002`；健康冷却与恢复。
- 非幂等：成功只发一次；传输/超时/503/JSON-RPC error 均只发一次并回 `-32003`；
  全部冷却时不投递直接拒绝。
- 并发与限流：QPS、并发上限拒绝；80 个不同 id 并发下请求/响应严格对应不串包；
  被拒请求有明确 `-32004`，无静默丢弃。
- 缓存：命中回源一次、参数隔离、TTL 过期失效、错误不缓存、对象键序规范化。
- 协议：批量 id 对应、通知 `204`、解析错误、非法请求、方法不支持。

### 8.2 用脚本起本地假上游做端到端验证

仓库自带一个零依赖的 Python 假上游 `scripts/fake_upstream.py`：

```bash
# 终端 A / B：两个 provider
python3 scripts/fake_upstream.py 19101 primary ok
python3 scripts/fake_upstream.py 19102 backup  ok
# 终端 C：网关（config.example.json 默认指向 19101/19102）
go run ./cmd/gateway -config config.example.json
# 终端 D：发请求
curl -s -H 'Content-Type: application/json' -X POST localhost:8080 \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}'
```

第三个参数控制行为，用于制造异常：

- `ok`：正常返回；
- `http503`：固定回 HTTP 503（只读请求会切到 backup，发交易只发一次回 `-32003`）；
- `timeout`：挂起不响应（触发超时，只读请求切换）；
- `rpcerr`：返回 JSON-RPC error 对象（确定性错误，不切换，对外 `-32000`）。

假上游会打印每次收到的 `req_id / method / 调用次数`，其中
`eth_sendRawTransaction` 单独计数，可直接核对非幂等方法到底被投递了几次。
