# JSON-RPC Provider Gateway (Go)

一个本地可运行的 JSON-RPC 2.0 网关：把请求路由到多个后端 provider，在 provider
超时 / 出错 / 不可用时自动故障转移，统一错误码，支持并发配额与只读结果缓存。
**非幂等（发交易类）方法只发送一次，绝不因故障转移而重复执行。**

仅依赖 Go 标准库，无第三方依赖。

## 架构

```
HTTP client
   │  JSON-RPC 2.0 (single / batch)
   ▼
internal/gateway   解析、批处理、配额、方法白名单、缓存、逐请求决策日志
   ├── internal/ratelimit  最大并发槽位 + 每秒令牌桶（超限显式拒绝）
   ├── internal/cache       只读方法 method+params TTL 缓存
   └── internal/router      按序选择 provider + 故障转移（非幂等单发）
          └── provider.Provider
                 ├── internal/provider/http  真实 HTTP JSON-RPC
                 └── internal/provider/fake  测试用可控假 provider
internal/jsonrpc   2.0 编解码（id 原样保留，绝不串包）
internal/gwerrors  统一错误类型 + 稳定错误码 + 上游错误分类
internal/config    JSON 配置（时长支持 "3s" 这种字符串）
```

## 路由与故障转移策略

- Provider 按配置中的顺序组成故障转移链；被标记为 `healthy:false` 的
  provider 在发请求前直接跳过。
- **只读 / 幂等方法**（`idempotent_methods`）：第一个可用 provider 失败后，
  自动尝试下一个，直到成功或全部失败；对外只返回一次最终结果。以下情况都
  会触发切换：传输层错误、非 2xx HTTP 状态、JSON-RPC `error` 对象、
  超时、上游返回非法响应。
- **非幂等方法**（`non_retryable_methods`，如 `eth_sendRawTransaction`）：
  只选择第一个可用 provider 并**发送且仅发送一次**。
  - 成功：如实返回结果；
  - 失败：**不切换、不重试**，返回错误码 `-32008 retry_not_allowed`，并在
    `data.upstream` 中保留那一次尝试的真实上游错误。调用方因此能确切知道
    这笔交易只发过一次，不会出现"到底发了几次"的歧义。
- 每个 provider 可通过 `methods` 限定只服务某些方法；网关还维护一个方法
  白名单（幂等 ∪ 非幂等 ∪ 可缓存方法的并集），不在白名单的方法直接返回
  `-32601`，不会把请求打到任何后端。

每次选择都会写决策日志，格式：
`[id=<请求ID> method=<方法> provider=<名称>] <判定依据>`，
例如 `failover: previous provider failed, trying next available provider -> success`。

## 统一错误码映射

所有上游错误先归一为 `gwerrors.GatewayError`，再转成 JSON-RPC `error` 对象；
`data.category` 给出机器可读类别，`data.provider` 标明来源 provider，
`data.http_status` / `data.upstream` 保留细节。

| 错误码  | 常量 | 触发场景 |
|--------|------|---------|
| -32700 | `parse_error` | 请求体不是合法 JSON / 空批次 / 非 2.0 请求 |
| -32600 | `invalid_request` | 批次超过 `max_batch_size` 等 |
| -32601 | `method_not_supported` | 方法不在网关白名单（明确拒绝，非静默丢弃） |
| -32602 | `invalid_params` | 参数非法（保留） |
| -32603 | `internal_error` | 其它未分类内部错误 |
| -32001 | `rate_limited` | 超过最大并发或每秒配额；`data` 可区分原因 |
| -32002 | `all_providers_down` | 所有 provider 不可用，或全部尝试后仍失败 |
| -32003 | `upstream_timeout` | 上游在超时时间内未响应（或 `context deadline exceeded`） |
| -32004 | `upstream_transport` | 连接被拒 / DNS / 连接重置等传输错误 |
| -32005 | `upstream_http_status` | 上游返回非 2xx（`data.http_status` 带状态码） |
| -32006 | `upstream_jsonrpc_error` | 上游 2xx 但返回 JSON-RPC `error` 对象（原文在 `data.upstream`） |
| -32007 | `upstream_invalid_response` | 上游响应不是合法 JSON-RPC（id 不符 / 缺字段等） |
| -32008 | `retry_not_allowed` | 非幂等方法单次尝试失败，按策略不重试 |

限流被拒（-32001）、provider 全不可用（-32002）、方法不支持（-32601）
使用互不相同的错误码，调用方可以稳定区分。

## 可缓存 / 可重试分类（默认配置）

- **可缓存（只读、幂等）**：`eth_getBalance`、`eth_blockNumber`、
  `eth_getCode`、`eth_chainId`、`net_version`、`web3_clientVersion`。
  成功结果按 `method + 规范化 params` 缓存，TTL（默认 `2s`）到期失效；
  发交易类方法永远不缓存。
- **可故障转移重试（幂等）**：`eth_call`、`eth_estimateGas`、
  `eth_getTransactionByHash/Receipt`、`eth_getBlockBy*`、
  `eth_getTransactionCount`、`eth_gasPrice` 等。
- **不重试（非幂等，单发）**：`eth_sendRawTransaction`、
  `eth_sendTransaction`、`personal_sendTransaction`、`eth_sign`、
  `personal_sign`。

以上均可在配置文件中覆盖。

## 并发与配额

- 批处理中的每个请求并发处理，响应按请求位置对齐，**请求 ID 与响应
  一一对应**；通知（无 `id`）不产生响应，全通知批次返回 HTTP 204。
- `max_concurrent`：全局限在途调用数，槽位满立即拒绝（非阻塞排队）。
- `requests_per_second`：令牌桶，桶满后按经过时间补充，超出立即拒绝。
  两类拒绝都返回 -32001，错误文案区分 `max_concurrent` /
  `requests_per_second`。

## 配置项

见 `configs/config.example.json`。

| 字段 | 说明 |
|------|------|
| `listen_addr` | 监听地址，默认 `:8080` |
| `default_timeout` | 单次上游调用超时，支持 `"500ms"` / `"3s"` |
| `max_batch_size` | 单个批次最大请求数，默认 100 |
| `max_concurrent` | 全局最大在途调用数，0 表示不限 |
| `requests_per_second` | 每秒配额，0 表示不限 |
| `providers[].name` | 唯一名称，用于日志和错误 `data.provider` |
| `providers[].url` | JSON-RPC HTTP 端点 |
| `providers[].timeout` | 覆盖默认超时 |
| `providers[].healthy` | 初始可用性 |
| `providers[].methods` | 该 provider 服务的方法白名单；空表示全部 |
| `idempotent_methods` | 允许故障转移重试的方法 |
| `non_retryable_methods` | 只发送一次、失败不重试的方法 |
| `cache.enabled` / `cache.ttl` / `cache.methods` | 缓存开关、TTL、可缓存方法 |

## 本地验证

需要 Go 1.25+（在 1.26 上开发验证）。

### 1. 零依赖 demo（内置本地假 provider）

```bash
go run ./cmd/gateway -demo -addr :18099
```

demo 内置三个本地 provider：A 对只读方法返回 JSON-RPC 错误、B 超时、
C 正常。另开终端：

```bash
# 只读：A 失败 -> B 超时 -> C 成功（自动故障转移，只返回一次结果）
curl -s -X POST localhost:18099 -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}'

# 发交易：只发给第一个 provider 一次（返回它的结果，绝不切换）
curl -s -X POST localhost:18099 -d '{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","params":["0xRAW"]}'

# 不支持的方法 -> -32601
curl -s -X POST localhost:18099 -d '{"jsonrpc":"2.0","id":3,"method":"foo_bar"}'

# 批次（含通知）
curl -s -X POST localhost:18099 -d '[{"jsonrpc":"2.0","id":"x","method":"eth_chainId"},{"jsonrpc":"2.0","method":"ev_note"}]'
```

服务端日志会逐条打印 `id / method / provider / 判定依据`。

### 2. 真实 provider 配置

```bash
cp configs/config.example.json configs/config.json   # 改成你的节点 URL
go run ./cmd/gateway -config configs/config.json
```

### 3. 运行测试

```bash
go test ./...                  # 全部单测
go test -race ./...            # 带竞态检测
go test -v ./internal/router   # 故障转移 / 非幂等等
go test -v ./internal/gateway  # 端到端：并发 ID、限流、缓存失效、单发
```

测试使用 `internal/provider/fake` 中完全可控的假 provider（可脚本化
成功 / JSON-RPC 错误 / 传输错误 / HTTP 状态 / 超时 / 非法响应、计数每个
方法实际被调用次数、切换可用性），覆盖：

- 各故障类别的自动故障转移，且每个 provider 只被调用一次；
- 非幂等方法成功/失败都只发一次，失败返回 -32008 且不落到后续 provider；
- 50 路并发下请求 ID 不串包；
- 并发配额耗尽时明确返回 -32001，可与全不可用 -32002、不支持 -32601 区分；
- 缓存命中不产生上游调用，TTL 到期后重新回源；
- 真实 HTTP provider（httptest）对 500 / 超时 / 坏 JSON / id 不匹配的分类。
