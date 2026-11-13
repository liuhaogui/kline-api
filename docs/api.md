# API

默认根地址：`http://127.0.0.1:8082/trade/api/v1`。下面所有行情接口也可使用 `/api/v1` 根路径。没有登录或 API Key 参数。

## 返回格式

REST JSON 保留原约定：HTTP 200，业务状态放在 `code`。成功为 `0`，参数错误 `400`，上游失败 `500`。来源不允许返回业务码 `403`，未知接口 `404`。OPTIONS 预检返回 HTTP 204；WebSocket 正常握手为 HTTP 101，握手协议错误可能直接返回 HTTP 错误。

成功示例：

```json
{"code":0,"list":[{"time":1700000000,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10}],"session":""}
```

错误示例：

```json
{"code":500,"message":"获取K线失败"}
```

`time` 为 Unix **秒**，不是毫秒。`volume` 含义由上游品种决定。REST 不额外包装成 `data.list`。

## TradingView 历史 K 线

`GET /kline/tv/bars`

| 参数 | 必须 | 说明 |
| --- | --- | --- |
| `symbol` | 是 | 如 `TVC:USOIL`、`NASDAQ:AAPL`、`HKEX:1810`、`BINANCE:BTCUSDT` |
| `interval` | 是 | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1D`, `1W`, `1M` |
| `count` | 否 | 默认 320，最多 1000；上游可能返回较少数量；无效或非正整数沿用默认值 |
| `session` | 否 | 空、`regular`、`extended`；非空仅限上述美股交易所前缀 |

保持原服务兼容行为：未知周期会降级为日线；调用方应使用表内周期。`interval` 大小写敏感。接受简写 `BTCUSDT`、`sh600000`、`sz000001` 和 `hf_XAU` / `hf_XAG` / `hf_CL` / `hf_BRENT`，但推荐使用搜索结果中的完整交易所代码。

```sh
curl 'http://127.0.0.1:8082/trade/api/v1/kline/tv/bars?symbol=NASDAQ%3AAAPL&interval=1h&count=100&session=extended'
```

响应包括 `code`、`list` 和所请求的 `session`。时段仅透传至上游，不在本地虚构盘前/盘后数据。

## TradingView 交易对搜索

`GET /kline/tv/search?text=apple&limit=30`

`text` 为空返回空数组；`limit` 默认 30，为本地结果数量上限。过滤 warrant 和 structured 类型，去除上游高亮标签。

```json
{"code":0,"list":[{"symbol":"NASDAQ:AAPL","description":"Apple Inc.","exchange":"NASDAQ","type":"stock"}]}
```

## 多源历史 K 线

`GET /kline/bars?symbol=BTCUSDT&interval=1D&count=100`

参数 `symbol`、`interval`、`count` 及 bar 格式同上，不支持 `session`。响应没有 `session` 字段。

| 代码 | 数据源 | 示例和限制 |
| --- | --- | --- |
| 以 USDT 结尾（不含交易所前缀） | Binance | `BTCUSDT`；周期同上述列表 |
| `hf_` 开头 | BiQuote | 仅 `hf_XAU`、`hf_XAG`、`hf_CL`、`hf_BRENT` |
| 其他 | Tencent | `sh600000`、`sz000001`、`hk00700` 等腾讯代码；不能直接传 `NASDAQ:AAPL` 等 TradingView 原生代码 |

BiQuote 保留原降级逻辑：1m/3m→5m，2h→1h，6h/12h→4h，周/月→日。Tencent 提供日/周/月、1/5/15/30/60 分钟，并用小时数据聚合 2/4/6/12 小时；未知周期按原服务逻辑降级。使用指定周期前需确认数据源支持，返回数据不代表精确覆盖所有周期。

## 实时 WebSocket

连接二选一：

- `ws://127.0.0.1:8082/trade/api/v1/kline/tv/ws`，可添加 `?session=regular` 或 `?session=extended`。
- `ws://127.0.0.1:8082/trade/api/v1/kline/ws`，使用多源代码，不支持时段参数。

发送：

```json
{"action":"subscribe","symbol":"TVC:USOIL","interval":"1D"}
```

服务端消息（确认与初始 bar 的先后不应由客户端假定）：

```json
{"type":"subscribed","symbol":"TVC:USOIL","interval":"1D"}
{"type":"bar","symbol":"TVC:USOIL","interval":"1D","bar":{"time":1700000000,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10}}
{"type":"error","message":"订阅失败"}
```

非空 `session` 会附加到 TV 消息，用于客户端校验时段；不同 session 的上游订阅隔离。相同连接重复订阅同一代码/周期不重复创建上游。

取消订阅：

```json
{"action":"unsubscribe","symbol":"TVC:USOIL","interval":"1D"}
```

响应 `type=unsubscribed`。连接断开自动取消其全部订阅。上游初始化或永久品种错误、网络不通不保证都有独立错误消息；客户端应具备数据等待超时处理。

服务端每 30 秒发协议级 Ping，90 秒无读取/回应超时断开；浏览器原生 WebSocket 自动处理 Pong，其他客户端需要读取连接并回复 Pong。单消息最大 64 KiB，默认最多 256 个连接，每连接 32 个订阅；达到上限会返回可见错误。

浏览器示例：

```js
const socket = new WebSocket('ws://127.0.0.1:8082/trade/api/v1/kline/tv/ws')
socket.onopen = () => socket.send(JSON.stringify({
  action: 'subscribe', symbol: 'TVC:USOIL', interval: '1D',
}))
socket.onmessage = event => console.log(JSON.parse(event.data))
socket.onerror = error => console.error(error)
// 页面卸载时：socket.close()
```

## 存活检查

`GET /healthz`

```json
{"code":0,"status":"ok","version":"dev"}
```

不会向上游请求行情，适合容器和负载均衡存活检测。公网行情可用性需另查实际 bars 请求的 `code` 和非空 `list`。
