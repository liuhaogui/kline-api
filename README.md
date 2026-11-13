# Kline API

可自行构建和部署的独立 K 线行情代理，提供历史 OHLCV、实时 WebSocket 和交易对搜索。使用 Go，无数据库、账户系统、交易执行或消息机器人依赖。

默认 API 根路径为 `/trade/api/v1`，同时提供 `/api/v1` 别名，兼容现有 TV Chart 数据源。时间戳为 Unix 秒。

## 功能和接口

| 接口（相对 API 根路径） | 用途 | 数据来源 |
| --- | --- | --- |
| `GET /kline/tv/bars` | 历史 K 线 | TradingView |
| `GET /kline/tv/ws` | 实时 K 线 WebSocket | TradingView |
| `GET /kline/tv/search` | 搜索交易对，返回 `交易所:代码` | TradingView |
| `GET /kline/bars` | 多源历史 K 线 | Binance / Tencent / BiQuote |
| `GET /kline/ws` | 多源实时 K 线 WebSocket | 同上，部分数据源通过轮询更新 |

另外提供 `GET /healthz` 存活检查。完整参数、返回格式及订阅协议见 [API 文档](docs/api.md)。

## 源码启动

需要 Go 1.25.5 或更高版本，且部署机器能够连接上游数据源的 HTTPS / WebSocket 地址。

```sh
git clone https://github.com/liuhaogui/kline-api.git
cd kline-api
go mod download
go run ./cmd/kline-api
```

另开终端验证：

```sh
curl --fail http://127.0.0.1:8082/healthz
curl 'http://127.0.0.1:8082/trade/api/v1/kline/tv/bars?symbol=TVC%3AUSOIL&interval=1D&count=2'
```

历史接口的业务错误也使用 HTTP 200，务必检查 JSON 的 `code`，不能只依赖 `curl --fail`。

## Docker Compose

```sh
cp .env.example .env
docker compose up -d --build
docker compose logs -f --tail=100
curl --fail http://127.0.0.1:8082/healthz
```

默认仅映射宿主机 `127.0.0.1:8082`，通过 Nginx 对外提供服务。若需要直接访问服务器端口，可在 `.env` 设置 `BIND_ADDR=0.0.0.0`，并配置服务器防火墙。容器不需要持久化卷。

## 二进制和服务器部署

```sh
make build
./bin/kline-api -version
LISTEN_ADDR=127.0.0.1:8082 ./bin/kline-api
```

也可运行 `make release VERSION=v0.1.0`，生成 Linux amd64、arm64 的压缩包和 SHA256 校验文件。仓库推送 `v*` 标签后，Release 工作流会测试、构建并上传这些文件；源码推送本身不会创建 Release。

Linux systemd 安装、Nginx、HTTPS/WSS、健康检查和更新步骤见 [部署文档](docs/deployment.md)。示例配置在 [deploy](deploy)。

## 配置

配置全部通过环境变量提供，模板见 [.env.example](.env.example)。直接执行程序**不会自动读取 `.env`**；Docker Compose 自动读取它，systemd 使用自己的 `EnvironmentFile`。

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `LISTEN_ADDR` | `:8082` | HTTP 监听地址 |
| `API_PREFIX` | `/trade/api/v1` | API 根路径；另保留 `/api/v1` 别名 |
| `CORS_ORIGINS` | `*` | 允许的 Origin，多个值用逗号分隔；也检查 WS 握手的 Origin |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` 等 logrus 级别 |
| `REQUEST_TIMEOUT` | `12s` | 非 WebSocket 请求的总预算 |
| `SHUTDOWN_TIMEOUT` | `15s` | HTTP 优雅关闭等待时间 |
| `MAX_WS_CONNECTIONS` | `256` | 进程级 WebSocket 连接上限 |
| `MAX_WS_SUBSCRIPTIONS` | `32` | 单连接同时订阅数量上限 |

`BIND_ADDR`、`PORT`、`VERSION` 仅用于 Compose 的宿主机映射和镜像构建。Compose 固定容器内部监听 `:8082`；直接使用 Dockerfile 时，内置健康检查也以该端口为准。

本服务默认无登录和 API Key。CORS 是浏览器来源控制，不是身份认证；不限制没有 Origin 的命令行客户端。

## 数据源和能力边界

- TradingView 使用网页内部接口，未使用用户 Cookie、私人 Token 或登录账户，不是官方授权的数据产品；接口变动、上游限流、地区网络和品种权限可能导致失败或延迟。
- 美股支持 `session=regular` 和 `session=extended`，仅接受 `NASDAQ`、`NYSE`、`AMEX`、`NYSEARCA` 前缀；不提供 `overnight` 参数，也不承诺全量历史回放或 5×23 数据覆盖。
- 多源接口按代码路由：`*USDT` → Binance，`hf_*` → BiQuote，其余 → Tencent。具体代码、周期和降级行为见 API 文档。
- 实时同源同代码同周期的订阅复用一条上游连接；普通取消订阅延迟 10 秒回收，进程关闭会立即取消。TradingView 历史请求最多 8 个并发，总预算 8 秒、最多 2 次尝试；实时断线退避重连。
- MIT 许可适用于本仓库代码，不包含第三方行情数据的使用许可。使用上游数据时应遵守对应提供方条款。

## 开发与验证

```sh
go mod verify
make check
make test
```

单元和集成测试使用本地模拟上游，覆盖历史响应、时段、订阅、共享连接、取消、重连与关闭，不依赖公网行情服务。`/healthz` 只检查本服务存活，不能证明上游行情可用。

项目提取范围与发布验证记录见 [项目说明](docs/project.md)。

## License

[MIT](LICENSE)
