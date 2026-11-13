# 部署

## 1. 前置条件

- Linux amd64 / arm64，或能运行 Go 的其他系统。
- 源码构建需要 Go 1.25.5+；使用容器需要 Docker Engine 与 Compose 插件。
- 服务器可以出站访问 TradingView 的 `data.tradingview.com:443`、`symbol-search.tradingview.com:443`，以及启用数据源对应的 Binance / Tencent / BiQuote 地址。Binance 实时连接使用 `stream.binance.com:9443`。
- 项目不连接原部署服务器，不需要复制数据库、生产配置、账户、私钥或 Cookie。

## 2. Docker

```sh
git clone https://github.com/liuhaogui/kline-api.git
cd kline-api
cp .env.example .env
docker compose config --quiet
docker compose up -d --build --wait
curl --fail http://127.0.0.1:8082/healthz
```

默认仅本机访问 8082，见后面的 Nginx 配置。修改 `.env` 后执行 `docker compose up -d` 重新创建服务；拉取新源码后执行 `docker compose up -d --build --wait`。查看日志用 `docker compose logs --tail=100 -f`；停止用 `docker compose down`。

## 3. Linux 二进制 + systemd

先执行 `make release VERSION=v0.1.0`，或下载对应 CPU 架构的 Release 压缩包，核对附带 SHA256。以下以解压后当前目录有 `kline-api` 为例；从源码 `make build` 则将安装源改为 `bin/kline-api`。

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin kline-api
sudo install -d -o root -g root /opt/kline-api
sudo install -m 0755 kline-api /opt/kline-api/kline-api
sudo install -m 0644 deploy/systemd/kline-api.service /etc/systemd/system/kline-api.service
sudo install -m 0600 .env.example /etc/kline-api.env
```

将 `/etc/kline-api.env` 的 `LISTEN_ADDR` 改为 `127.0.0.1:8082`，其余按需调整。`useradd` 仅首次安装执行；已有对应用户时跳过。

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now kline-api
sudo systemctl status kline-api
sudo journalctl -u kline-api -f
curl --fail http://127.0.0.1:8082/healthz
```

更新时，先把新程序安装到 `/opt/kline-api/kline-api.new`，保留旧程序备份，再 `mv` 原子替换并 `systemctl restart kline-api`。重新检查 `/healthz` 和实际行情请求；失败时恢复备份并重启。不要直接覆盖正在执行的二进制。

## 4. Nginx 与路径

示例 [kline-api.conf](../deploy/nginx/kline-api.conf) 放在 Nginx **http 上下文**中的 `conf.d`，不是放进另一个 `server` 块。

```sh
sudo cp deploy/nginx/kline-api.conf /etc/nginx/conf.d/kline-api.conf
# 编辑 server_name 为自己的域名；确认 80 端口没有冲突的现有站点。
sudo nginx -t
sudo systemctl reload nginx
```

代理保留完整 URI，服务直接接收 `/trade/api/v1/kline/...`；不要用带 URI `/` 的 `proxy_pass` 把 `/trade/` 去掉。`/api/v1/kline/...` 别名也支持。自定义 `API_PREFIX` 时需同步调整 location。

配置包含 `Upgrade` / `Connection`、HTTP/1.1 和 120 秒代理超时，避免 WebSocket 退化成轮询。`map` 只定义一次；如果在现有站点合并配置，不要重复定义同名变量。

公网 HTTPS 站点使用自己的证书配置 `listen 443 ssl` 和证书路径，并将客户端地址改成 `https://你的域名/trade/api/v1`，WebSocket 使用 `wss://`。仓库不附带证书、私钥或自动申请证书脚本。

## 5. 前端配置

在支持手动配置 K 线 API 的客户端中添加：

```text
https://你的域名/trade/api/v1
```

Chrome 扩展首次访问新主机时需批准浏览器权限。前端不能把 `http://` 地址的服务当作 `https://` 使用；HTTPS 网页应配套 HTTPS/WSS 服务。服务端 `CORS_ORIGINS` 可设为 `*`，或逗号分隔的真实前端 Origin。

## 6. 上线验证与故障定位

```sh
curl --fail https://你的域名/healthz
curl 'https://你的域名/trade/api/v1/kline/tv/bars?symbol=TVC%3AUSOIL&interval=1D&count=2'
curl 'https://你的域名/trade/api/v1/kline/tv/search?text=AAPL&limit=5'
```

验收实际 bars 应同时检查 `code=0` 和 `list` 非空。WS 可使用 API 文档的浏览器示例，确认收到 `subscribed` 及 `bar`；代理只返回 HTTP 200 不是成功升级。

| 现象 | 检查 |
| --- | --- |
| 健康检查正常但 bars `code=500` | 容器/服务日志与上游网络、限流、品种权限 |
| 历史正常、实时失败 | Nginx Upgrade、TLS、WS Origin、连接/订阅上限 |
| 页面仍请求旧地址 | 客户端当前 API 地址、扩展主机授权及页面刷新 |
| 来源未获允许 | `CORS_ORIGINS` 是否与客户端 Origin 完全一致 |
| 拒绝连接 | 监听地址、端口映射、防火墙、进程是否启动 |
| `session` 参数错误 | 仅 regular/extended，且必须是支持的美股交易所完整代码 |

服务不内置账户或鉴权。需要限制公开访问时，在自己的网关实施访问控制；不要将 CORS 当作认证。根据实际服务器资源调整连接上限与入口流量。日志写标准输出/journal，不会写入应用目录。
