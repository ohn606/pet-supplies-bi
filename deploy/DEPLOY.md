# 云服务器部署 Runbook

> 目标主机：阿里云轻量应用服务器 **<SERVER_IP>**
> 对外端口：**9888**（80 = 个人作品集，8080 = 饥荒管理平台，均已被占用）
> 部署包由 `python deploy/package.py` 生成：`dist/pet-supplies-bi-deploy.tar.gz`（约 157MB）

## 0. 端口分工

| 端口 | 角色 | 说明 |
|---|---|---|
| 9888 | Nginx（对外） | 公网访问入口，**必须在阿里云控制台防火墙放行 TCP** |
| 9889 | Python 看板服务（内部） | 仅监听 127.0.0.1，外网不可达 |
| 80 | 既有个人作品集 | **不改动** |
| 8080 | 既有饥荒管理平台 | **不改动** |

若主机无 Nginx，脚本自动降级：Python 直接监听 `0.0.0.0:9888`。

## 0.1 宝塔面板环境（重要）

本机 Nginx 由**宝塔**接管，主配置 `/www/server/nginx/conf/nginx.conf` 实际只 include：

```
include /www/server/panel/vhost/nginx/*.conf;
```

`/etc/nginx/conf.d/` **不会被加载**。因此站点配置必须落在
`/www/server/panel/vhost/nginx/pet-bi-9888.conf`，reload 走 `/etc/init.d/nginx reload`。
脚本已内置该分支；若检测到非宝塔环境则自动回退到 `conf.d` 路径。

现有站点 `html_<SERVER_IP>.conf`（80 端口）保持原样，本次只**新增**文件。

## 1. 上传（本机执行）

```bash
scp -i <私钥> dist/pet-supplies-bi-deploy.tar.gz root@<SERVER_IP>:/tmp/
scp -i <私钥> deploy/bootstrap_remote.sh      root@<SERVER_IP>:/tmp/
```

## 2. 服务器端一键部署（SSH 执行）

```bash
bash /tmp/bootstrap_remote.sh
```

脚本依次完成：环境探测 → 装依赖 → 解压到 `/opt/pet-bi` → systemd 守护 + 开机自启
→ 28 个接口预热 → 系统防火墙放行 → Nginx 反代（`nginx -t` 校验失败自动回滚，
不影响 80 端口站点）→ 三项冒烟验收。

## 3. 阿里云控制台（需人工操作）

防火墙 / 安全组 → 添加规则：**TCP 9888**，源 `0.0.0.0/0`（演示期）。

## 4. 验收

```bash
curl -s -o /dev/null -w "首页 %{http_code}\n" http://<SERVER_IP>:9888/
curl -s -o /dev/null -w "API  %{http_code}\n" http://<SERVER_IP>:9888/api/overview
curl -s -o /dev/null -w "图片 %{http_code}\n" http://<SERVER_IP>:9888/static/img/products/AP221c6ce139.jpg
```

浏览器检查：7 个子页 + 双角色切换 + 商品图显示 + 时间筛选联动。

## 5. 运维速查

```bash
journalctl -u bi-dashboard -f      # 实时日志（>1s 请求带 [SLOW] 标记）
systemctl restart bi-dashboard     # 重启
systemctl stop bi-dashboard        # 停止
du -sh /opt/pet-bi                 # 占用（约 600MB 解包后）
tail -f /opt/pet-bi/dashboard/access.log
```

## 6. 安全边界（演示用途）

- 服务为**只读**演示，无写接口；CSV 导出为流式只读。
- SQL 参数已做白名单 / 正则净化（v1.7.1），日期、类目、行为类型均受限。
- 建议：答辩结束后停止服务或限制来源 IP；如需长期对外，加 HTTPS 并接入鉴权。
