#!/usr/bin/env bash
# =============================================================
# 宠物用品 BI 看板 —— 服务器端一键部署脚本（v1.8.1）
# 用法（root 执行）：bash /tmp/bootstrap_remote.sh
# 前置：/tmp/pet-supplies-bi-deploy.tar.gz 已上传
#
# 端口分工（关键）：
#   对外 9888  ← Nginx（公网可访问）
#   内部 9889  ← Python 看板服务（仅 127.0.0.1，外网不可达）
#   无 Nginx 时自动降级：Python 直接监听 0.0.0.0:9888
# 设计原则：只新增 conf.d 文件，不删除/覆盖 80 端口上的既有站点
# =============================================================
set -euo pipefail

APP_DIR=/opt/pet-bi
OUT_PORT=9888          # 对外
IN_PORT=9889           # Python 服务内部端口
PKG=/tmp/pet-supplies-bi-deploy.tar.gz
RS=2017-07-03
RE=2018-10-02

# 重要（宝塔面板环境）：
#   本机 Nginx 由宝塔接管，主配置只 include /www/server/panel/vhost/nginx/*.conf，
#   /etc/nginx/conf.d/ **不会被加载**。因此站点配置必须落在前者目录。
#   现有站点 html_<SERVER_IP>.conf（80 端口）保持原样，本次只新增文件。
NGINX_VHOST_DIR=/www/server/panel/vhost/nginx
NGINX_CONF=pet-bi-9888.conf
[ -d "$NGINX_VHOST_DIR" ] || NGINX_VHOST_DIR=/etc/nginx/conf.d
NGINX_TEST="nginx -t"
NGINX_RELOAD="nginx -s reload"
command -v nginx >/dev/null 2>&1 && {
    NGINX_PREFIX=$(nginx -V 2>&1 | tr ' ' '\n' | grep -- '--prefix=' | cut -d= -f2)
    [ -n "$NGINX_PREFIX" ] && [ -x "${NGINX_PREFIX}/sbin/nginx" ] && {
        NGINX_TEST="${NGINX_PREFIX}/sbin/nginx -t"
        NGINX_RELOAD="${NGINX_PREFIX}/sbin/nginx -s reload"
    }
}
[ -x /etc/init.d/nginx ] && NGINX_RELOAD="/etc/init.d/nginx reload"

log() { printf '\n\033[1;36m>>> %s\033[0m\n' "$*"; }

# ---------- 0. 环境探测 ----------
log "0/7 环境探测"
. /etc/os-release 2>/dev/null || true
echo "OS  : ${PRETTY_NAME:-unknown}"
echo "CPU : $(nproc) 核    内存: $(free -m | awk '/Mem:/{print $2}') MB"
echo "磁盘: 剩余 $(df -h / | awk 'NR==2{print $4}')"
[ -f "$PKG" ] || { echo "!! 未找到 $PKG，请先上传"; exit 1; }
[ "$(id -u)" = "0" ] || { echo "!! 请用 root 执行"; exit 1; }

# ---------- 1. 依赖 ----------
log "1/7 依赖检查"
export DEBIAN_FRONTEND=noninteractive
if command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq >/dev/null 2>&1 || echo "（apt update 未成功，继续，依赖已存在则不受影响）"
    command -v nginx  >/dev/null 2>&1 || apt-get install -y -qq nginx  >/dev/null 2>&1 || true
    command -v python3 >/dev/null 2>&1 || apt-get install -y -qq python3 >/dev/null 2>&1 || true
elif command -v yum >/dev/null 2>&1; then
    command -v nginx  >/dev/null 2>&1 || yum install -y nginx  >/dev/null 2>&1 || true
    command -v python3 >/dev/null 2>&1 || yum install -y python3 >/dev/null 2>&1 || true
fi
PY=$(command -v python3 || command -v python)
echo "Python: $PY ($($PY -V 2>&1))"
HAS_NGINX=0
command -v nginx >/dev/null 2>&1 && HAS_NGINX=1
echo "Nginx : $([ $HAS_NGINX = 1 ] && echo '可用' || echo '不可用（Python 直连）')"

# ---------- 2. 解压 ----------
log "2/7 解压部署包到 $APP_DIR"
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR"
tar xzf "$PKG" -C "$APP_DIR"
du -sh "$APP_DIR"
ls "$APP_DIR"

# ---------- 3. systemd ----------
log "3/7 配置 systemd 守护"
if [ $HAS_NGINX = 1 ]; then
    BIND_HOST=127.0.0.1; SVC_PORT=$IN_PORT
else
    BIND_HOST=0.0.0.0;   SVC_PORT=$OUT_PORT
fi
cat > "$APP_DIR/deploy/env" <<EOF
DASHBOARD_HOST=$BIND_HOST
DASHBOARD_PORT=$SVC_PORT
EOF
sed -i "s#^Environment=DASHBOARD_HOST=.*#Environment=DASHBOARD_HOST=$BIND_HOST#" \
    "$APP_DIR/deploy/bi-dashboard.service"
sed -i "s#^Environment=DASHBOARD_PORT=.*#Environment=DASHBOARD_PORT=$SVC_PORT#" \
    "$APP_DIR/deploy/bi-dashboard.service"
sed -i "s#^ExecStart=.*#ExecStart=$PY $APP_DIR/dashboard/server.py#" \
    "$APP_DIR/deploy/bi-dashboard.service"
cp "$APP_DIR/deploy/bi-dashboard.service" /etc/systemd/system/bi-dashboard.service
# 本机物理内存仅约 1.6GB（mysqld 已占 ~184MB，另有宝塔面板与既有站点），
# 演示服务必须自带内存天花板，避免某个重查询 OOM 波及 MySQL / Nginx / 面板。
if ! grep -q '^MemoryMax' /etc/systemd/system/bi-dashboard.service; then
    cat >> /etc/systemd/system/bi-dashboard.service <<'SVC'

# 内存护栏（1.6GB 小内存主机）：超限由内核回收，不会连带拖垮 MySQL/Nginx
MemoryMax=700M
MemoryHigh=550M
# 演示服务可牺牲：OOM 时优先被杀，保护同机的 MySQL 与既有站点
OOMScoreAdjust=600
SVC
fi
systemctl daemon-reload
systemctl enable bi-dashboard >/dev/null 2>&1 || true
systemctl restart bi-dashboard
sleep 3
echo "服务状态: $(systemctl is-active bi-dashboard)"
echo "监听: $BIND_HOST:$SVC_PORT"

# ---------- 4. 预热（首查填充 SQLite 页缓存 + 服务端缓存）----------
log "4/7 预热全部接口（30~60s）"
warm() {
    local u="$1" extra="${2:-}"
    local url="http://127.0.0.1:${SVC_PORT}/api/${u}?start=${RS}&end=${RE}${extra}"
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 120 "$url")
    printf '  %-34s %s\n' "$u" "$code"
}
for u in overview sales_trend behavior_trend category_sales heatmap time_analysis \
         user_profile user_summary user_search_terms user_path_analysis conversion_funnel \
         user_retention rfm_analysis user_segments pet_lifecycle recommendation_stats \
         brand_analysis category_detail price_distribution product_summary \
         product_sales_trend pet_top pet_category_share temporal_rules \
         repurchase_cycle replenishment_candidates ab_presets; do warm "$u"; done
warm "recommendation_daily" "&bucket=month"

# ---------- 5. 防火墙 ----------
log "5/7 系统防火墙"
if command -v ufw >/dev/null 2>&1; then
    ufw allow ${OUT_PORT}/tcp >/dev/null 2>&1 || true; echo "ufw 已放行 ${OUT_PORT}"
elif command -v firewall-cmd >/dev/null 2>&1; then
    firewall-cmd --permanent --add-port=${OUT_PORT}/tcp >/dev/null 2>&1 || true
    firewall-cmd --reload >/dev/null 2>&1 || true; echo "firewalld 已放行 ${OUT_PORT}"
else
    echo "无 ufw/firewalld，跳过"
fi
echo "!! 仍需到阿里云控制台 → 防火墙/安全组 → 放行 TCP ${OUT_PORT}"

# ---------- 6. Nginx 反代（新增独立 conf，不动 80 站点）----------
if [ $HAS_NGINX = 1 ]; then
    log "6/7 配置 Nginx：listen ${OUT_PORT} → 127.0.0.1:${IN_PORT}"
    CONF="$NGINX_VHOST_DIR/$NGINX_CONF"
    mkdir -p "$NGINX_VHOST_DIR"
    cat > "$CONF" <<EOF
server {
    listen ${OUT_PORT};
    server_name _;

    client_max_body_size 10m;
    gzip on;
    gzip_types text/plain text/css application/json application/javascript image/svg+xml;
    gzip_min_length 1024;

    location / {
        proxy_pass http://127.0.0.1:${IN_PORT};
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_read_timeout 120s;
    }
}
EOF
    if $NGINX_TEST 2>/dev/null; then
        $NGINX_RELOAD 2>/dev/null || true
        echo "Nginx 已加载 $CONF（80 端口既有站点未改动）"
    else
        echo "!! nginx -t 未通过，移除配置以免影响既有站点"
        $NGINX_TEST
        rm -f "$CONF"
    fi
else
    log "6/7 跳过 Nginx（Python 直连 ${OUT_PORT}）"
fi

# ---------- 7. 验收 ----------
log "7/7 验收"
IP=$(curl -s --max-time 5 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')
for path in "/" "/api/overview" "/static/img/products/AP221c6ce139.jpg"; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 60 \
           "http://127.0.0.1:${OUT_PORT}${path}")
    printf '  %-46s %s\n' "$path" "$code"
done
echo
echo "=========================================================="
echo "  部署完成 · v1.8.1"
echo "  公网访问: http://${IP}:${OUT_PORT}"
echo "  实时日志: journalctl -u bi-dashboard -f"
echo "  重启服务: systemctl restart bi-dashboard"
echo "=========================================================="
