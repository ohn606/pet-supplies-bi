# -*- coding: utf-8 -*-
"""构建云部署包（v1.8.1）
====================================
只打包看板服务运行时真正需要的产物，其余（7.2GB 原始数据、训练脚本、
输出截图、论文文档）一律不进包，控制上传体积。

包内清单：
    dashboard/            server.py + templates/ + static/（含 958 张本地商品图）
    warehouse/pet_supplies_dw.db   数仓（含全部 ADS 预聚合表）
    analytics/            时序规则 / 生命周期 / A-B 模拟结果与脚本
    recommendation/       推荐结果 JSON 与脚本
    data/raw/             仅 data_period.json + data_dictionary.json（服务启动读取）
    deploy/               systemd unit + 运维 runbook

用法：python deploy/package.py
产物：dist/pet-supplies-bi-deploy.tar.gz（约 620MB，gzip 后更小）
"""
import os
import tarfile
import time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = os.path.join(BASE, "dist")

INCLUDE = [
    ("dashboard/server.py", "dashboard/server.py"),
    ("dashboard/templates", "dashboard/templates"),
    ("dashboard/static", "dashboard/static"),
    ("warehouse/pet_supplies_dw.db", "warehouse/pet_supplies_dw.db"),
    ("warehouse/sql", "warehouse/sql"),
    ("analytics", "analytics"),
    ("recommendation", "recommendation"),
    ("data/raw/data_period.json", "data/raw/data_period.json"),
    ("data/raw/data_dictionary.json", "data/raw/data_dictionary.json"),
    ("deploy/bi-dashboard.service", "deploy/bi-dashboard.service"),
    ("deploy/bootstrap_remote.sh", "deploy/bootstrap_remote.sh"),
    ("deploy/DEPLOY.md", "deploy/DEPLOY.md"),
    ("CHANGELOG.md", "CHANGELOG.md"),
]

SKIP_SUFFIX = (".log", ".pyc", ".bak")


def add(tf, src, arc):
    full = os.path.join(BASE, src)
    if not os.path.exists(full):
        print(f"[警告] 缺失 {src}，跳过")
        return
    if os.path.isfile(full):
        tf.add(full, arcname=arc)
        return
    for root, _dirs, files in os.walk(full):
        for f in files:
            p = os.path.join(root, f)
            rel = os.path.relpath(p, BASE).replace("\\", "/")
            if f.endswith(SKIP_SUFFIX) or "__pycache__" in rel:
                continue
            tf.add(p, arcname=rel)


def main():
    os.makedirs(DIST, exist_ok=True)
    out = os.path.join(DIST, "pet-supplies-bi-deploy.tar.gz")
    t0 = time.time()
    with tarfile.open(out, "w:gz") as tf:
        for src, arc in INCLUDE:
            add(tf, src, arc)
    size = os.path.getsize(out) / 1024 / 1024
    print(f"打包完成: {out}")
    print(f"体积: {size:.1f} MB   耗时: {time.time() - t0:.1f}s")
    print("上传后: tar xzf pet-supplies-bi-deploy.tar.gz -C /opt/pet-bi")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
