"""把热门商品的真实图片下载到本地静态目录，消除看板对外网 CDN 的强依赖。

背景（Q1）：dim_product.image_url 直连 m.media-amazon.com。答辩现场/部署环境
若无法访问该 CDN，商品图会全部回落到占位插画。本地化后形成四级回退：
    本地静态图 → 外网原图 → 类目插画 → 内联 SVG

设计要点：
- **按商品重要度优先**：只抓 leaflet-spec.md 里会真正出现在榜单/对比/详情中的
  商品（默认 TOP 800，按 total_revenue 排序），避免全量 1.7 万张拖垮打包体积。
- **文件名 = product_id**：前端无需改 DB 即可按 ID 拼本地路径，miss 时透明回退。
- **幂等且可续跑**：目标文件已存在且 >1KB 即跳过。
- **失败留痕**：下载失败写入 <out>/_failed.csv，可人工补图或增补运行。
- **不修改数据库**：保持「数仓只读」原则，本地化纯静态资源层行为。

用法：
    python etl/download_product_images.py [--top 800] [--workers 16]
"""
import argparse
import concurrent.futures as futures
import csv
import os
import sqlite3
import sys
import urllib.request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "warehouse", "pet_supplies_dw.db")
OUT_DIR = os.path.join(BASE_DIR, "dashboard", "static", "img", "products")
FAILED_CSV = "failed.csv"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def fetch_targets(top_n: int):
    """按销售额取 TOP N 且有图 URL 的商品。"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT product_id, image_url
            FROM dim_product
            WHERE image_url IS NOT NULL AND image_url <> ''
            ORDER BY COALESCE(total_revenue, 0) DESC, COALESCE(total_sales, 0) DESC
            LIMIT ?
            """,
            (top_n,),
        ).fetchall()
        return [(r["product_id"], r["image_url"]) for r in rows]
    finally:
        conn.close()


def download_one(item):
    pid, url = item
    dst = os.path.join(OUT_DIR, pid + ".jpg")
    # 幂等：已成功下载过的跳过（紧跟首次运行失败的 0 字节/极小文件）
    if os.path.exists(dst) and os.path.getsize(dst) > 1024:
        return (pid, "skip", os.path.getsize(dst))
    req = urllib.request.Request(url, headers={"User-Agent": UA,
                                               "Accept": "image/avif,image/webp,image/*,*/*;q=0.8"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            ctype = (resp.headers.get("Content-Type") or "").lower()
            data = resp.read()
        if len(data) < 1024 or "image" not in ctype:
            return (pid, "bad_response", len(data))
        with open(dst, "wb") as f:
            f.write(data)
        return (pid, "ok", len(data))
    except Exception as e:  # 网络抖动/403/404 一律记录后继续，不中断整批
        return (pid, "error", repr(e)[:160])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=800, help="按销售额取前 N 个有图商品")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    targets = fetch_targets(args.top)
    print(f"[INFO] 目标商品 {len(targets)} 个，输出目录 {OUT_DIR}", flush=True)

    stats = {"ok": 0, "skip": 0, "error": 0, "bad_response": 0}
    failed = []
    total_bytes = 0
    with futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, (pid, status, info) in enumerate(ex.map(download_one, targets), 1):
            stats[status if status in stats else "error"] += 1
            if status in ("ok", "skip") and isinstance(info, int):
                total_bytes += info
            if status in ("error", "bad_response"):
                failed.append((pid, status, info))
            if i % 50 == 0 or i == len(targets):
                print(f"[PROG] {i}/{len(targets)} "
                      f"ok={stats['ok']} skip={stats['skip']} "
                      f"fail={stats['error'] + stats['bad_response']}", flush=True)

    if failed:
        with open(os.path.join(OUT_DIR, FAILED_CSV), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["product_id", "reason", "detail"])
            w.writerows(failed)

    print("=" * 56)
    print(f"成功下载   : {stats['ok']}")
    print(f"已存在跳过 : {stats['skip']}")
    print(f"失败       : {stats['error'] + stats['bad_response']}")
    print(f"本地图总大小: {total_bytes / 1024 / 1024:.1f} MB")
    print(f"目录        : {OUT_DIR}")
    if failed:
        print(f"失败清单    : {os.path.join(OUT_DIR, FAILED_CSV)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
