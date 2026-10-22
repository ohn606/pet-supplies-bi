#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
商品图 URL 回填（ODS → DWD）

背景
----
原始数据集中**存在**商品图片 URL，但在 data/build_real_ods.py 构建 ODS 时被
降级成 0/1 计数（image_count），URL 本体被丢弃：

  * amazon_reviews.csv            → 字段 image（形如 "['https://...','https://...']"）
  * chewy_scraper_sample.csv      → 字段 images（同上，Python list 字符串）
  * amazon_pet_supplies_dataset_sample.csv → 字段 images（'~' 分隔）

因此看板「宠物热销榜」商品图只能回落到类目占位图。本脚本把 URL 重新抽取回来，
作为**新增的派生列**写入 dim_product.image_url，保持可追溯：

  - 不臆造任何 URL，取不到即为空；
  - 同时保留原始 URL（image_url_src）与来源（image_source）用于审计；
  - 仅做尺寸令牌升级（_SS40_ → _SS400_），不改变图片内容本身。

用法
----
    python etl/build_product_images.py           # 抽取 + 写 CSV + 回填 dim_product
    python etl/build_product_images.py --dry-run # 只抽取并打印覆盖率，不写库
"""
import argparse
import csv
import hashlib
import os
import re
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
REAL_DIR = os.path.join(RAW_DIR, "real")
DB_PATH = os.path.join(BASE_DIR, "warehouse", "pet_supplies_dw.db")
OUT_CSV = os.path.join(RAW_DIR, "product_images.csv")

csv.field_size_limit(sys.maxsize)

URL_RE = re.compile(r"https?://[^\s'\"]+")


def hid(prefix, text, n=10):
    """与 data/build_real_ods.py 保持一致的 ID 生成规则"""
    return prefix + hashlib.md5(str(text).encode("utf-8")).hexdigest()[:n]


def first_url(raw):
    """从任意形态的 image(s) 字段中取第一个可用 URL"""
    if not raw:
        return ""
    s = str(raw).strip()
    if not s or s in ("[]", "nan"):
        return ""
    m = URL_RE.search(s)
    return m.group(0) if m else ""


def upscale(u):
    """把 40px 缩略图令牌升级到 400px（内容不变，仅尺寸），并换用现行 CDN 域名"""
    if not u:
        return ""
    u = re.sub(r"\._(SS|SL)\d+_\.", "._SS400_.", u)
    u = u.replace("images-na.ssl-images-amazon.com", "m.media-amazon.com")
    u = u.replace("images-eu.ssl-images-amazon.com", "m.media-amazon.com")
    u = u.replace("https://ecx.images-amazon.com", "https://m.media-amazon.com")
    return u


# ------------------------- 1. Amazon Reviews 2018（AP* 商品，占热销榜主体） -------------------------
def scan_amazon_reviews():
    path = os.path.join(REAL_DIR, "amazon_reviews.csv")
    if not os.path.exists(path):
        print(f"[跳过] 缺少 {path}")
        return {}
    print("[S1] 扫描 amazon_reviews.csv（1.2GB，仅取 Pet_Supplies 且首次出现）…")
    t0 = time.time()
    mapping, seen, scanned = {}, 0, 0
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        for r in csv.DictReader(f):
            scanned += 1
            if (r.get("category") or "").strip() != "Pet_Supplies":
                continue
            pid = hid("AP", (r.get("itemName") or "").strip())
            if pid in mapping:
                continue
            u = first_url(r.get("image"))
            if u:
                mapping[pid] = (u, "amazon_reviews_2018")
            if scanned % 2_000_000 == 0:
                print(f"     已扫描 {scanned:,} 行，命中 {len(mapping):,} 个商品 "
                      f"（{time.time()-t0:.0f}s）")
    print(f"     扫描 {scanned:,} 行，命中 {len(mapping):,} 个商品（{time.time()-t0:.0f}s）")
    return mapping


# ------------------------- 2. Chewy 商品属性（CW*） -------------------------
def scan_chewy():
    path = os.path.join(REAL_DIR, "chewy_scraper_sample.csv")
    if not os.path.exists(path):
        print(f"[跳过] 缺少 {path}")
        return {}
    out = {}
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        for r in csv.DictReader(f):
            key = (r.get("sku") or "").strip() or (r.get("uniq_id") or "").strip()
            if not key:
                continue
            u = first_url(r.get("images"))
            if u:
                out[hid("CW", key)] = (u, "chewy")
    print(f"[S2] Chewy 商品 {len(out):,} 个")
    return out


# ------------------------- 3. Amazon India 宠物商品（AI*） -------------------------
def scan_amazon_in():
    path = os.path.join(REAL_DIR, "amazon_pet_supplies_dataset_sample.csv")
    if not os.path.exists(path):
        print(f"[跳过] 缺少 {path}")
        return {}
    out = {}
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        for r in csv.DictReader(f):
            key = (r.get("asin") or "").strip() or (r.get("uniq_id") or "").strip()
            if not key:
                continue
            u = first_url(r.get("images"))
            if u:
                out[hid("AI", key)] = (u, "amazon_in_pet")
    print(f"[S3] Amazon(IN) 商品 {len(out):,} 个")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只统计覆盖率，不写库")
    args = ap.parse_args()

    mapping = {}
    for fn in (scan_chewy, scan_amazon_in, scan_amazon_reviews):
        mapping.update(fn())

    # 与商品主数据对齐
    conn = sqlite3.connect(DB_PATH)
    ids = [r[0] for r in conn.execute("SELECT product_id FROM dim_product")]
    total = len(ids)
    hit = [p for p in ids if p in mapping]

    # 动销商品（热销榜实际展示范围）覆盖率
    sold = [r[0] for r in conn.execute(
        "SELECT product_id FROM dim_product WHERE total_sales > 0")]
    sold_hit = [p for p in sold if p in mapping]

    print("\n===== 覆盖率 =====")
    print(f"全量商品      : {len(hit):,} / {total:,}  ({len(hit)/total:.1%})")
    print(f"动销商品      : {len(sold_hit):,} / {len(sold):,}  ({len(sold_hit)/len(sold):.1%})")

    rows = []
    for pid in ids:
        src, source = mapping.get(pid, ("", ""))
        rows.append({"product_id": pid,
                     "image_url": upscale(src),
                     "image_url_src": src,
                     "image_source": source})
    conn.close()

    if args.dry_run:
        print("\n[dry-run] 未写入任何文件/数据库")
        return

    os.makedirs(RAW_DIR, exist_ok=True)
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["product_id", "image_url",
                                          "image_url_src", "image_source"])
        w.writeheader()
        w.writerows(rows)
    print(f"\n[写出] {OUT_CSV}  ({len(rows):,} 行)")

    conn = sqlite3.connect(DB_PATH)
    cols = [c[1] for c in conn.execute("PRAGMA table_info(dim_product)")]
    if "image_url" not in cols:
        conn.execute("ALTER TABLE dim_product ADD COLUMN image_url TEXT")
        print("[DDL] dim_product 新增列 image_url")
    conn.executemany(
        "UPDATE dim_product SET image_url = ? WHERE product_id = ?",
        [(r["image_url"] or None, r["product_id"]) for r in rows])
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM dim_product WHERE image_url IS NOT NULL "
                     "AND image_url <> ''").fetchone()[0]
    conn.close()
    print(f"[回填] dim_product.image_url 已赋值 {n:,} / {total:,} 行")


if __name__ == "__main__":
    main()
