"""
宠物用品电商 - 数据可视化平台 (BI Dashboard)
Flask + ECharts 构建的交互式数据可视化平台
"""
import os
import sys
import json
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

from flask import Flask, render_template, jsonify, request, send_from_directory

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
DB_PATH = os.path.join(WAREHOUSE_DIR, "pet_supplies_dw.db")
REC_DIR = os.path.join(BASE_DIR, "recommendation")

app = Flask(__name__, 
            template_folder=os.path.join(os.path.dirname(__file__), "templates"),
            static_folder=os.path.join(os.path.dirname(__file__), "static"))

# ============================================================
# 数据查询辅助函数
# ============================================================

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def query_json(sql, params=None):
    conn = get_db()
    try:
        if params:
            rows = conn.execute(sql, params).fetchall()
        else:
            rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

# ============================================================
# API 路由 - 看板数据
# ============================================================

@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/overview")
def api_overview():
    """总览数据"""
    conn = get_db()
    try:
        # 总用户数
        total_users = conn.execute("SELECT COUNT(*) FROM dim_user").fetchone()[0]
        # 总订单数
        total_orders = conn.execute("SELECT COUNT(*) FROM fact_orders WHERE order_status='已完成'").fetchone()[0]
        # 总销售额
        total_revenue = conn.execute("SELECT SUM(actual_amount) FROM fact_orders WHERE order_status='已完成'").fetchone()[0] or 0
        # 总商品数
        total_products = conn.execute("SELECT COUNT(*) FROM dim_product WHERE shelf_status=1").fetchone()[0]
        # 复购用户数
        repurchase = conn.execute("SELECT COUNT(*) FROM dim_user WHERE lifetime_orders >= 2").fetchone()[0]
        # 评价总数
        total_reviews = conn.execute("SELECT COUNT(*) FROM fact_reviews").fetchone()[0]
        # RFM分布
        rfm = query_json("SELECT rfm_segment, COUNT(*) as cnt FROM dim_user WHERE lifetime_orders>0 GROUP BY rfm_segment")
        
        return jsonify({
            "total_users": total_users,
            "total_orders": total_orders,
            "total_revenue": round(total_revenue, 2),
            "total_products": total_products,
            "repurchase_users": repurchase,
            "repurchase_rate": round(repurchase / total_users * 100, 1) if total_users else 0,
            "total_reviews": total_reviews,
            "rfm_distribution": rfm
        })
    finally:
        conn.close()

@app.route("/api/sales_trend")
def api_sales_trend():
    """销售趋势"""
    days = request.args.get("days", 90, type=int)
    data = query_json(f"""
        SELECT dd.date_str, dd.day_name, dd.is_weekend,
               COALESCE(ads.order_count, 0) as order_count,
               COALESCE(ads.actual_amount, 0) as revenue,
               COALESCE(ads.user_count, 0) as user_count,
               COALESCE(ads.avg_order_amount, 0) as avg_order,
               COALESCE(ads.new_user_count, 0) as new_users
        FROM dim_date dd
        LEFT JOIN ads_daily_sales ads ON dd.date_id = ads.date_id
        WHERE dd.date_str >= date('now', '-' || ? || ' days')
        ORDER BY dd.date_id
    """, [days])
    return jsonify(data)

@app.route("/api/behavior_trend")
def api_behavior_trend():
    """行为趋势"""
    days = request.args.get("days", 90, type=int)
    data = query_json(f"""
        SELECT dd.date_str, dd.day_name,
               COALESCE(adb.pv, 0) as pv,
               COALESCE(adb.uv, 0) as uv,
               COALESCE(adb.collect_count, 0) as collect_count,
               COALESCE(adb.cart_add_count, 0) as cart_add_count
        FROM dim_date dd
        LEFT JOIN ads_daily_behavior adb ON dd.date_id = adb.date_id
        WHERE dd.date_str >= date('now', '-' || ? || ' days')
        ORDER BY dd.date_id
    """, [days])
    return jsonify(data)

@app.route("/api/category_sales")
def api_category_sales():
    """类目销售分析"""
    data = query_json("""
        SELECT main_category, sub_category,
               SUM(sales_amount) as revenue,
               SUM(order_count) as orders,
               SUM(sales_quantity) as quantity
        FROM ads_category_sales
        GROUP BY main_category, sub_category
        ORDER BY revenue DESC
    """)
    return jsonify(data)

@app.route("/api/heatmap")
def api_heatmap():
    """时段分析热力图"""
    data = query_json("""
        SELECT order_hour, 
               CASE 
                   WHEN order_hour BETWEEN 0 AND 5 THEN '深夜'
                   WHEN order_hour BETWEEN 6 AND 11 THEN '上午'
                   WHEN order_hour BETWEEN 12 AND 17 THEN '下午'
                   ELSE '晚上'
               END as period,
               COUNT(*) as cnt,
               SUM(actual_amount) as revenue,
               AVG(actual_amount) as avg_amount
        FROM fact_orders
        WHERE order_status='已完成'
        GROUP BY order_hour
        ORDER BY order_hour
    """)
    return jsonify(data)

@app.route("/api/user_profile")
def api_user_profile():
    """用户画像分析"""
    # 宠物类型分布
    pet_data = query_json("""
        SELECT pet_type, COUNT(*) as cnt FROM dim_user GROUP BY pet_type
    """)
    # 城市等级分布
    city_data = query_json("""
        SELECT city_tier, COUNT(*) as cnt FROM dim_user GROUP BY city_tier
    """)
    # 消费层级分布
    consume_data = query_json("""
        SELECT consume_level, COUNT(*) as cnt, AVG(lifetime_amount) as avg_amount
        FROM dim_user WHERE lifetime_orders>0 GROUP BY consume_level
    """)
    # 年龄分布
    age_data = query_json("""
        SELECT 
            CASE WHEN age<25 THEN '18-24' WHEN age<30 THEN '25-29'
                 WHEN age<35 THEN '30-34' WHEN age<40 THEN '35-39'
                 WHEN age<45 THEN '40-44' ELSE '45+' END as age_group,
            COUNT(*) as cnt
        FROM dim_user GROUP BY age_group ORDER BY age_group
    """)
    
    return jsonify({
        "pet_type": pet_data,
        "city_tier": city_data,
        "consume_level": consume_data,
        "age_group": age_data
    })

@app.route("/api/top_products")
def api_top_products():
    """热门商品"""
    limit = request.args.get("limit", 20, type=int)
    data = query_json(f"""
        SELECT product_name, main_category, sub_category, 
               price, total_sales, total_revenue, 
               ROUND(avg_rating, 1) as avg_rating, review_count
        FROM dim_product
        WHERE total_sales > 0
        ORDER BY total_sales DESC
        LIMIT {limit}
    """)
    return jsonify(data)

@app.route("/api/rfm_analysis")
def api_rfm_analysis():
    """RFM分析"""
    data = query_json("""
        SELECT rfm_segment, COUNT(*) as cnt, 
               ROUND(AVG(lifetime_amount), 1) as avg_amount,
               ROUND(AVG(lifetime_orders), 1) as avg_orders,
               ROUND(AVG(avg_order_amount), 1) as avg_aov
        FROM dim_user WHERE lifetime_orders>0
        GROUP BY rfm_segment
    """)
    return jsonify(data)

@app.route("/api/recommendation_stats")
def api_recommendation_stats():
    """推荐系统效果统计"""
    result_path = os.path.join(REC_DIR, "recommendation_results.json")
    if os.path.exists(result_path):
        with open(result_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data)
    return jsonify({"error": "推荐结果尚未生成", "results": {}})

# ============================================================
# 页面路由
# ============================================================

@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")

@app.route("/products")
def products_page():
    return render_template("products.html")

@app.route("/recommendation")
def recommendation_page():
    return render_template("recommendation.html")

# ============================================================
# 启动
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("宠物用品电商 BI 可视化平台")
    print(f"启动地址: http://0.0.0.0:5000")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=False)
