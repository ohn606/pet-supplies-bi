# 真实公开数据集接入说明（替代程序生成的模拟数据）

> 适用范围：宠物用品电商用户行为分析与智能推荐系统研究（毕业设计）
> 更新时间：2026-10-09
> 配套脚本：`data/download_real_datasets.py`（下载）、`data/build_real_ods.py`（字段对齐与 ODS 构建）

---

## 一、为什么必须替换模拟数据

原实验数据由 `data/generate_data.py` 程序生成，存在三个致命问题：

1. **不可作为研究证据**：模拟数据的分布由人为参数（价格区间、复购概率、促销周期）设定，任何"规律发现"本质上是对生成参数的复述，存在循环论证。
2. **评审可质疑**：毕业论文答辩中，模拟数据构成的实验结论无法支撑"真实业务洞察"类主张。
3. **不可复现与不可比对**：无法与领域已有研究成果横向对比。

因此本项目改为接入**三个平台的公开真实数据集**，通过统一 ODS 层完成多源融合，保持原有数仓建模、ETL、OLAP、推荐算法与可视化流程不变。

---

## 二、数据集清单（每平台 1–2 个，按匹配度排序）

### 🔹 平台一：Kaggle

| # | 数据集 | 官方链接 | 规模 | 主要字段 | 适用任务 | 许可 |
|---|---|---|---|---|---|---|
| **K1** | **Amazon Reviews 2018 (Full Dataset)**<br>rogate16 镜像，源数据为 UCSD McAuley Lab | [Kaggle](https://www.kaggle.com/datasets/rogate16/amazon-reviews-2018-full-dataset) | 551,159 条评论（1.23 GB CSV，14 列）<br>其中 **Pet_Supplies 类目 169,708 条**（筛选 verified 购买后） | `userName`(用户) `itemName`(商品) `brand` `price` `rating` `reviewTime` `summary` `reviewText` `vote` `verified` `category` `image` `description` `feature` | ① 用户-商品-评分矩阵构建<br>② 协同过滤 / 矩阵分解 / 混合推荐<br>③ 商品维、订单事实、评价事实<br>④ 用户-商品二部图与购物篮分析 | **License: Other**<br>原始发布方（Ni et al., UCSD）声明：数据非其所有，但**要求仅用于研究目的**并引用 Ni et al. 2019 → 学术研究可用 ✅ |
| **K2** | **Amazon Pet Supplies Data**（亚马逊印度站宠物用品商品数据） | [Kaggle](https://www.kaggle.com/datasets/crawlfeeds/amazon-pet-supplies-data) | 343 条商品（214 kB，15 列） | `asin` `title` `price` `currency` `brand` `breadcrumbs`(四级类目面包屑) `description` `availability` `images` `overview` `about_item` `specifications` | ① 商品维属性补全<br>② **四级类目体系构建的真实依据**（如 `Pet Supplies \| Cats \| Toys \| Feather Toys`）<br>③ 价格与品牌分布校准 | **CC0: Public Domain** ✅<br>可自由使用，学术使用建议注明来源 |
| **K3** | **Chewy Products Dataset**（美国最大宠物电商商品数据） | [Kaggle](https://www.kaggle.com/datasets/crawlfeeds/chewy-data) | 215 条商品（239 kB，21 列） | `name` `sku` `breadcrumb`(层级类目) `brand` `Price` `average_rating` `reviews_count` `ingredients` `nutrition_analysis` `feeding_instructions` | ① 宠物垂类电商价格带校准<br>② 商品评分与评论数分布<br>③ 宠物食品营养属性（可扩展） | **CC0: Public Domain** ✅ |

> K1 为**核心数据源**（提供用户-商品交互），K2/K3 为**商品属性补充源**，三者共同构成宠物垂类商品主数据。

---

### 🔹 平台二：阿里云天池

| # | 数据集 | 官方链接 | 规模 | 主要字段 | 适用任务 | 许可 |
|---|---|---|---|---|---|---|
| **T1** | **淘宝用户购物行为数据集 UserBehavior**<br>（dataId=649，阿里巴巴官方发布） | [天池](https://tianchi.aliyun.com/dataset/649) | **100,150,807 条行为**（解压后 3.67 GB）<br>987,994 用户 / 4,162,024 商品 / 9,439 类目<br>时间跨度 2017-11-25 ~ 2017-12-03 | `user_id` `item_id` `item_category` `behavior_type`(pv/buy/cart/fav) `timestamp` | ① **真实 pv/收藏/加购/购买 行为序列**<br>② 转化漏斗、行为热力图、用户路径<br>③ 留存队列分析<br>④ 隐式反馈推荐与序列建模 | **CC BY-NC-SA 4.0** ✅<br>署名-非商业性使用-相同方式共享<br>学位论文属非商业用途，需署名并引用官方给出的 3 篇论文 |
| **T2** | **阿里移动推荐算法数据集**<br>（dataId=46） | [天池](https://tianchi.aliyun.com/dataset/46) | 约 1,226 万条（2014-11-18 ~ 2014-12-18） | `user_id` `item_id` `behavior_type`(1点击/2收藏/3加购/4支付) `item_category` `time` `user_geohash` | 备选行为源；含地理位置字段，可做地域分析 | **CC BY-NC-SA 4.0** ✅ |

> **实际获取说明**：本开发环境对 `tianchi.aliyun.com` 域名不可达（连接超时），且天池下载需阿里云账号登录。
> 因此采用**同一官方数据集的 Kaggle 镜像**（`marwa80/userbehavior`，[链接](https://www.kaggle.com/datasets/marwa80/userbehavior)，906 MB 压缩包）获取完全相同的 UserBehavior 数据，
> 并在论文中同时标注天池官方来源与镜像获取路径，保证可追溯。
> 若需从官方渠道获取，见 §五 手动下载指引。

---

### 🔹 平台三：飞桨 AI Studio

| # | 数据集 | 官方链接 | 规模 | 主要字段 | 适用任务 | 许可 |
|---|---|---|---|---|---|---|
| **A1** | **电商用户购物行为数据集**（UserBehavior 的飞桨镜像，datasetId=316201） | [AI Studio](https://aistudio.baidu.com/datasetdetail/316201/0) | UserBehavior.csv 3.5 GB（与 T1 同源） | 同 T1：`user_id` `item_id` `item_category` `behavior_type` `timestamp` | 同 T1；作为国内合规镜像渠道 | **CC0 1.0** ✅<br>公共领域奉献，使用限制最少 |
| **A2** | **宠物狗分类数据集（11 类）**<br>datasetId=167263 | [AI Studio](https://aistudio.baidu.com/aistudio/datasetdetail/167263) | 1,091 张图像 | 图像 + 类别标签 | ❌ 与本论文**结构化行为分析主线不匹配**，仅可用于商品图像特征扩展 | **CC0 1.0** |

> **平台适配性说明（为何飞桨侧无更优选择）**：
> 飞桨 AI Studio 的宠物相关数据集**以计算机视觉类为主**（猫狗分类、犬种细粒度识别、宠物年龄识别等），
> 与本研究"用户行为分析 + 推荐系统"的结构化数据需求不匹配。经检索，平台上**唯一**符合结构化电商行为分析需求的数据集
> 即 A1（UserBehavior 镜像），且需登录下载。
> **替代方案**：以天池官方源 T1 为行为数据主来源，飞桨 A1 作为国内镜像备份渠道；二者数据内容一致，互为冗余。
> 若后续需引入非结构化数据扩展商品图像特征，再考虑 A2 类 CV 数据集。

---

## 三、多源融合方案与字段对齐

### 3.1 数据源角色分工

```
┌──────────────── 统一 ODS 层（与 generate_data.py 完全同构）────────────────┐
│  users.csv  products.csv  orders.csv  order_items.csv  behavior_logs.csv  reviews.csv │
└───────────────────────────────────────────────────────────────────────────┘
        ▲              ▲                ▲                    ▲
        │              │                │                    │
   S1 Amazon      S1+S2+S3        S1 Amazon            S4 淘宝 UserBehavior
   评价用户        宠物商品主数据      评价即购买订单          pv/fav/cart/buy 真实序列
   (48,473)      (20,836)         (169,708)             (2,478,615 抽样)
```

### 3.2 字段对齐三原则

| 原则 | 处理方式 | 示例 |
|---|---|---|
| **直接映射** | 真实数据中存在 → 直接映射 | `rating ← overall`、`review_content ← summary+reviewText`、`price ← price` |
| **显式派生** | 可由真实数据推导 → 标注派生规则 | `pet_type` ← 用户购买商品所属宠物类型的**众数推断**；<br>`consume_level` ← 真实累计消费金额**分位数**（P90/P60）；<br>`activity_level` ← 真实订单数分档；<br>`quality_score` ← 商品历史评分**均值** |
| **明确缺失** | 真实数据中不存在 → 置标记值，**严禁随机生成** | `age=0`、`gender='未知'`、`pay_method='未知'`、`duration_seconds=0`、`device_type='未知'` |

> ⚠️ 关键约束：所有"不可用"字段均在数据字典中标注，论文中如实说明，**不以模拟值填充**，以保证数据真实性与可复现性。

### 3.3 关键主键构造

原始镜像未提供 `reviewerID` / `ASIN`，采用稳定哈希构造主键（确定性，可复现）：

- `user_id = "AU" + md5(userName)[:10]`（先剔除 `Amazon Customer` / `Kindle Customer` 匿名占位用户名 13,293 条）
- `product_id = "AP" + md5(itemName)[:10]`
- 淘宝侧：`user_id = "TU" + 原始user_id`、`product_id = "TI" + 原始item_id`（原始 ID 已脱敏，直接保留）

### 3.4 跨源实体空间处理（松耦合引用）

行为事实表（淘宝）的用户/商品实体空间与商品主数据域（Amazon）**不同源**，无法建立真实映射。
处理方式：`fact_user_behavior` 对 `dim_user`/`dim_product` **不强制外键**，仅对 `dim_date` 强制；
跨源关联通过 `data_source` 字段实现。这样既支持异构融合，又避免伪造实体映射。

对应地，为避免跨源统计失真（原漏斗接口混合了行为表用户与订单表用户），
**转化漏斗全部环节改为统一取自行为事实表**（浏览→收藏→加购→下单）。

### 3.5 抽样策略（淘宝行为数据）

1 亿条全量入库不现实，采用**用户级系统抽样**：保留 `user_id % 40 == 0` 的用户（约 24,700 人），
保留其**完整行为序列**（共 2,478,615 条），保证用户级路径/漏斗/留存分析的序列完整性，
而非按行随机抽样（后者会破坏用户行为序列）。

---

## 四、真实数据规模（构建后）

| 层 | 表 | 记录数 | 来源 |
|---|---|---|---|
| ODS | users | 48,473 | S1 |
| ODS | products | 20,836 | S1(20,318) + S2(343) + S3(175) |
| ODS | orders | 169,708 | S1 |
| ODS | order_items | 169,708 | S1 |
| ODS | behavior_logs | 2,648,323 | S4(2,478,615) + S1 评价事件(169,708) |
| ODS | reviews | 169,708 | S1 |
| 时间跨度 | — | 2017-07-03 ~ 2018-10-02 | S1(日期粒度) + S4(秒级) |

类目分布（真实推断）：狗用品 7,193 / 通用用品 6,232 / 猫用品 4,282 / 水族用品 1,385 / 小宠用品 908 / 鸟类用品 836
行为分布（真实）：浏览 2,218,299 / 评价 169,708 / 加购 138,370 / 收藏 71,785 / 下单 50,161

---

## 五、复现步骤

```bash
# 1) 下载公开数据集（Kaggle 公开端点无需登录；天池/飞桨需手动，见脚本输出指引）
python data/download_real_datasets.py --list          # 查看清单
python data/download_real_datasets.py                 # 下载可自动获取的数据集

# 2) 构建统一 ODS 层（字段对齐 + 真实数据派生；模拟数据自动备份到 data/raw/simulated_backup/）
python data/build_real_ods.py

# 3) 重跑数据仓库 ETL（时间维度自动适配真实数据周期）
python etl/run_etl.py

# 4) 重跑分析与推荐
python analytics/spark_analytics.py
python recommendation/hybrid_recommender.py

# 5) 启动可视化平台
python dashboard/server.py
```

手动下载（天池 / 飞桨）：

1. 打开 https://tianchi.aliyun.com/dataset/649 → 阿里云账号登录 → 下载 `UserBehavior.csv.zip`(905.80 MB) → 解压至 `data/raw/real/`
2. 或打开 https://aistudio.baidu.com/datasetdetail/316201/0 → 飞桨账号登录 → 下载 `UserBehavior.csv`(3.5 GB) → 放至 `data/raw/real/`
3. 重跑 `python data/build_real_ods.py` 即可自动纳入融合流程

---

## 六、参考文献（可直接引用）

### 6.1 中文参考文献（GB/T 7714-2015 格式）

```
[1] NI J, LI J, MC AULEY J. Amazon review data (2018)[DB/OL]. San Diego: UCSD McAuley Lab, 2019
    [2026-10-09]. https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/.
[2] NI J, LI J, MC AULEY J. 基于远程标注评论与细粒度方面的推荐解释[C]//2019年自然语言处理经验方法会议
    暨第9届国际自然语言处理联合会议论文集(EMNLP-IJCNLP). 中国香港: ACL, 2019: 188-197.
[3] ROGATE16. Amazon reviews 2018 (full dataset)[DB/OL]. Kaggle, 2020[2026-10-09].
    https://www.kaggle.com/datasets/rogate16/amazon-reviews-2018-full-dataset.
[4] 阿里巴巴集团. 淘宝用户购物行为数据集UserBehavior[DB/OL]. 阿里云天池, 2018[2026-10-09].
    https://tianchi.aliyun.com/dataset/649.
[5] 朱寒, 李翔, 张鹏业, 等. 面向推荐系统的树型深度模型学习[C]//第24届ACM SIGKDD知识发现与数据挖掘
    会议论文集. 伦敦: ACM, 2018: 1079-1088.
[6] 朱寒, 陈大庆, 徐子如, 等. 树型索引与深度模型的联合优化[C]//神经信息处理系统进展(NeurIPS).
    温哥华: Curran Associates, 2019: 3971-3980.
[7] 朱经纬, 徐子如, 段伟, 等. 束搜索下的最优树模型学习[C]//第37届国际机器学习会议(ICML).
    PMLR, 2020: 11650-11660.
[8] CRAWL FEEDS. Amazon pet supplies data[DB/OL]. Kaggle, 2022[2026-10-09].
    https://www.kaggle.com/datasets/crawlfeeds/amazon-pet-supplies-data.
[9] CRAWL FEEDS. Chewy products dataset[DB/OL]. Kaggle, 2023[2026-10-09].
    https://www.kaggle.com/datasets/crawlfeeds/chewy-data.
[10] 百度飞桨. 电商用户购物行为数据集[DB/OL]. 飞桨AI Studio, 2025[2026-10-09].
     https://aistudio.baidu.com/datasetdetail/316201/0.
```

### 6.2 英文参考文献（APA 7th / BibTeX）

**APA 7th**

```
Crawl Feeds. (2022). Amazon pet supplies data [Data set]. Kaggle.
    https://www.kaggle.com/datasets/crawlfeeds/amazon-pet-supplies-data
Crawl Feeds. (2023). Chewy products dataset [Data set]. Kaggle.
    https://www.kaggle.com/datasets/crawlfeeds/chewy-data
Alibaba Group. (2018). User behavior data from Taobao for recommendation [Data set]. Tianchi.
    https://tianchi.aliyun.com/dataset/649
Ni, J., Li, J., & McAuley, J. (2019). Amazon review data (2018) [Data set]. UCSD McAuley Lab.
    https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/
Ni, J., Li, J., & McAuley, J. (2019). Justifying recommendations using distantly-labeled reviews
    and fine-grained aspects. In Proceedings of the 2019 Conference on Empirical Methods in
    Natural Language Processing (EMNLP-IJCNLP) (pp. 188–197). Association for Computational Linguistics.
rogate16. (2020). Amazon reviews 2018 (full dataset) [Data set]. Kaggle.
    https://www.kaggle.com/datasets/rogate16/amazon-reviews-2018-full-dataset
Zhu, H., Li, X., Zhang, P., Li, G., He, J., Li, H., & Gai, K. (2018). Learning tree-based deep model
    for recommender systems. In Proceedings of the 24th ACM SIGKDD International Conference on
    Knowledge Discovery & Data Mining (pp. 1079–1088). ACM.
Zhu, H., Chen, D., Xu, Z., et al. (2019). Joint optimization of tree-based index and deep model for
    recommender systems. Advances in Neural Information Processing Systems, 32, 3971–3980.
Zhu, J., Xu, Z., Dai, W., et al. (2020). Learning optimal tree models under beam search.
    In Proceedings of the 37th International Conference on Machine Learning (pp. 11650–11660). PMLR.
```

**BibTeX**

```bibtex
@dataset{ni2019amazon,
  author    = {Ni, Jianmo and Li, Jiacheng and McAuley, Julian},
  title     = {Amazon Review Data (2018)},
  year      = {2019},
  publisher = {UCSD McAuley Lab},
  url       = {https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/}
}

@inproceedings{ni2019justifying,
  author    = {Ni, Jianmo and Li, Jiacheng and McAuley, Julian},
  title     = {Justifying Recommendations using Distantly-Labeled Reviews and Fine-Grained Aspects},
  booktitle = {Proceedings of the 2019 Conference on Empirical Methods in Natural Language Processing
               and the 9th International Joint Conference on Natural Language Processing (EMNLP-IJCNLP)},
  pages     = {188--197},
  year      = {2019},
  publisher = {Association for Computational Linguistics}
}

@dataset{alibaba2018userbehavior,
  author    = {Alibaba Group},
  title     = {User Behavior Data from Taobao for Recommendation},
  year      = {2018},
  publisher = {Tianchi},
  note      = {Licensed under CC BY-NC-SA 4.0},
  url       = {https://tianchi.aliyun.com/dataset/649}
}

@inproceedings{zhu2018tdm,
  author    = {Zhu, Han and Li, Xiang and Zhang, Pengye and Li, Guozheng and He, Jie and
               Li, Han and Gai, Kun},
  title     = {Learning Tree-based Deep Model for Recommender Systems},
  booktitle = {Proceedings of the 24th ACM SIGKDD International Conference on
               Knowledge Discovery and Data Mining (KDD)},
  pages     = {1079--1088},
  year      = {2018}
}

@inproceedings{zhu2019jtm,
  author    = {Zhu, Han and Chen, Daqing and Xu, Ziru and others},
  title     = {Joint Optimization of Tree-based Index and Deep Model for Recommender Systems},
  booktitle = {Advances in Neural Information Processing Systems (NeurIPS)},
  volume    = {32},
  pages     = {3971--3980},
  year      = {2019}
}

@inproceedings{zhu2020otm,
  author    = {Zhu, Jingwei and Xu, Ziru and Dai, Wei and others},
  title     = {Learning Optimal Tree Models under Beam Search},
  booktitle = {Proceedings of the 37th International Conference on Machine Learning (ICML)},
  pages     = {11650--11660},
  year      = {2020}
}

@dataset{crawlfeeds2022amazonpet,
  author    = {Crawl Feeds},
  title     = {Amazon Pet Supplies Data},
  year      = {2022},
  publisher = {Kaggle},
  note      = {CC0: Public Domain},
  url       = {https://www.kaggle.com/datasets/crawlfeeds/amazon-pet-supplies-data}
}

@dataset{crawlfeeds2023chewy,
  author    = {Crawl Feeds},
  title     = {Chewy Products Dataset},
  year      = {2023},
  publisher = {Kaggle},
  note      = {CC0: Public Domain},
  url       = {https://www.kaggle.com/datasets/crawlfeeds/chewy-data}
}
```

---

## 七、授权许可结论（可否用于学术研究）

| 数据集 | 许可 | 学术研究 | 需引用 | 商业使用 | 备注 |
|---|---|---|---|---|---|
| K1 Amazon Reviews 2018 | Other（Ni et al. 声明） | ✅ 允许 | ✅ 必须引用 Ni et al. 2019 | ⚠️ 发布方声明仅限研究目的 | 需在论文"数据来源"章节显式声明 |
| K2 Amazon Pet Supplies | **CC0** | ✅ | 建议引用 | ✅ | 公共领域 |
| K3 Chewy Products | **CC0** | ✅ | 建议引用 | ✅ | 公共领域 |
| T1/T2 天池 UserBehavior | **CC BY-NC-SA 4.0** | ✅（非商业） | ✅ 必须署名 + 引用官方 3 篇论文 | ❌ 禁止 | 学位论文属非商业用途；若成果后续商用需单独授权 |
| A1 飞桨电商行为数据集 | **CC0 1.0** | ✅ | 建议引用 | ✅ | 使用限制最少的国内镜像渠道 |

**结论**：上述数据集均可用于本毕业论文的学术研究，且均可在论文中合法引用。
唯一需注意的约束是天池 UserBehavior 的 **NC（非商业）** 条款——学位论文属于非商业用途，符合许可要求，
但须在论文中明确标注来源与许可协议，且不得将基于该数据的成果直接商业化。

---

## 八、真实数据带来的方法论调整（论文需同步说明）

| 原（模拟数据） | 现（真实数据） | 论文处理 |
|---|---|---|
| 用户年龄/性别/收入/城市齐全 | 公开数据集不可用 | 画像维度改为**可推断字段**：宠物类型（购买品类众数）、消费层级（金额分位）、活跃度（订单数分档）；缺失字段在局限中说明 |
| 订单含小时级时间戳 | Amazon 源仅日期粒度 | 小时级时段分析改用淘宝行为数据（秒级时间戳）；漏斗/热力图/路径统一取自行为表 |
| 全链路单一数据源 | 四源异构融合 | 强化"多源数据融合"章节，说明实体空间差异与松耦合引用设计 |
| 促销/折扣/支付方式字段 | 不可用 | 相关分析（促销效果、支付偏好）从论文中移除或改为说明性描述 |
| 数据时间跨度 2025-07~2026-06 | 2017-07 ~ 2018-10 | 论文中如实标注数据时间窗口，并说明对结论时效性的影响 |
| 金额单位 ¥ | 原始 USD/INR，折算 CNY | 明确标注汇率（1 USD≈7.10 CNY，1 INR≈0.085 CNY），仅用于统一量纲 |

---

## 九、风险提示

1. **跨源实体不统一**：Amazon 商品域与淘宝行为域无公共主键，二者不可做实体级联合分析，只能在**方法层面**融合（商品/推荐用 Amazon，行为序列用淘宝）。论文必须明确说明，避免评审质疑。
2. **镜像数据字段缺失**：rogate16 镜像丢失 `reviewerID`/`asin`，主键由哈希重构，若评审追问，需说明这是镜像局限而非数据造假，并可补充从 UCSD 官方源（含完整字段）重新获取的方案。
3. **时效性**：数据窗口为 2017–2018 年，结论的时效性需在论文"局限性"中声明。
4. **抽样代表性**：淘宝数据按 `user_id % 40` 系统抽样，属于等概率抽样，需在论文中说明抽样方法与样本量。
