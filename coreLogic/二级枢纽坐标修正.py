import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial import cKDTree
import os

# ==========================================
# 1. 路径配置（请根据实际情况微调）
# ==========================================
FILE_ORDERS = '/Users/zhangyt/Desktop/美团比赛/深圳市南山区运单抽样_清洗后.xlsx'
FILE_HUBS_OLD = '/Users/zhangyt/Desktop/美团比赛/南山区全级次枢纽清单_大学城补强版.csv'


def rebalance_secondary_hubs_final():
    print("Step 1: 正在加载数据...")
    df_orders = pd.read_excel(FILE_ORDERS)
    df_hubs = pd.read_csv(FILE_HUBS_OLD)

    # 自动识别列名（适配“级别”或“枢纽级别”）
    level_col = '级别' if '级别' in df_hubs.columns else '枢纽级别'
    name_col = '名称' if '名称' in df_hubs.columns else '枢纽名称'

    print(f"检测到列名: [{level_col}], 正在解析枢纽身份...")

    # --- 身份识别逻辑 ---
    # 只要包含“母港”或“一级”的都视为固定枢纽
    fixed_mask = df_hubs[level_col].str.contains('母港|一级', na=False)
    # 只要包含“二级”的都视为待调整枢纽
    secondary_mask = df_hubs[level_col].str.contains('二级', na=False)

    fixed_hubs = df_hubs[fixed_mask].copy()
    secondary_hubs = df_hubs[secondary_mask].copy()
    num_secondary = len(secondary_hubs)

    if num_secondary == 0:
        unique_levels = df_hubs[level_col].unique()
        print(f"❌ 错误：未找到二级枢纽！当前列中的值有：{unique_levels}")
        return

    print(f"✅ 识别成功：固定枢纽 {len(fixed_hubs)} 个，二级枢纽 {num_secondary} 个。")

    # --- Step 2: 空间排他（绕开母港和一级枢纽） ---
    print("Step 2: 正在执行空间排他，确保二级枢纽不与一级/母港重叠...")
    fixed_coords = fixed_hubs[['纬度', '经度']].values
    order_coords_all = df_orders[['pickup_latitude', 'pickup_longitude']].values

    # 构建空间树，计算订单到最近固定枢纽的距离
    fixed_tree = cKDTree(fixed_coords)
    dist, _ = fixed_tree.query(order_coords_all, k=1)

    # 排除掉距离一级枢纽/母港太近（约 800米，经纬度 0.008）的订单
    # 剩下的就是“服务空白区”的订单
    exclusion_radius = 0.008
    residual_orders = order_coords_all[dist > exclusion_radius]

    # 如果空白区订单太少，自动缩小范围（保底逻辑）
    if len(residual_orders) < num_secondary * 10:
        print("💡 空白区订单过少，正在自动优化排他半径...")
        residual_orders = order_coords_all[dist > np.percentile(dist, 15)]

    # --- Step 3: 在空白区进行聚类 ---
    print(f"Step 3: 正在残差区聚类生成 {num_secondary} 个新中心点...")
    # 随机采样以提升计算效率
    sample_size = min(len(residual_orders), 100000)
    cluster_data = residual_orders[np.random.choice(len(residual_orders), sample_size, replace=False)]

    kmeans = KMeans(n_clusters=num_secondary, n_init=15, random_state=42)
    new_centroids = kmeans.fit(cluster_data).cluster_centers_

    # --- Step 4: 整合并输出 ---
    # 为了美观，按纬度对新的二级枢纽进行排序
    new_centroids = new_centroids[new_centroids[:, 0].argsort()]

    secondary_hubs['纬度'] = new_centroids[:, 0]
    secondary_hubs['经度'] = new_centroids[:, 1]
    # 保持名称整洁
    secondary_hubs[name_col] = [f"均衡区二级枢纽_{i + 1:02d}" for i in range(num_secondary)]

    # 合并固定枢纽和新的二级枢纽
    df_final = pd.concat([fixed_hubs, secondary_hubs], ignore_index=True)

    # 严格按照图片格式排序和筛选列
    output_cols = ['ID', '名称', '级别', '纬度', '经度']
    # 如果原始列名是“枢纽名称”等，这里做个映射
    df_final = df_final.rename(columns={name_col: '名称', level_col: '级别'})
    df_final = df_final[output_cols].sort_values(by='ID')

    output_path = "/Users/zhangyt/Desktop/美团比赛/南山区枢纽清单_均衡优化版_Final.csv"
    df_final.to_csv(output_path, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 30)
    print(f"🏆 处理完成！")
    print(f"总枢纽数: {len(df_final)}")
    print(f"输出路径: {output_path}")
    print("=" * 30)


if __name__ == "__main__":
    rebalance_secondary_hubs_final()