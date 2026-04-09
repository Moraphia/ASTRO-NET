import pandas as pd
import numpy as np
from geopy.distance import geodesic

# ==============================================
# 1. 基础配置
# ==============================================
FILE_PATH = "/Users/zhangyt/Desktop/美团比赛/目标枢纽午高峰抽样订单_200个.csv"
# 请输入你上一步标定出的折叠率（若未标定，暂时使用南山经验值 1.45）
DETOUR_FACTOR = 1.45


# ==============================================
# 2. 数据处理逻辑
# ==============================================
def calculate_rider_speed(file_path, detour_factor):
    # 读取数据
    df = pd.read_csv(file_path)

    # A. 时间转换：将字符串转为 datetime 格式
    # 确保列名与你文件中的 fetch_time 和 arrived_time 一致
    df['fetch_t'] = pd.to_datetime(df['fetch_time'])
    df['arrived_t'] = pd.to_datetime(df['arrived_time'])

    # B. 计算配送耗时（单位：小时）
    # dt.total_seconds() 转为秒，再除以 3600 转为小时
    df['delivery_hours'] = (df['arrived_t'] - df['fetch_t']).dt.total_seconds() / 3600.0

    # C. 计算路网实际距离
    # 逻辑：(起点纬度,起点经度) 到 (终度纬度,终度经度) 的直线距离 * 折叠率
    def get_real_dist(row):
        p_coord = (row['pickup_latitude'], row['pickup_longitude'])
        d_coord = (row['dropoff_latitude'], row['dropoff_longitude'])
        straight_dist = geodesic(p_coord, d_coord).km
        return straight_dist * detour_factor

    df['real_distance_km'] = df.apply(get_real_dist, axis=1)

    # D. 计算速度 (km/h) 并过滤异常数据
    # 过滤掉耗时小于等于0或速度快得离谱（如超过 60km/h）的数据
    df_clean = df[(df['delivery_hours'] > 0) & (df['delivery_hours'] < 2)].copy()
    df_clean['speed_kmh'] = df_clean['real_distance_km'] / df_clean['delivery_hours']

    # 进一步过滤极端速度值（骑手通常在 15-40km/h 之间，含红绿灯等待）
    df_final = df_clean[(df_clean['speed_kmh'] > 3) & (df_clean['speed_kmh'] < 50)]

    return df_final


# ==============================================
# 3. 执行并输出报告
# ==============================================
print("=" * 50)
print(f"正在基于折叠率 {DETOUR_FACTOR} 计算骑手平均配送速度...")
print("=" * 50)

result_df = calculate_rider_speed(FILE_PATH, DETOUR_FACTOR)

if not result_df.empty:
    avg_speed = result_df['speed_kmh'].mean()
    median_speed = result_df['speed_kmh'].median()

    print(f"有效样本量: {len(result_df)} 单")
    print(f"平均配送时速: {avg_speed:.2f} km/h")
    print(f"配送时速中位数: {median_speed:.2f} km/h")
    print("-" * 50)
    print("📈 VRP 模型参数建议：")
    print(f"建议设置骑手速度 V = {avg_speed:.2f} km/h")
    print(f"这意味着在考虑路网折叠和红绿灯后，骑手每分钟约行驶 {avg_speed / 60:.3f} km")
    print("=" * 50)
else:
    print("❌ 错误：未能计算出有效速度，请检查文件中的时间戳格式或坐标数据。")