import pandas as pd
import numpy as np
from geopy.distance import geodesic
from datetime import timedelta

# ==========================================
# 1. 全局参数配置
# ==========================================
RIDER_SPEED_KMH = 9.31  # 骑手平均速度 km/h
DRONE_SMALL_SPEED_KMH = 60  # 小无人机速度
DRONE_BIG_SPEED_KMH = 80  # 大无人机速度
RIDER_FOLD_RATE = 1.4387  # 骑手路径折叠率

# 时间窗约束 (分钟)
TIME_WINDOWS = {'health': 15, 'waimai': 30, 'shangou': 45, 'pinhaofan': 45}


# ==========================================
# 2. 距离计算辅助函数
# ==========================================
def calc_dist(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).kilometers


def calc_rider_time(dist_straight):
    actual_dist = dist_straight * RIDER_FOLD_RATE
    return (actual_dist / RIDER_SPEED_KMH) * 60  # 返回分钟


def calc_drone_time(dist_straight, speed):
    return (dist_straight / speed) * 60 + 2  # 增加2分钟起降时间


# ==========================================
# 3. 数据预处理与枢纽索引构建
# ==========================================
print("正在加载数据...")
# 假设数据已清洗，时间列已转为 datetime
orders = pd.read_excel('/Users/zhangyt/Desktop/美团比赛/深圳市南山区运单抽样_清洗后.xlsx')
hubs = pd.read_csv('/Users/zhangyt/Desktop/美团比赛/南山区枢纽清单_均衡优化版_Final.csv')

# 分类枢纽
core_hubs = hubs[hubs['级别'] == '母港']
normal_hubs = hubs[hubs['级别'].isin(['一级', '二级'])]


def find_nearest_hub(lat, lon, hub_df):
    distances = hub_df.apply(lambda row: calc_dist(lat, lon, row['纬度'], row['经度']), axis=1)
    nearest_idx = distances.idxmin()
    return hub_df.loc[nearest_idx], distances.min()


# ==========================================
# 4. 订单路由与时效计算核心逻辑
# ==========================================
def simulate_order(row, big_drone_max_wait):
    p_lat, p_lon = row['pickup_latitude'], row['pickup_longitude']
    d_lat, d_lon = row['dropoff_latitude'], row['dropoff_longitude']
    biz = row['biz_line']
    time_limit = TIME_WINDOWS.get(biz, 30)

    dist_total = calc_dist(p_lat, p_lon, d_lat, d_lon)

    # 纯骑手基线 (Base VRP approximation)
    pure_rider_time = calc_rider_time(dist_total)

    # 策略分流
    strategy = "Pure_Rider"
    synergy_time = pure_rider_time

    if dist_total > 2:
        # 寻找最近的枢纽
        nearest_any_hub, dist_to_any_hub = find_nearest_hub(p_lat, p_lon, hubs)
        nearest_core_hub_pickup, dist_to_core_pickup = find_nearest_hub(p_lat, p_lon, core_hubs)
        nearest_core_hub_dropoff, dist_to_core_dropoff = find_nearest_hub(d_lat, d_lon, core_hubs)

        if 2 < dist_total <= 7:
            strategy = "Short_Air"
            rider_t = calc_rider_time(dist_to_any_hub)
            drone_dist = calc_dist(nearest_any_hub['纬度'], nearest_any_hub['经度'], d_lat, d_lon)
            drone_t = calc_drone_time(drone_dist, DRONE_SMALL_SPEED_KMH)
            synergy_time = rider_t + drone_t + 3  # 3分钟接驳换电时间

        elif dist_total > 7 and dist_to_core_pickup <= 2:
            strategy = "Long_Air_Direct"
            rider_t = calc_rider_time(dist_to_core_pickup)
            big_drone_dist = calc_dist(nearest_core_hub_pickup['纬度'], nearest_core_hub_pickup['经度'],
                                       nearest_core_hub_dropoff['纬度'], nearest_core_hub_dropoff['经度'])
            big_drone_t = calc_drone_time(big_drone_dist, DRONE_BIG_SPEED_KMH)
            small_drone_dist = calc_dist(nearest_core_hub_dropoff['纬度'], nearest_core_hub_dropoff['经度'], d_lat,
                                         d_lon)
            small_drone_t = calc_drone_time(small_drone_dist, DRONE_SMALL_SPEED_KMH)
            # 加入迭代的大飞机等待时间
            synergy_time = rider_t + big_drone_max_wait + big_drone_t + small_drone_t + 5

        elif dist_total > 8 and dist_to_core_pickup > 2:
            strategy = "Long_Air_Relay"
            rider_t = calc_rider_time(dist_to_any_hub)
            small_to_core_dist = calc_dist(nearest_any_hub['纬度'], nearest_any_hub['经度'],
                                           nearest_core_hub_pickup['纬度'], nearest_core_hub_pickup['经度'])
            small_to_core_t = calc_drone_time(small_to_core_dist, DRONE_SMALL_SPEED_KMH)
            big_drone_dist = calc_dist(nearest_core_hub_pickup['纬度'], nearest_core_hub_pickup['经度'],
                                       nearest_core_hub_dropoff['纬度'], nearest_core_hub_dropoff['经度'])
            big_drone_t = calc_drone_time(big_drone_dist, DRONE_BIG_SPEED_KMH)
            small_to_user_dist = calc_dist(nearest_core_hub_dropoff['纬度'], nearest_core_hub_dropoff['经度'], d_lat,
                                           d_lon)
            small_to_user_t = calc_drone_time(small_to_user_dist, DRONE_SMALL_SPEED_KMH)
            synergy_time = rider_t + small_to_core_t + big_drone_max_wait + big_drone_t + small_to_user_t + 8

    # 判断超时
    is_timeout = synergy_time > time_limit
    pure_rider_timeout = pure_rider_time > time_limit

    return pd.Series([strategy, pure_rider_time, synergy_time, is_timeout, pure_rider_timeout])


# ==========================================
# 5. 大无人机等待时间迭代与评估
# ==========================================
wait_time_candidates = [2, 5, 8, 12, 15]  # 候选等待时间（分钟）
best_wait_time = 0
lowest_timeout_rate = 1.0

print("开始迭代大无人机最大等待时间...")
# 为了快速迭代，可以先抽样 10% 的数据进行超参数搜寻
sample_orders = orders.sample(frac=0.1, random_state=42)

for wt in wait_time_candidates:
    print(f"正在测试大飞机等待上限: {wt} 分钟...")
    results = sample_orders.apply(lambda row: simulate_order(row, wt), axis=1)
    results.columns = ['Strategy', 'Rider_Time', 'Synergy_Time', 'Is_Timeout', 'Rider_Timeout']

    timeout_rate = results['Is_Timeout'].mean()
    print(f"测试完成，当前系统总体超时率: {timeout_rate:.2%}")

    if timeout_rate < lowest_timeout_rate:
        lowest_timeout_rate = timeout_rate
        best_wait_time = wt

print(f"\n✅ 迭代得出最优大飞机等待时间上限: {best_wait_time} 分钟")

# ==========================================
# 6. 全量数据测算与枢纽库存动态分析
# ==========================================
print("正在应用最优参数进行全量测算...")
final_results = orders.apply(lambda row: simulate_order(row, best_wait_time), axis=1)
final_results.columns = ['Strategy', 'Rider_Time', 'Synergy_Time', 'Is_Timeout', 'Rider_Timeout']

orders = pd.concat([orders, final_results], axis=1)

# 输出宏观指标
synergy_avg_time = orders['Synergy_Time'].mean()
rider_avg_time = orders['Rider_Time'].mean()
print(f"地空协同平均耗时: {synergy_avg_time:.2f} 分钟")
print(f"纯骑手模式平均耗时: {rider_avg_time:.2f} 分钟")
print(f"地空协同超时率: {orders['Is_Timeout'].mean():.2%} vs 纯骑手超时率: {orders['Rider_Timeout'].mean():.2%}")

# ... (枢纽无人机数量时间序列模拟可在进一步的循环中细化)