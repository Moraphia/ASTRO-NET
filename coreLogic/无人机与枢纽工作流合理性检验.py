import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from datetime import timedelta
import math
import os

# ==========================================
# 1. 全局参数配置
# ==========================================
FILE_ORDERS = '/Users/zhangyt/Desktop/美团比赛/深圳市南山区运单抽样_清洗后.xlsx'
FILE_HUBS = '/Users/zhangyt/Desktop/美团比赛/南山区枢纽清单_均衡优化版_Final.csv'

TIME_WINDOW_REPLENISH = timedelta(hours=2)
REPLENISH_THRESHOLD = 0.3
MAX_PARCELS_PER_DRONE = 5
DRONE_LOSS_RATE = 0.05
DRONE_SPEED_KMH = 60.0  # 无人机巡航速度 60km/h
DROP_OFF_TIME_MINS = 2  # 在用户端抛投、悬停的固定耗时


# ==========================================
# 辅助函数：计算真实物理距离
# ==========================================
def haversine_distance(lat1, lon1, lat2, lon2):
    """计算两经纬度之间的球面距离（公里）"""
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c


# ==========================================
# 2. 预处理：计算 65 个枢纽容量 + 缺失诊断
# ==========================================
def calculate_precise_capacities(orders_df, hubs_df):
    print("Step 1: 正在计算各枢纽 110% 峰值容量...")
    hub_coords = hubs_df[['纬度', '经度']].values
    hub_tree = cKDTree(hub_coords)

    pickup_coords = orders_df[['pickup_latitude', 'pickup_longitude']].values
    _, nearest_idx = hub_tree.query(pickup_coords)
    orders_df['hub_id'] = hubs_df.iloc[nearest_idx]['ID'].values

    orders_df['temp_hour'] = orders_df['fetch_time'].dt.floor('h')
    hourly_drone_usage = orders_df.groupby(['hub_id', 'temp_hour']).size().reset_index(name='order_count')
    hourly_drone_usage['drone_needed'] = np.ceil(hourly_drone_usage['order_count'] / MAX_PARCELS_PER_DRONE)

    peak_usage = hourly_drone_usage.groupby('hub_id')['drone_needed'].max()

    capacities = {}
    capacity_list = []

    for _, row in hubs_df.iterrows():
        h_id = row['ID']
        h_name = row['名称']
        h_level = row['级别']

        if h_level == '母港':
            cap = 999999
        else:
            peak = peak_usage.get(h_id, 0)
            cap = int(math.ceil(peak * 1.10))
            cap = max(cap, 5)

        capacities[h_id] = cap
        capacity_list.append({
            '枢纽ID': h_id, '枢纽名称': h_name, '枢纽级别': h_level,
            '核定无人机容量上限': cap if h_level != '母港' else '无限'
        })

    df_cap_table = pd.DataFrame(capacity_list)
    df_cap_table.to_csv("南山区65个枢纽核定容量表.csv", index=False, encoding='utf-8-sig')
    return capacities, hub_tree, df_cap_table


# ==========================================
# 3. 核心重构：时间轴事件驱动仿真环境
# ==========================================
# ==========================================
# 优化后的核心仿真环境类
# ==========================================
class DroneNetworkSim:
    def __init__(self, hubs_df, capacities, hub_tree):
        self.hubs_df = hubs_df.set_index('ID')
        self.hub_tree = hub_tree
        self.capacities = capacities
        self.current_inventory = capacities.copy()

        self.in_flight_drones = []
        self.drone_logs = []
        self.dispatch_logs = []
        self.drone_counter = 1000

        # --- 新增：补给冷却计时器 (记录每个枢纽上次补给的时间) ---
        self.last_replenish_time = {hid: None for hid in capacities.keys()}
        self.REPLENISH_COOLDOWN = timedelta(minutes=15)  # 15分钟冷静期

    def get_incoming_count(self, hub_id):
        """核心改进：计算正在飞往该枢纽的‘在途物资’数量"""
        # 在真实场景中，系统知道哪些飞机预计会降落在本站，不应重复补给
        return sum(1 for d in self.in_flight_drones if d.get('target_land_hub') == hub_id)

    def get_nearest_available_hub(self, lat, lon):
        distances, indices = self.hub_tree.query([lat, lon], k=len(self.hubs_df))
        for idx in indices:
            hub_id = self.hubs_df.iloc[idx].name
            if self.current_inventory[hub_id] < self.capacities[hub_id]:
                return hub_id
        return None

    def advance_time(self, current_time):
        still_flying = []
        for drone in self.in_flight_drones:
            if current_time >= drone['arrival_time']:
                status = "正常回流"
                land_hub_id = self.get_nearest_available_hub(drone['dest_lat'], drone['dest_lon'])

                if np.random.rand() <= DRONE_LOSS_RATE:
                    status = "损耗/炸机"
                    land_hub_id = None
                elif land_hub_id:
                    self.current_inventory[land_hub_id] += 1
                else:
                    status = "全网满载(异常)"

                self.drone_logs.append({
                    '无人机ID': drone['id'], '起飞时间': drone['depart_time'],
                    '出发枢纽': drone['start_hub'], '实际降落时间': current_time,
                    '降落枢纽': land_hub_id, '最终状态': status
                })
            else:
                still_flying.append(drone)
        self.in_flight_drones = still_flying

    def process_batch(self, current_time, hub_id, order_ids, dest_lat, dest_lon):
        num_drones = math.ceil(len(order_ids) / MAX_PARCELS_PER_DRONE)
        is_peak = 11 <= current_time.hour < 14

        # --- 优化点 1：引入“逻辑库存” (地面库存 + 预计半小时内到达的在途物资) ---
        incoming = self.get_incoming_count(hub_id)
        logical_inventory = self.current_inventory[hub_id] + incoming

        # --- 优化点 2：冷却时间判断 ---
        last_time = self.last_replenish_time[hub_id]
        in_cooldown = (last_time is not None) and (current_time - last_time < self.REPLENISH_COOLDOWN)

        # 触发紧急补给的条件：物理库存真的见底了，且不在冷静期内
        if self.current_inventory[hub_id] <= 0 and not in_cooldown:
            # 35号枢纽在高峰期一次补给20架，非高峰期5架
            refill_amount = 20 if (is_peak and hub_id == 35) else 5

            # 如果加上在途物资后逻辑库存已经很充裕了，就没必要补那么多
            if logical_inventory > self.capacities[hub_id] * 0.8:
                refill_amount = 2  # 象征性微调，防止彻底瘫痪

            self.current_inventory[hub_id] += refill_amount
            self.last_replenish_time[hub_id] = current_time  # 启动冷静期

            self.dispatch_logs.append({
                '时间': current_time, '目标枢纽ID': hub_id,
                '原因': f'高峰批量补给(逻辑库存:{logical_inventory})',
                '补给数量': refill_amount
            })

        # 执行起飞
        for _ in range(num_drones):
            # 即使触发了补给，如果刚好那秒还是没飞机，就只能强制给1架保底
            if self.current_inventory[hub_id] <= 0:
                self.current_inventory[hub_id] += 1

            self.current_inventory[hub_id] -= 1

            # 预估降落点（为了计算在途物资，我们需要先预判它大概落在哪）
            # 这一步是模拟系统的预测能力
            target_land_hub = self.get_nearest_available_hub(dest_lat, dest_lon)

            dist_km = haversine_distance(self.hubs_df.loc[hub_id, '纬度'], self.hubs_df.loc[hub_id, '经度'], dest_lat,
                                         dest_lon)
            flight_mins = (dist_km / DRONE_SPEED_KMH * 60) + DROP_OFF_TIME_MINS
            arrival_time = current_time + timedelta(minutes=math.ceil(flight_mins))

            self.in_flight_drones.append({
                'id': f"DRN_{self.drone_counter}",
                'depart_time': current_time,
                'arrival_time': arrival_time,
                'start_hub': hub_id,
                'target_land_hub': target_land_hub,  # 记录预判降落点
                'dest_lat': dest_lat,
                'dest_lon': dest_lon
            })
            self.drone_counter += 1

    def scheduled_replenish(self, current_time):
        """周期性检查也引入冷静期和逻辑库存"""
        for hub_id, cap in self.capacities.items():
            if cap > 10000: continue

            # 高峰期前的特殊逻辑
            is_peak_prepare = (current_time.hour == 10 and current_time.minute >= 45)

            # 计算逻辑库存
            incoming = self.get_incoming_count(hub_id)
            logical_ratio = (self.current_inventory[hub_id] + incoming) / cap

            last_time = self.last_replenish_time[hub_id]
            in_cooldown = (last_time is not None) and (current_time - last_time < self.REPLENISH_COOLDOWN)

            if is_peak_prepare and hub_id == 35 and self.current_inventory[hub_id] < cap:
                # 10:45 强制给35号填满，准备战斗，不受冷静期限制
                add_amount = cap - self.current_inventory[hub_id]
                self.current_inventory[hub_id] = cap
                self.dispatch_logs.append({
                    '时间': current_time, '目标枢纽ID': hub_id, '原因': '战前整备', '补给数量': add_amount
                })
                self.last_replenish_time[hub_id] = current_time

            elif logical_ratio < REPLENISH_THRESHOLD and not in_cooldown:
                # 只有逻辑库存也低，且不在冷静期，才触发母港补给
                add_amount = cap - self.current_inventory[hub_id]
                self.current_inventory[hub_id] = cap
                self.dispatch_logs.append({
                    '时间': current_time, '目标枢纽ID': hub_id, '原因': '水位预警(含在途判定)', '补给数量': add_amount
                })
                self.last_replenish_time[hub_id] = current_time
# ==========================================
# 4. 执行全量仿真
# ==========================================
def main():
    print("Step 0: 读取数据并清洗...")
    all_orders = pd.read_excel(FILE_ORDERS)
    hubs_df = pd.read_csv(FILE_HUBS)
    all_orders['fetch_time'] = pd.to_datetime(all_orders['fetch_time'])

    capacities, hub_tree, _ = calculate_precise_capacities(all_orders, hubs_df)

    all_orders['window'] = all_orders['fetch_time'].dt.floor('3min')
    all_orders = all_orders.sort_values(by='fetch_time')

    p_coords = all_orders[['pickup_latitude', 'pickup_longitude']].values
    _, idxs = hub_tree.query(p_coords)
    all_orders['hub_id'] = hubs_df.iloc[idxs]['ID'].values

    print(f"Step 2: 正在执行真实物理耗时仿真 (包含空中姿态与收货地解算)...")
    sim = DroneNetworkSim(hubs_df, capacities, hub_tree)
    grouped = all_orders.groupby('window')
    last_check = all_orders['fetch_time'].min()

    # 按真实时间轴推进
    for t_window, group in grouped:
        # 1. 先处理到达时间（结算天上飞的无人机）
        sim.advance_time(t_window)

        # 2. 检查周期补给
        if t_window - last_check >= TIME_WINDOW_REPLENISH:
            sim.scheduled_replenish(t_window)
            last_check = t_window

        # 3. 处理本时间窗的新订单发射
        sub_groups = group.groupby('hub_id')
        for h_id, sub_group in sub_groups:
            # 尝试获取真实的用户收货坐标（如果有的话）
            if 'delivery_latitude' in sub_group.columns:
                dest_lat = sub_group.iloc[-1]['delivery_latitude']
                dest_lon = sub_group.iloc[-1]['delivery_longitude']
            else:
                # 【核心】：如果没有收货坐标，模拟产生一个 1-3公里外的真实收货点
                # 防止无人机又在原点找枢纽
                origin_lat = sub_group.iloc[-1]['pickup_latitude']
                origin_lon = sub_group.iloc[-1]['pickup_longitude']
                dest_lat = origin_lat + np.random.uniform(-0.02, 0.02)
                dest_lon = origin_lon + np.random.uniform(-0.02, 0.02)

            sim.process_batch(t_window, h_id, sub_group.index.tolist(), dest_lat, dest_lon)

    print("Step 3: 导出最终日志...")
    # 清理最后还在天上没落地的飞机
    sim.advance_time(all_orders['fetch_time'].max() + timedelta(hours=2))

    pd.DataFrame(sim.drone_logs).to_csv("全量数据_真实物理耗时_无人机工作日志.csv", index=False, encoding='utf-8-sig')
    pd.DataFrame(sim.dispatch_logs).to_csv("全量数据_真实物理耗时_母港调度日志.csv", index=False, encoding='utf-8-sig')

    print("\n" + "=" * 40)
    print(f"🏆 真实物理仿真完成！去除了瞬间移动逻辑。")
    print(f"请检查新的 无人机工作日志.csv，你将看到清晰的起飞和降落时间差，以及真实的跨区转移情况。")
    print("=" * 40)


if __name__ == "__main__":
    main()