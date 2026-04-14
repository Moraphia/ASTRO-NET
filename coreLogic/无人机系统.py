import pandas as pd
import numpy as np
import math

# ==========================================
# 1. 全局业务参数配置
# ==========================================
FILE_HUBS = '/Users/zhangyt/Desktop/美团比赛/南山区枢纽清单_均衡优化版_Final.csv'
FILE_ORDERS = '/Users/zhangyt/Desktop/美团比赛/测试数据.csv'

SPEED_RIDER_KMH = 7.40
SPEED_DRONE_SMALL_KMH = 60.0
SPEED_DRONE_LARGE_KMH = 80.0
FOLDING_RATE = 1.4387
TRANSFER_TIME_MINS = 2.0

# VRP 与库存参数
DRONE_CAPACITY = 5  # 小无人机最大载单量
HUB_INITIAL_DRONES = 10  # 每个非母港枢纽的初始小无人机数量
HUB_MAX_CAPACITY = 15  # [新增] 枢纽最大停机位容量，用于判定是否可降落
HUB_CRITICAL_THRESHOLD = 2  # 触发母港配给的临界点

SLA_DICT = {'health': 15, 'waimai': 30, 'pinhaofan': 45, 'shangou': 45}

# 全局状态跟踪
hub_inventory = {}
rebalance_counter = 0


# ==========================================
# 2. 辅助计算函数
# ==========================================
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


def get_nearest_node(lat, lon, nodes_df):
    """寻找绝对距离最近的节点（不考虑容量）"""
    distances = nodes_df.apply(lambda row: haversine_distance(lat, lon, row['纬度'], row['经度']), axis=1)
    min_idx = distances.idxmin()
    nearest_node = nodes_df.loc[min_idx]
    return nearest_node['ID'], nearest_node['名称'], nearest_node['纬度'], nearest_node['经度'], distances.min()


def get_nearest_available_hub(lat, lon, normal_hubs_df):
    """
    [核心回归] 寻找距离(lat, lon)最近，且具有空余停机位的枢纽
    """
    distances = normal_hubs_df.apply(lambda row: haversine_distance(lat, lon, row['纬度'], row['经度']), axis=1)
    sorted_hubs = normal_hubs_df.loc[distances.sort_values().index]

    for _, row in sorted_hubs.iterrows():
        hid = row['ID']
        # 检查是否还有停机位
        if hub_inventory.get(hid, 0) < HUB_MAX_CAPACITY:
            return hid, row['名称'], row['纬度'], row['经度'], distances[row.name]

    # 如果全网瘫痪都满了（极端兜底逻辑），强行降落在最近的
    fallback = sorted_hubs.iloc[0]
    return fallback['ID'], fallback['名称'], fallback['纬度'], fallback['经度'], distances[fallback.name]


# ==========================================
# 3. 核心路由决策引擎
# ==========================================
def get_order_route(order, hubs_df):
    motherports = hubs_df[hubs_df['级别'] == '母港']
    normal_hubs = hubs_df[hubs_df['级别'] != '母港']

    pick_lat, pick_lon = order['pickup_latitude'], order['pickup_longitude']
    del_lat, del_lon = order['dropoff_latitude'], order['dropoff_longitude']

    D_total = haversine_distance(pick_lat, pick_lon, del_lat, del_lon)

    if D_total < 2.0:
        return None

    s_hub_id, s_hub_name, s_hub_lat, s_hub_lon, dist_to_s_hub = get_nearest_node(pick_lat, pick_lon, normal_hubs)
    s_mp_id, s_mp_name, s_mp_lat, s_mp_lon, dist_to_s_mp = get_nearest_node(pick_lat, pick_lon, motherports)
    e_mp_id, e_mp_name, e_mp_lat, e_mp_lon, dist_to_e_mp = get_nearest_node(del_lat, del_lon, motherports)

    order_info = order.to_dict()

    # 逻辑 B：>= 8km 且 商家距离最近母港 > 2km -> 二级喂单 + 母港干线
    if D_total >= 8.0 and dist_to_s_mp > 2.0:
        t_rider = (dist_to_s_hub * FOLDING_RATE / SPEED_RIDER_KMH) * 60
        t_feed = (haversine_distance(s_hub_lat, s_hub_lon, s_mp_lat, s_mp_lon) / SPEED_DRONE_SMALL_KMH) * 60
        t_main = (haversine_distance(s_mp_lat, s_mp_lon, e_mp_lat, e_mp_lon) / SPEED_DRONE_LARGE_KMH) * 60

        arrival = t_rider + TRANSFER_TIME_MINS + t_feed + TRANSFER_TIME_MINS + t_main + TRANSFER_TIME_MINS

        order_info.update({
            'final_node_id': e_mp_id, 'final_node_name': e_mp_name, 'final_node_lat': e_mp_lat,
            'final_node_lon': e_mp_lon,
            'arrival_time': arrival,
            'chain': f"骑手->{s_hub_name}->小机(喂单)->{s_mp_name}->大机干线->{e_mp_name}"
        })

    # 逻辑 C：>= 7km -> 骑手直送母港 + 母港干线
    elif D_total >= 7.0:
        t_rider = (dist_to_s_mp * FOLDING_RATE / SPEED_RIDER_KMH) * 60
        t_main = (haversine_distance(s_mp_lat, s_mp_lon, e_mp_lat, e_mp_lon) / SPEED_DRONE_LARGE_KMH) * 60

        arrival = t_rider + TRANSFER_TIME_MINS + t_main + TRANSFER_TIME_MINS

        order_info.update({
            'final_node_id': e_mp_id, 'final_node_name': e_mp_name, 'final_node_lat': e_mp_lat,
            'final_node_lon': e_mp_lon,
            'arrival_time': arrival,
            'chain': f"骑手->{s_mp_name}->大机干线->{e_mp_name}"
        })

    # 逻辑 D：2km <= D < 7km -> 标准二级枢纽支线
    else:
        t_rider = (dist_to_s_hub * FOLDING_RATE / SPEED_RIDER_KMH) * 60
        arrival = t_rider + TRANSFER_TIME_MINS

        order_info.update({
            'final_node_id': s_hub_id, 'final_node_name': s_hub_name, 'final_node_lat': s_hub_lat,
            'final_node_lon': s_hub_lon,
            'arrival_time': arrival,
            'chain': f"骑手->{s_hub_name}"
        })

    return order_info


# ==========================================
# 4. VRP 求解器与动态降落
# ==========================================
def solve_vrp_batch(start_node_name, start_node_lat, start_node_lon, batch_orders, normal_hubs_df):
    start_time = max([o['arrival_time'] for o in batch_orders]) + TRANSFER_TIME_MINS

    current_lat, current_lon = start_node_lat, start_node_lon
    current_time = start_time
    unvisited = batch_orders.copy()

    path_nodes = [start_node_name]
    processed = []

    # --- 阶段 1：派送循环 ---
    while unvisited:
        best_idx = min(range(len(unvisited)), key=lambda i: haversine_distance(
            current_lat, current_lon, unvisited[i]['dropoff_latitude'], unvisited[i]['dropoff_longitude']))

        target = unvisited.pop(best_idx)
        dist = haversine_distance(current_lat, current_lon, target['dropoff_latitude'], target['dropoff_longitude'])

        current_time += (dist / SPEED_DRONE_SMALL_KMH) * 60
        target['final_arrival_mins'] = current_time
        current_time += 1.0  # 悬停抛投耗时

        oid_str = str(target.get('order_seq_id', '0000'))
        path_nodes.append(f"顾客({oid_str[-4:]})")

        current_lat, current_lon = target['dropoff_latitude'], target['dropoff_longitude']
        processed.append(target)

    # --- 阶段 2：[核心补全] 派送完毕，就近寻找可用枢纽降落 ---
    # 此时的 current_lat, current_lon 是最后一个顾客的坐标
    land_id, land_name, land_lat, land_lon, land_dist = get_nearest_available_hub(current_lat, current_lon,
                                                                                  normal_hubs_df)

    # 计算返航降落耗时 (虽然不计入顾客 SLA，但消耗了无人机占用时间)
    current_time += (land_dist / SPEED_DRONE_SMALL_KMH) * 60
    current_time += TRANSFER_TIME_MINS

    path_nodes.append(f"{land_name}(降落)")
    full_vrp_str = " -> ".join(path_nodes)

    # [资产回流] 降落点枢纽无人机库存 +1
    if land_id in hub_inventory:
        hub_inventory[land_id] += 1

    for p in processed:
        p['vrp_path'] = full_vrp_str

    return processed


# ==========================================
# 5. 主程序：仿真与统计
# ==========================================
def run_simulation():
    print("🚀 系统初始化...")
    hubs_df = pd.read_csv(FILE_HUBS)
    try:
        orders_df = pd.read_csv(FILE_ORDERS, encoding='gbk')
    except:
        orders_df = pd.read_csv(FILE_ORDERS, encoding='utf-8-sig')

    orders_df.columns = [c.strip() for c in orders_df.columns]

    global hub_inventory, rebalance_counter
    normal_hubs = hubs_df[hubs_df['级别'] != '母港']
    # 初始化库存
    hub_inventory = {row['ID']: HUB_INITIAL_DRONES for _, row in normal_hubs.iterrows()}
    rebalance_counter = 0

    valid_list = []
    for _, row in orders_df.iterrows():
        route = get_order_route(row, hubs_df)
        if route: valid_list.append(route)

    if not valid_list:
        print("❌ 无满足条件的订单。")
        return

    df_step1 = pd.DataFrame(valid_list)
    final_output = []
    total_flights = 0

    for node_id, group in df_step1.groupby('final_node_id'):
        node_info = group.iloc[0]
        sorted_orders = group.sort_values('arrival_time').to_dict('records')

        for i in range(0, len(sorted_orders), DRONE_CAPACITY):
            batch = sorted_orders[i: i + DRONE_CAPACITY]

            # [资产流出] 起飞点扣减库存
            if node_id in hub_inventory:
                hub_inventory[node_id] -= 1
                # 检查并触发母港补给
                if hub_inventory[node_id] <= HUB_CRITICAL_THRESHOLD:
                    hub_inventory[node_id] += 5
                    rebalance_counter += 1

            total_flights += 1
            # 传入 normal_hubs 用于降落点寻址
            processed_batch = solve_vrp_batch(
                node_info['final_node_name'],
                node_info['final_node_lat'],
                node_info['final_node_lon'],
                batch,
                normal_hubs
            )
            final_output.extend(processed_batch)

    df_final = pd.DataFrame(final_output)
    df_final['SLA'] = df_final['biz_line'].apply(lambda x: SLA_DICT.get(str(x).lower(), 30))
    df_final['是否超时'] = df_final.apply(lambda r: '超时 ❌' if r['final_arrival_mins'] > r['SLA'] else '达标 ✅',
                                          axis=1)

    report = df_final[['order_seq_id', 'biz_line', 'SLA', 'final_arrival_mins', '是否超时', 'chain', 'vrp_path']].copy()
    report.columns = ['订单号', '业务', 'SLA(分)', '总耗时(分)', '结果', '前置链路', 'VRP配送路径']

    report[''] = ''
    report['VRP系统全局数据面板'] = ''
    report.at[0, 'VRP系统全局数据面板'] = f"测试集总单量: {len(orders_df)}"
    report.at[1, 'VRP系统全局数据面板'] = f"无人机承接单量: {len(df_final)}"
    report.at[2, 'VRP系统全局数据面板'] = f"综合超时率: {(df_final['是否超时'] == '超时 ❌').mean():.2%}"
    report.at[3, 'VRP系统全局数据面板'] = f"总发车次数(VRP): {total_flights} 次"
    report.at[4, 'VRP系统全局数据面板'] = f"母港补给触发次数: {rebalance_counter} 次"

    output_name = "无人机VRP库管系统分析.csv"
    report.to_csv(output_name, index=False, encoding='utf-8-sig')
    print(f"✅ 成功！文件已保存至：{output_name}")


if __name__ == "__main__":
    run_simulation()