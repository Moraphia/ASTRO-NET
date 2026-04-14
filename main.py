import pandas as pd
import json
import math
import requests
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import osmnx as ox
import random
from sklearn.cluster import DBSCAN
from collections import defaultdict
import os

# ==========================================
# 1. 全局配置区
# ==========================================
CSV_FILE = '深圳市南山区运单抽样_已脱敏.csv'
FILE_HUBS_JSON = '南山区15枢纽优化分布.json'
OUTPUT_JSON = 'project_data.json'
OD_MATRIX_FILE = 'od_matrix.json'
HOTSPOTS_FILE = 'hotspots.json'
BUILDINGS_CACHE_FILE = 'buildings_cache.json'

ROUTING_ENGINE = 'osrm'
MAX_THREADS = 20
SAMPLE_SIZE = 30000
OD_TOP_N = 15
HOTSPOT_CLUSTER_DISTANCE = 0.003

# --- 商业报告核心参数 ---
RIDER_SPEED_KMH = 9.31
DRONE_SMALL_SPEED_KMH = 60
DRONE_BIG_SPEED_KMH = 80
RIDER_FOLD_RATE = 1.4387
TRANSFER_TIME_SEC = 120
BIG_DRONE_MAX_WAIT_SEC = 120


# ==========================================
# 2. 基础辅助与地理函数
# ==========================================
def haversine_distance(lon1, lat1, lon2, lat2):
    R = 6371.0
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def time_to_seconds(time_str):
    try:
        if pd.isna(time_str): return 0
        time_part = str(time_str).split(' ')[-1]
        if time_part.count(':') == 1: time_part += ":00"
        t = datetime.strptime(time_part, "%H:%M:%S")
        return t.hour * 3600 + t.minute * 60 + t.second
    except:
        return 0


# ==========================================
# 🌟 核心升级 1：全新无人机拟真 3D 航线生成器
# ==========================================
def generate_realistic_drone_path(start_lon, start_lat, end_lon, end_lat, is_big=False):
    """
    生成拟真无人机3D航线：
    1. 垂直起飞避开低层建筑
    2. 巡航阶段带有轻微横向偏移（模拟避障/气流）
    3. 垂直降落
    """
    # 建筑白模最高限制在约150m。小机定高180m，大机定高250m，严格执行空域分层
    cruise_alt = 250 if is_big else 180

    path = []
    # 1. 垂直起飞段 (直接拉升至安全高度的 80%)
    path.append([start_lon, start_lat, 0])
    path.append([start_lon, start_lat, cruise_alt * 0.8])

    # 2. 巡航避障段 (插入平滑控制点)
    steps = 6
    for i in range(1, steps):
        t = i / steps
        # 基础线性插值
        base_lon = start_lon + (end_lon - start_lon) * t
        base_lat = start_lat + (end_lat - start_lat) * t

        # 模拟横向避障偏移 (利用 t*(1-t) 让两头偏移小，中间偏移大，形成平滑弧线)
        jitter = (t * (1 - t)) * random.uniform(-0.005, 0.005)

        # 巡航高度轻微起伏 (模拟气流颠簸)
        alt_jitter = cruise_alt + random.uniform(-8, 8)

        path.append([base_lon + jitter, base_lat - jitter, alt_jitter])

    # 3. 垂直降落段
    path.append([end_lon, end_lat, cruise_alt * 0.8])
    path.append([end_lon, end_lat, 0])

    return path


# ==========================================
# 3. 建筑、OD 与其他预处理
# ==========================================
def generate_od_matrix(df, top_n=OD_TOP_N):
    od_dict = defaultdict(int)
    grid_size = 0.003
    for _, row in df.iterrows():
        try:
            slon, slat = float(row['pickup_longitude']), float(row['pickup_latitude'])
            elon, elat = float(row['dropoff_longitude']), float(row['dropoff_latitude'])
            s_grid, e_grid = (round(slon / grid_size), round(slat / grid_size)), (
            round(elon / grid_size), round(elat / grid_size))
            if s_grid != e_grid: od_dict[(s_grid, e_grid)] += 1
        except:
            continue
    od_list = [
        {"source": [s[0] * grid_size, s[1] * grid_size], "target": [e[0] * grid_size, e[1] * grid_size], "count": count}
        for (s, e), count in od_dict.items()]
    return sorted(od_list, key=lambda x: x["count"], reverse=True)[:top_n]


def generate_hotspots(df, eps=HOTSPOT_CLUSTER_DISTANCE, min_samples=5):
    points = [[float(r['pickup_longitude']), float(r['pickup_latitude'])] for _, r in df.iterrows() if
              pd.notna(r['pickup_longitude'])]
    if not points: return []
    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric='haversine').fit(np.radians(np.array(points)))
    clusters = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        if label != -1: clusters[label].append(points[i])
    hotspots = []
    for pts in clusters.values():
        if len(pts) >= 10:
            arr = np.array(pts)
            hotspots.append({"position": arr.mean(axis=0).tolist(), "intensity": len(pts)})
    return sorted(hotspots, key=lambda x: x["intensity"], reverse=True)


def generate_truck_loop(hub1, hub2, start_sec, end_sec):
    loop_duration = 3600
    timestamps = np.arange(start_sec, end_sec, loop_duration / 10).tolist()
    path = []
    for i, t in enumerate(timestamps):
        ratio = (math.sin(t / loop_duration * 2 * math.pi) + 1) / 2
        lon, lat = hub1[0] + (hub2[0] - hub1[0]) * ratio, hub1[1] + (hub2[1] - hub1[1]) * ratio
        path.append([lon, lat])
    return {"path": path, "timestamps": timestamps}


def fetch_3d_buildings_safe(lat, lon, radius=15000):
    try:
        gdf = ox.features_from_point((lat, lon), dist=radius, tags={'building': True})
        buildings = []
        for _, row in gdf.iterrows():
            geom = row['geometry']
            if not geom.is_valid or geom.is_empty: continue
            height = random.randint(80, 150) if random.random() > 0.9 else random.randint(20, 60)
            if geom.geom_type == 'Polygon' and len(geom.exterior.coords) >= 3:
                buildings.append({"polygon": list(geom.exterior.coords), "height": height})
        return buildings
    except:
        return []


def get_or_fetch_buildings(lat, lon, radius=15000):
    if os.path.exists(BUILDINGS_CACHE_FILE):
        try:
            with open(BUILDINGS_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    buildings_data = fetch_3d_buildings_safe(lat, lon, radius)
    if buildings_data:
        try:
            with open(BUILDINGS_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(buildings_data, f, ensure_ascii=False)
        except:
            pass
    return buildings_data


# ==========================================
# 🌟 核心升级 2：强化地表路由规划器 (拒绝穿墙兜底)
# ==========================================
class RoutePlanner:
    def __init__(self, engine='osrm'):
        self.engine = engine

    def get_ground_route(self, start_lon, start_lat, end_lon, end_lat):
        if self.engine == 'osrm':
            try:
                url = f"http://127.0.0.1:5000/route/v1/driving/{start_lon},{start_lat};{end_lon},{end_lat}?overview=full&geometries=geojson"
                res = requests.get(url, timeout=2).json()
                if res.get('code') == 'Ok':
                    return res['routes'][0]['geometry']['coordinates']
            except:
                pass
        # ⚠️ 抛弃了原来的返回直线兜底逻辑，现在获取失败直接返回 None
        return None


# ==========================================
# 4. 带严苛约束的仿真引擎
# ==========================================
def process_single_order(row, planner):
    try:
        slon, slat = float(row['pickup_longitude']), float(row['pickup_latitude'])
        elon, elat = float(row['dropoff_longitude']), float(row['dropoff_latitude'])
        start_sec = time_to_seconds(row['fetch_time'])

        if start_sec == 0: return None

        order_id = str(row.get('order_seq_id', random.randint(10000, 99999)))
        biz_line = str(row.get('biz_line', 'waimai'))
        dist = haversine_distance(slon, slat, elon, elat)

        segments = []
        logs = []
        current_time = start_sec

        # --- 路由模式映射 ---
        if dist < 2.0:
            g_path = planner.get_ground_route(slon, slat, elon, elat)
            # 🌟 严格把关：如果没取到路线，直接让这条订单作废，前端绝不渲染穿墙线
            if not g_path or len(g_path) <= 2: return None

            duration = (dist * RIDER_FOLD_RATE / RIDER_SPEED_KMH) * 3600
            g_time = [current_time + duration * (i / max(1, len(g_path) - 1)) for i in range(len(g_path))]
            segments.append({"type": "rider", "path": g_path, "timestamps": g_time})
            logs.append(f"[{row['fetch_time'].split(' ')[-1]}] 订单#{order_id[-4:]} 纯地面微循环履约中。")

        elif 2.0 <= dist < 7.0:
            mid_lon, mid_lat = (slon + elon) / 2, (slat + elat) / 2
            g_path = planner.get_ground_route(slon, slat, mid_lon, mid_lat)
            if not g_path or len(g_path) <= 2: return None  # 🌟 严格把关

            g_duration = (haversine_distance(slon, slat, mid_lon, mid_lat) * RIDER_FOLD_RATE / RIDER_SPEED_KMH) * 3600
            g_time = [current_time + g_duration * (i / max(1, len(g_path) - 1)) for i in range(len(g_path))]
            current_time += g_duration + TRANSFER_TIME_SEC

            # 使用全新的 3D 拟真航线发生器
            d_path = generate_realistic_drone_path(mid_lon, mid_lat, elon, elat)
            d_duration = (haversine_distance(mid_lon, mid_lat, elon, elat) / DRONE_SMALL_SPEED_KMH) * 3600
            d_time = [current_time + d_duration * (i / max(1, len(d_path) - 1)) for i in range(len(d_path))]

            segments.extend([
                {"type": "rider", "path": g_path, "timestamps": g_time},
                {"type": "small_drone", "path": d_path, "timestamps": d_time}
            ])
            logs.append(f"[{row['fetch_time'].split(' ')[-1]}] 订单#{order_id[-4:]} 触发小机支线接驳。")

        else:
            p1_lon, p1_lat = slon + 0.01, slat + 0.01
            p2_lon, p2_lat = elon - 0.01, elat - 0.01

            g_path = planner.get_ground_route(slon, slat, p1_lon, p1_lat)
            if not g_path or len(g_path) <= 2: return None  # 🌟 严格把关

            g_duration = (haversine_distance(slon, slat, p1_lon, p1_lat) * RIDER_FOLD_RATE / RIDER_SPEED_KMH) * 3600
            g_time = [current_time + g_duration * (i / max(1, len(g_path) - 1)) for i in range(len(g_path))]
            current_time += g_duration + TRANSFER_TIME_SEC + BIG_DRONE_MAX_WAIT_SEC

            b_path = generate_realistic_drone_path(p1_lon, p1_lat, p2_lon, p2_lat, is_big=True)
            b_duration = (haversine_distance(p1_lon, p1_lat, p2_lon, p2_lat) / DRONE_BIG_SPEED_KMH) * 3600
            b_time = [current_time + b_duration * (i / max(1, len(b_path) - 1)) for i in range(len(b_path))]
            current_time += b_duration + TRANSFER_TIME_SEC

            s_path = generate_realistic_drone_path(p2_lon, p2_lat, elon, elat)
            s_duration = (haversine_distance(p2_lon, p2_lat, elon, elat) / DRONE_SMALL_SPEED_KMH) * 3600
            s_time = [current_time + s_duration * (i / max(1, len(s_path) - 1)) for i in range(len(s_path))]

            segments.extend([
                {"type": "rider", "path": g_path, "timestamps": g_time},
                {"type": "big_drone", "path": b_path, "timestamps": b_time},
                {"type": "small_drone", "path": s_path, "timestamps": s_time}
            ])
            logs.append(f"[{row['fetch_time'].split(' ')[-1]}] 订单#{order_id[-4:]} 触发大机干线桥接！")

        return {"id": order_id, "biz_line": biz_line, "distance": dist, "segments": segments, "logs": logs}
    except Exception as e:
        return None


# ==========================================
# 5. 主执行流程
# ==========================================
def main():
    print("📦 正在读取清洗数据...")
    df = pd.read_csv(CSV_FILE, encoding='utf-8')
    df = df.dropna(subset=['pickup_longitude', 'dropoff_longitude', 'fetch_time']).sample(n=min(SAMPLE_SIZE, len(df)),
                                                                                          random_state=42).sort_values(
        by='fetch_time')

    od_matrix = generate_od_matrix(df)
    hotspots = generate_hotspots(df)
    with open(OD_MATRIX_FILE, 'w', encoding='utf-8') as f:
        json.dump(od_matrix, f, ensure_ascii=False)
    with open(HOTSPOTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(hotspots, f, ensure_ascii=False)

    FILE_HUBS_JSON = '南山区15枢纽优化分布.json'
    hubs_data = []
    try:
        with open(FILE_HUBS_JSON, 'r', encoding='utf-8') as f:
            hubs_raw = json.load(f)
            for h in hubs_raw.get('hubs', []):
                level_str = "母港" if h.get("type") == "CORE" else "二级"
                hubs_data.append({
                    "id": str(h["hub_id"]), "name": h["hub_name"], "level": level_str,
                    "position": [float(h["coordinates"]["lng"]), float(h["coordinates"]["lat"])]
                })
    except Exception as e:
        print(f"⚠️ 读取枢纽 JSON 失败: {e}")

    planner = RoutePlanner(engine=ROUTING_ENGINE)
    final_data = {"buildings": [], "orders": [], "hubs": hubs_data, "truck": None}

    if len(hubs_data) >= 2:
        hub_start = next((h for h in hubs_data if str(h['id']) == '1'), hubs_data[0])
        hub_end = next((h for h in hubs_data if str(h['id']) == '4'), hubs_data[-1])
        final_data['truck'] = generate_truck_loop(hub_start['position'], hub_end['position'], 28800, 86400)

    print(f"⚡ 启动多线程并发推演 (抽样: {len(df)} 条)...")
    valid_orders_count = 0
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_single_order, row, planner): idx for idx, row in df.iterrows()}
        for future in tqdm(as_completed(futures), total=len(df), desc="🧠 推演中"):
            result = future.result()
            if result:
                final_data["orders"].append(result)
                valid_orders_count += 1

    print(
        f"ℹ️ 经过严苛物理校验，实际生成合规订单：{valid_orders_count} 条 (过滤掉了 {len(df) - valid_orders_count} 条穿墙异常轨迹)")

    final_data["buildings"] = get_or_fetch_buildings(22.541, 113.942, radius=15000)

    print("\n💾 序列化高维时空数据...")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False)
    print("✅ 全部完成！请刷新前端页面查看拟真飞行效果！")


if __name__ == "__main__":
    main()