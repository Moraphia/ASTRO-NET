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

# ==========================================
# 1. 全局配置区
# ==========================================
CSV_FILE = '深圳市南山区运单抽样_已脱敏.csv'#阿哒哒哒
OUTPUT_JSON = 'project_data.json'
OD_MATRIX_FILE = 'od_matrix.json'
HOTSPOTS_FILE = 'hotspots.json'

ROUTING_ENGINE = 'osrm'
DRONE_MAX_RANGE_KM = 20.0
MAX_THREADS = 20
SAMPLE_SIZE = 30000
OD_TOP_N = 15  # 只显示流量最大的前N条OD对
HOTSPOT_CLUSTER_DISTANCE = 0.003  # 约300米


# ==========================================
# 2. 基础工具函数 (保持不变)
# ==========================================
def haversine_distance(lon1, lat1, lon2, lat2):
    R = 6371.0
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return R * c


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
# 3. 新增：OD矩阵生成函数
# ==========================================
def generate_od_matrix(df, top_n=OD_TOP_N):
    """
    生成主要的OD（起点-终点）流量矩阵
    返回格式: [{source: [lon, lat], target: [lon, lat], count: 数量, weight: 权重}, ...]
    """
    print("\n📊 正在生成 OD 流量矩阵...")

    # 使用经纬度网格进行聚合（简化处理）
    od_dict = defaultdict(int)
    grid_size = 0.003  # 约300米的网格

    for _, row in df.iterrows():
        try:
            slon, slat = float(row['pickup_longitude']), float(row['pickup_latitude'])
            elon, elat = float(row['dropoff_longitude']), float(row['dropoff_latitude'])

            # 网格化
            s_grid = (round(slon / grid_size), round(slat / grid_size))
            e_grid = (round(elon / grid_size), round(elat / grid_size))

            if s_grid != e_grid:  # 排除同网格内的行程
                od_dict[(s_grid, e_grid)] += 1
        except:
            continue

    # 转换为前端需要的格式
    od_list = []
    for (s_grid, e_grid), count in od_dict.items():
        s_lon, s_lat = s_grid[0] * grid_size, s_grid[1] * grid_size
        e_lon, e_lat = e_grid[0] * grid_size, e_grid[1] * grid_size

        # 计算距离作为权重因子
        distance = haversine_distance(s_lon, s_lat, e_lon, elat)
        weight = count * distance  # 流量 × 距离 = 运输工作量

        od_list.append({
            "source": [s_lon, s_lat],
            "target": [e_lon, elat],
            "count": count,
            "weight": weight
        })

    # 按流量排序，取前top_n
    od_list.sort(key=lambda x: x["count"], reverse=True)
    top_od = od_list[:top_n]

    print(f"✅ 生成 {len(top_od)} 条主要OD流")
    return top_od


# ==========================================
# 4. 新增：热点区域识别函数
# ==========================================
def generate_hotspots(df, eps=HOTSPOT_CLUSTER_DISTANCE, min_samples=5):
    """
    使用DBSCAN聚类识别热点区域
    返回格式: [{position: [lon, lat], intensity: 强度, radius: 半径}, ...]
    """
    print("\n🔥 正在识别运单热点区域...")

    # 提取所有起点坐标
    points = []
    for _, row in df.iterrows():
        try:
            lon, lat = float(row['pickup_longitude']), float(row['pickup_latitude'])
            points.append([lon, lat])
        except:
            continue

    if not points:
        return []

    # DBSCAN聚类
    coords = np.array(points)
    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric='haversine').fit(
        np.radians(coords)
    )

    # 统计每个簇
    clusters = {}
    for i, label in enumerate(clustering.labels_):
        if label == -1:  # 噪声点
            continue
        if label not in clusters:
            clusters[label] = {"points": [], "count": 0}
        clusters[label]["points"].append(coords[i])
        clusters[label]["count"] += 1

    # 转换为热点数据
    hotspots = []
    for label, cluster in clusters.items():
        if cluster["count"] < 10:  # 过滤掉太小的簇
            continue

        points_arr = np.array(cluster["points"])
        center_lon, center_lat = points_arr.mean(axis=0)

        # 计算簇的半径（最大距离）
        distances = []
        for point in points_arr:
            dist = haversine_distance(center_lon, center_lat, point[0], point[1])
            distances.append(dist)

        radius = max(distances) if distances else 0.1

        hotspots.append({
            "position": [float(center_lon), float(center_lat)],
            "intensity": cluster["count"],
            "radius": radius  # 公里
        })

    # 按强度排序
    hotspots.sort(key=lambda x: x["intensity"], reverse=True)
    print(f"✅ 识别出 {len(hotspots)} 个热点区域")
    return hotspots


# ==========================================
# 5. 原有的路径规划类 (保持不变)
# ==========================================
class RoutePlanner:
    def __init__(self, engine='osrm'):
        self.engine = engine

    def get_ground_route(self, start_lon, start_lat, end_lon, end_lat):
        if self.engine == 'osrm':
            return self._get_osrm_route(start_lon, start_lat, end_lon, end_lat)
        return [[start_lon, start_lat], [end_lon, end_lat]]

    def _get_osrm_route(self, start_lon, start_lat, end_lon, end_lat):
        url = f"http://127.0.0.1:5000/route/v1/driving/{start_lon},{start_lat};{end_lon},{end_lat}?overview=full&geometries=geojson"
        try:
            res = requests.get(url, timeout=5).json()
            if res.get('code') == 'Ok':
                return res['routes'][0]['geometry']['coordinates']
        except:
            pass
        return [[start_lon, start_lat], [end_lon, end_lat]]


def generate_3d_drone_route(start_lon, start_lat, end_lon, end_lat):
    path = []
    path.append([start_lon, start_lat, 0])
    path.append([start_lon, start_lat, 60])

    vec_x = end_lon - start_lon
    vec_y = end_lat - start_lat
    normal_x = -vec_y
    normal_y = vec_x

    swerve_dir = random.choice([-1, 1])
    peak_alt = random.randint(120, 180)

    wp1_x = start_lon + vec_x * 0.33 + normal_x * 0.15 * swerve_dir
    wp1_y = start_lat + vec_y * 0.33 + normal_y * 0.15 * swerve_dir
    path.append([wp1_x, wp1_y, peak_alt])

    wp2_x = start_lon + vec_x * 0.66 + normal_x * 0.08 * swerve_dir
    wp2_y = start_lat + vec_y * 0.66 + normal_y * 0.08 * swerve_dir
    path.append([wp2_x, wp2_y, peak_alt - 20])

    path.append([end_lon, end_lat, 60])
    path.append([end_lon, end_lat, 0])

    return path


def fetch_3d_buildings_safe(lat, lon, radius=15000):
    print(f"\n🏢 正在抓取南山区(半径{radius / 1000}公里)的大范围 3D 建筑...")
    try:
        ox.settings.timeout = 600
        tags = {'building': True}
        gdf = ox.features_from_point((lat, lon), dist=radius, tags=tags)
        buildings_data = []

        for _, row in gdf.iterrows():
            try:
                geom = row['geometry']
                if not geom.is_valid or geom.is_empty:
                    continue

                height = 40
                if 'height' in row and pd.notna(row['height']):
                    try:
                        height = float(str(row['height']).replace('m', '').strip())
                    except:
                        pass
                elif 'building:levels' in row and pd.notna(row['building:levels']):
                    try:
                        height = float(row['building:levels']) * 3.5
                    except:
                        pass
                else:
                    height = random.randint(80, 150) if random.random() > 0.9 else random.randint(20, 60)

                if geom.geom_type == 'Polygon':
                    coords = list(geom.exterior.coords)
                    if len(coords) >= 3:
                        buildings_data.append({"polygon": coords, "height": height})
                elif geom.geom_type == 'MultiPolygon':
                    for poly in geom.geoms:
                        coords = list(poly.exterior.coords)
                        if len(coords) >= 3:
                            buildings_data.append({"polygon": coords, "height": height})
            except Exception:
                continue

        print(f"✅ 成功生成 {len(buildings_data)} 栋 3D 建筑！")
        return buildings_data
    except Exception as e:
        print(f"⚠️ 建筑数据抓取失败: {e}")
        return []


# ==========================================
# 6. 单线程处理核心逻辑 (修复直线穿墙Bug)
# ==========================================
def process_single_order(row, planner):
    try:
        start_lon, start_lat = float(row['pickup_longitude']), float(row['pickup_latitude'])
        end_lon, end_lat = float(row['dropoff_longitude']), float(row['dropoff_latitude'])

        if haversine_distance(start_lon, start_lat, end_lon, end_lat) > DRONE_MAX_RANGE_KM:
            return None

        start_sec = time_to_seconds(row['fetch_time'])
        end_sec = time_to_seconds(row['arrived_time'])
        if start_sec >= end_sec or start_sec == 0: return None

        ground_path = planner.get_ground_route(start_lon, start_lat, end_lon, end_lat)

        # 【核心修复】：如果 OSRM 寻路失败返回了直线的两点，直接丢弃这条异常穿墙数据！
        if len(ground_path) <= 2:
            return None

        rider_timestamps = [start_sec + (end_sec - start_sec) * (i / max(1, len(ground_path) - 1)) for i in
                            range(len(ground_path))]

        drone_path = generate_3d_drone_route(start_lon, start_lat, end_lon, end_lat)
        drone_end_sec = start_sec + (end_sec - start_sec) * 0.6
        time_span = drone_end_sec - start_sec

        drone_timestamps = [
            start_sec,
            start_sec + time_span * 0.05,
            start_sec + time_span * 0.35,
            start_sec + time_span * 0.65,
            start_sec + time_span * 0.95,
            drone_end_sec
        ]

        return {
            "rider": {"path": ground_path, "timestamps": rider_timestamps},
            "drone": {"path": drone_path, "timestamps": drone_timestamps}
        }
    except Exception:
        return None


# ==========================================
# 7. 主执行流程 (增强版)
# ==========================================
def main():
    print("📦 正在读取并清洗脱敏运单数据...")
    df = pd.read_csv(CSV_FILE, encoding='utf-8')
    df = df.dropna(subset=['pickup_longitude', 'dropoff_longitude', 'fetch_time', 'arrived_time'])

    actual_sample_size = min(SAMPLE_SIZE, len(df))
    df = df.sample(n=actual_sample_size, random_state=42).sort_values(by='fetch_time')

    # 步骤1: 生成OD矩阵和热点数据
    od_matrix = generate_od_matrix(df)
    hotspots = generate_hotspots(df)

    # 保存额外的数据文件
    with open(OD_MATRIX_FILE, 'w', encoding='utf-8') as f:
        json.dump(od_matrix, f, ensure_ascii=False)

    with open(HOTSPOTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(hotspots, f, ensure_ascii=False)

    print("✅ OD矩阵和热点数据已保存")

    # 步骤2: 处理轨迹数据
    planner = RoutePlanner(engine=ROUTING_ENGINE)
    final_data = {"buildings": [], "riders": [], "drones": []}

    print(f"⚡ 启动多线程并发引擎 (抽样处理量: {actual_sample_size} 条, 线程数: {MAX_THREADS})...")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_single_order, row, planner): idx for idx, row in df.iterrows()}

        for future in tqdm(as_completed(futures), total=actual_sample_size, desc="🧠 算法推演中", unit="单"):
            result = future.result()
            if result:
                final_data["riders"].append(result["rider"])
                final_data["drones"].append(result["drone"])

    # 步骤3: 获取建筑数据
    center_lat = 22.541
    center_lon = 113.942
    final_data["buildings"] = fetch_3d_buildings_safe(center_lat, center_lon, radius=15000)

    # 步骤4: 保存主数据
    print("\n💾 运算完毕！正在序列化高维时空数据...")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False)

    print(f"""
    🎉 彻底完工！生成结果汇总：
    ========================================
    1. 轨迹数据: {len(final_data['riders'])} 条双轨推演路径
    2. 3D建筑:   {len(final_data['buildings'])} 栋城市白模
    3. OD矩阵:   {len(od_matrix)} 条主要流量走廊
    4. 热点区域: {len(hotspots)} 个运力聚集区
    ========================================
    数据文件已保存至:
    - {OUTPUT_JSON} (主轨迹数据)
    - {OD_MATRIX_FILE} (OD流量矩阵)
    - {HOTSPOTS_FILE} (热力点数据)
    """)


if __name__ == "__main__":
    main()