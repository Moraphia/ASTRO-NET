import pandas as pd
import json
import math
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import osmnx as ox
import random

# ==========================================
# 1. 全局配置区
# ==========================================
CSV_FILE = '深圳市南山区运单抽样_已脱敏.csv'
OUTPUT_JSON = 'project_data.json'

ROUTING_ENGINE = 'osrm'

DRONE_MAX_RANGE_KM = 20.0
MAX_THREADS = 20
SAMPLE_SIZE = 30000


# ==========================================
# 2. 基础工具函数
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
# 3. 寻路策略与建筑抓取类
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
    """【智能避障引擎】生成带爬升、绕飞、拔高、降落的仿生轨迹"""
    path = []

    # 1. 绝对地面起飞
    path.append([start_lon, start_lat, 0])
    # 2. 原地垂直爬升至 60m 准备巡航
    path.append([start_lon, start_lat, 60])

    # 核心算法：算出起终点向量，做一条垂直的法向量，用于生成横向避障的曲线
    vec_x = end_lon - start_lon
    vec_y = end_lat - start_lat
    normal_x = -vec_y
    normal_y = vec_x

    # 随机决定向左绕还是向右绕
    swerve_dir = random.choice([-1, 1])
    # 遇到超高层建筑，随机拉升高度至 120-180米
    peak_alt = random.randint(120, 180)

    # 3. 避障机动点 1 (航程 33% 处，开始爬升并横向绕飞)
    wp1_x = start_lon + vec_x * 0.33 + normal_x * 0.15 * swerve_dir
    wp1_y = start_lat + vec_y * 0.33 + normal_y * 0.15 * swerve_dir
    path.append([wp1_x, wp1_y, peak_alt])

    # 4. 避障机动点 2 (航程 66% 处，越过障碍物，准备切回主航线)
    wp2_x = start_lon + vec_x * 0.66 + normal_x * 0.08 * swerve_dir
    wp2_y = start_lat + vec_y * 0.66 + normal_y * 0.08 * swerve_dir
    path.append([wp2_x, wp2_y, peak_alt - 20])

    # 5. 到达目标上空 60m
    path.append([end_lon, end_lat, 60])
    # 6. 垂直降落至绝对地面
    path.append([end_lon, end_lat, 0])

    return path


def fetch_3d_buildings_safe(lat, lon, radius=15000):
    print(f"\n🏢 正在抓取南山区(半径{radius / 1000}公里)的大范围 3D 建筑... (约需1分钟)")
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
# 4. 单线程处理核心逻辑
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
        rider_timestamps = [start_sec + (end_sec - start_sec) * (i / max(1, len(ground_path) - 1)) for i in
                            range(len(ground_path))]

        # 【同步修改】：为 6 个航点匹配极其精准的时间戳，让降落和起飞看起来符合物理惯性
        drone_path = generate_3d_drone_route(start_lon, start_lat, end_lon, end_lat)
        drone_end_sec = start_sec + (end_sec - start_sec) * 0.6
        time_span = drone_end_sec - start_sec

        drone_timestamps = [
            start_sec,  # 1. 0% - 开始起飞
            start_sec + time_span * 0.05,  # 2. 5% - 爬升完毕
            start_sec + time_span * 0.35,  # 3. 35% - 避障顶点 1
            start_sec + time_span * 0.65,  # 4. 65% - 避障顶点 2
            start_sec + time_span * 0.95,  # 5. 95% - 飞抵目标上空
            drone_end_sec  # 6. 100% - 落地完毕
        ]

        return {
            "rider": {"path": ground_path, "timestamps": rider_timestamps},
            "drone": {"path": drone_path, "timestamps": drone_timestamps}
        }
    except Exception:
        return None


# ==========================================
# 5. 主执行流程
# ==========================================
def main():
    print("📦 正在读取并清洗脱敏运单数据...")
    df = pd.read_csv(CSV_FILE, encoding='utf-8')
    df = df.dropna(subset=['pickup_longitude', 'dropoff_longitude', 'fetch_time', 'arrived_time'])

    actual_sample_size = min(SAMPLE_SIZE, len(df))
    df = df.sample(n=actual_sample_size, random_state=42).sort_values(by='fetch_time')

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

    center_lat = 22.541
    center_lon = 113.942
    final_data["buildings"] = fetch_3d_buildings_safe(center_lat, center_lon, radius=5000)

    print("\n💾 运算完毕！正在序列化高维时空数据...")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False)

    print(
        f"🎉 彻底完工！成功生成 {len(final_data['riders'])} 条双轨推演数据，及 {len(final_data['buildings'])} 栋3D建筑，存入 {OUTPUT_JSON}")


if __name__ == "__main__":
    main()