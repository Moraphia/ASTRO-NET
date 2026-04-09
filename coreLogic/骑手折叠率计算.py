import pandas as pd
import numpy as np
import requests
import time
from geopy.distance import geodesic

# ==============================================
# 1. 核心参数配置
# ==============================================
AMAP_WEB_KEY = "9af0cf60bb0a74ffbb01ca57ed65ad4e"  # 请替换为你的Web服务Key
FILE_PATH = "/Users/zhangyt/Desktop/美团比赛/目标枢纽午高峰抽样订单_200个.csv"
HUB_COORD = (113.948541, 22.530976)  # (经度, 纬度)


# ==============================================
# 2. 高德API封装（严格遵循2次/秒频率）
# ==============================================
def get_amap_distance(dest_lng, dest_lat):
    """
    获取枢纽到目的地坐标的骑行实际距离
    """
    url = "https://restapi.amap.com/v4/direction/bicycling"
    # 格式要求：经度,纬度 (小数点后不超过6位)
    origin = f"{HUB_COORD[0]:.6f},{HUB_COORD[1]:.6f}"
    destination = f"{dest_lng:.6f},{dest_lat:.6f}"

    params = {
        "key": AMAP_WEB_KEY,
        "origin": origin,
        "destination": destination
    }

    try:
        res = requests.get(url, params=params, timeout=10)
        res_json = res.json()

        # 【关键：排障打印】
        print(f"DEBUG - 完整返回内容: {res_json}")

        # 如果返回的是 v3/通用 格式
        if str(res_json.get("status")) == "0":
            print(f"❌ 错误代码: {res_json.get('infocode')}, 原因: {res_json.get('info')}")
            return None

        # 如果返回的是 v4 格式
        if res_json.get("errcode") == 0:
            distance_m = res_json["data"]["paths"][0]["distance"]
            return distance_m / 1000
        else:
            return None
    except Exception as e:
        print(f"⚠️ 网络请求异常: {e}")
        return None


# ==============================================
# 3. 数据处理与计算
# ==============================================
print("=" * 60)
print("开始标定：计算南山区路网折叠率（Detour Factor）")
print("=" * 60)

# 加载数据
df = pd.read_csv(FILE_PATH)
# 仅取前200单
df_sample = df.head(200)

results = []

for index, row in df_sample.iterrows():
    # A. 提取文件中的坐标
    d_lat = row['dropoff_latitude']
    d_lng = row['dropoff_longitude']

    # B. 计算直线距离 (geodesic输入顺序为纬度,经度)
    straight_dist = geodesic((HUB_COORD[1], HUB_COORD[0]), (d_lat, d_lng)).km

    # C. 获取实际路网距离
    actual_dist = get_amap_distance(d_lng, d_lat)

    if actual_dist is not None:
        # 计算该单的折叠率
        factor = actual_dist / straight_dist if straight_dist > 0 else 1.0
        results.append({
            'straight': straight_dist,
            'actual': actual_dist,
            'factor': factor
        })
        print(
            f"进度: {index + 1:03d}/200 | 直线: {straight_dist:.3f}km | 实际: {actual_dist:.3f}km | 比率: {factor:.3f}")
    else:
        print(f"进度: {index + 1:03d}/200 | 调用失败，跳过...")

    # D. 严格限频：每秒2次，即每单处理完强制休息0.5秒
    time.sleep(0.5)

# ==============================================
# 4. 生成最终标定报告
# ==============================================
if results:
    res_df = pd.DataFrame(results)
    avg_straight = res_df['straight'].mean()
    avg_actual = res_df['actual'].mean()
    # 最终折叠率建议值
    final_detour_factor = avg_actual / avg_straight

    print("\n" + "=" * 60)
    print("🎯 最终标定报告")
    print("-" * 60)
    print(f"成功样本总数：{len(res_df)}")
    print(f"平均直线距离：{avg_straight:.4f} km")
    print(f"平均实际距离：{avg_actual:.4f} km")
    print(f"【核心参数】推荐折叠率 (Detour Factor)：{final_detour_factor:.4f}")
    print("=" * 60)
    print(f"使用建议：后续骑手路程 = 直线距离 * {final_detour_factor:.4f}")
else:
    print("\n❌ 错误：未成功获取任何有效样本，请检查API Key和网络连接。")