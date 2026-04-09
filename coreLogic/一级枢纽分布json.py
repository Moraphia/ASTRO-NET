import json
import pandas as pd
import folium
from folium import Circle, GeoJson

# ==============================================
# 【配置】严格对齐源文件的 15 个枢纽坐标与标签
# ==============================================
# 经纬度已完全匹配你提供的数值，不差一个字
hubs_data = [
    # --- 4个核心母港（Core Hubs） ---
    {"id": 1, "name": "西丽3号母港 (北核心)", "lng": 113.974875, "lat": 22.58325, "is_core": True},
    {"id": 2, "name": "前海母港 (西核心)", "lng": 113.917125, "lat": 22.54275, "is_core": True},
    {"id": 3, "name": "后海母港 (东核心)", "lng": 113.943375, "lat": 22.51125, "is_core": True},
    {"id": 4, "name": "蛇口母港 (南核心)", "lng": 113.927625, "lat": 22.48425, "is_core": True},

    # --- 11个次级枢纽（Secondary Hubs / 接驳站） ---
    {"id": 5, "name": "留仙洞站", "lng": 113.9260, "lat": 22.5670, "is_core": False},
    {"id": 6, "name": "大学城站", "lng": 113.964375, "lat": 22.59275, "is_core": False},
    {"id": 7, "name": "塘朗枢纽", "lng": 113.9880, "lat": 22.6100, "is_core": False},
    {"id": 8, "name": "华侨城站", "lng": 113.9810, "lat": 22.5360, "is_core": False},
    {"id": 9, "name": "科技园北站", "lng": 113.9486, "lat": 22.5558, "is_core": False},
    {"id": 10, "name": "南头青青世界", "lng": 113.901375, "lat": 22.51125, "is_core": False},
    {"id": 11, "name": "南油西站", "lng": 113.9180, "lat": 22.5070, "is_core": False},
    {"id": 12, "name": "大新站", "lng": 113.917125, "lat": 22.53375, "is_core": False},
    {"id": 13, "name": "赤湾站", "lng": 113.8990, "lat": 22.4770, "is_core": False},
    {"id": 14, "name": "深圳湾口岸站", "lng": 113.9550, "lat": 22.5050, "is_core": False},
    {"id": 15, "name": "月亮湾站", "lng": 113.9082, "lat": 22.4929, "is_core": False},
]

# ==============================================
# 1. 生成 JSON 结构化数据
# ==============================================
json_output = {
    "district": "南山区",
    "deployment_phase": "Phase 2: Full Coverage",
    "core_hub_count": 4,
    "total_hub_count": 15,
    "hubs": []
}

for h in hubs_data:
    item = {
        "hub_id": h["id"],
        "hub_name": h["name"],
        "coordinates": {"lng": h["lng"], "lat": h["lat"]}, # 保持原始精度输出
        "type": "CORE" if h["is_core"] else "SECONDARY",
        "role": "Main Logistics Hub" if h["is_core"] else "Rider Intercept Station",
        "influence_radius_km": 10 if h["is_core"] else 2
    }
    json_output["hubs"].append(item)

# 确保保存路径正确，如果需要保存到当前文件夹，请去掉 "../"
with open("南山区15枢纽优化分布.json", "w", encoding="utf-8") as f:
    json.dump(json_output, f, ensure_ascii=False, indent=2)

print("✅ JSON 数据已更新（完全匹配源文件精度）：南山区15枢纽优化分布.json")

# ==============================================
# 2. 生成交互式地理地图
# ==============================================
# 设置中心点为南山区大致地理中心
nanshan_center = [22.5330, 113.9300]
m = folium.Map(location=nanshan_center, zoom_start=12, tiles="CartoDB positron")

# 南山区边界加载
GeoJson(
    "https://geo.datav.aliyun.com/areas_v3/bound/440305.json",
    name="南山区边界轮廓",
    style_function=lambda x: {
        "color": "#333333",
        "weight": 2.5,
        "fillOpacity": 0.05
    }
).add_to(m)

# 绘制枢纽与覆盖范围
for h in hubs_data:
    is_core = h["is_core"]

    # 样式配置
    if is_core:
        color = "#E74C3C"  # 核心枢港：深红色
        fill_color = "#3498DB"  # 10km覆盖圈：蓝色
        opacity = 0.15
        radius = 10000  # 核心母港 10km 半径
        icon_type = "cloud"
    else:
        color = "#95A5A6"  # 次级枢纽：灰色
        fill_color = "#BDC3C7"
        opacity = 0.05
        radius = 2000  # 次级接驳 2km 半径
        icon_type = "info-sign"

    # 标记点：标签 (Tooltip) 与名称 (Popup) 均与源文件一致
    folium.Marker(
        location=[h["lat"], h["lng"]],
        tooltip=h["name"], # 悬停显示标签
        popup=f"ID: {h['id']}<br>Name: {h['name']}<br>Lat: {h['lat']}<br>Lng: {h['lng']}",
        icon=folium.Icon(color='red' if is_core else 'lightgray', icon=icon_type)
    ).add_to(m)

    # 覆盖圈
    Circle(
        location=[h["lat"], h["lng"]],
        radius=radius,
        color=fill_color if is_core else "#cccccc",
        fill=True,
        fillOpacity=opacity,
        weight=1.5 if is_core else 0.5,
        dash_array='5, 5' if not is_core else None
    ).add_to(m)

m.save("南山区低空经济枢纽全境覆盖图.html")
print("✅ 交互地图已生成：南山区低空经济枢纽全境覆盖图.html")