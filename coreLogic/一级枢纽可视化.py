import json
import folium
from folium import Circle

# 读取你的 JSON 文件
with open("../枢纽影响范围可视化数据.json", "r", encoding="utf-8") as f:
    data = json.load(f)

hubs = data["hub_visualization_data"]

# 地图中心（用你南山区第一个枢纽）
center_lat = hubs[0]["coordinates"]["latitude"]
center_lng = hubs[0]["coordinates"]["longitude"]

# 创建地图
m = folium.Map(
    location=[center_lat, center_lng],
    zoom_start=12,
    tiles="https://webrd04.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={x}&y={y}&z={z}",
    attr="高德地图"
)

# 绘制每个枢纽 + 10公里影响范围
for hub in hubs:
    lat = hub["coordinates"]["latitude"]
    lng = hub["coordinates"]["longitude"]
    name = hub["hub_name"]
    radius = hub["influence_range"]["radius"] * 1000  # 转成米

    # 画点
    folium.Marker(
        location=[lat, lng],
        popup=name,
        icon=folium.Icon(color="red", icon="plane")
    ).add_to(m)

    # 画10公里圈
    Circle(
        location=[lat, lng],
        radius=radius,
        color="blue",
        fill=True,
        fill_opacity=0.1
    ).add_to(m)

# 保存成网页（双击就能打开看）
m.save("枢纽10公里影响范围可视化.html")
print("✅ 已生成：枢纽10公里影响范围可视化.html")