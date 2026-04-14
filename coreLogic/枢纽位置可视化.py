import pandas as pd
import folium
import os
import json

# ==========================================
# 1. 路径配置
# ==========================================
FILE_HUBS = "/Users/zhangyt/Desktop/美团比赛/南山区枢纽清单_均衡优化版_Final.csv"
OUTPUT_HTML = "/Users/zhangyt/Desktop/美团比赛/南山区枢纽布局分布图_清晰版.html"


def generate_clean_map():
    if not os.path.exists(FILE_HUBS):
        print("❌ 错误：找不到枢纽文件。")
        return

    df = pd.read_csv(FILE_HUBS)

    # 1. 创建地图：使用浅色高对比度底图，方便查看地名
    m = folium.Map(
        location=[22.536, 113.935],
        zoom_start=12,
        tiles='CartoDB positron',
        control_scale=True
    )

    # 2. 注入南山区行政边界 (使用标准的 GeoJSON 逻辑)
    # 这里我们添加一个带阴影的边界线，突出南山区范围
    nanshan_geo = "https://geo.datav.aliyun.com/areas_v3/bound/440305.json"
    folium.GeoJson(
        nanshan_geo,
        name='南山区边界',
        style_function=lambda x: {
            'fillColor': '#f2f2f2',
            'color': '#666666',
            'weight': 3,
            'fillOpacity': 0.1,
            'dashArray': '5, 5'
        }
    ).add_to(m)

    # 3. 枢纽样式配置
    style_config = {
        '母港': {'color': '#E31A1C', 'fill_color': '#FB9A99'},  # 深红
        '一级': {'color': '#1F78B4', 'fill_color': '#A6CEE3'},  # 深蓝
        '二级': {'color': '#FF7F00', 'fill_color': '#FDBF6F'}  # 橙色
    }

    # 4. 遍历枢纽点
    for _, row in df.iterrows():
        h_id = row['ID']
        h_name = row['名称']
        h_level = str(row['级别'])
        lat = row['纬度']
        lon = row['经度']

        # 匹配颜色
        cfg = style_config['二级']  # 默认
        if '母港' in h_level:
            cfg = style_config['母港']
        elif '一级' in h_level:
            cfg = style_config['一级']

        # A. 绘制高对比度标记点
        folium.CircleMarker(
            location=[lat, lon],
            radius=6,
            color=cfg['color'],
            weight=2,
            fill=True,
            fill_color=cfg['fill_color'],
            fill_opacity=1.0,
            popup=f"ID: {h_id} | {h_name} ({h_level})"
        ).add_to(m)

        # B. 核心优化：常驻显示编号和名称 (不再层叠，使用透明背景)
        folium.map.Marker(
            [lat, lon],
            icon=folium.DivIcon(
                icon_size=(200, 30),
                icon_anchor=(-10, 10),  # 文字偏移在点位的右上方
                html=f'''<div style="
                    font-size: 8pt; 
                    color: #333; 
                    background-color: rgba(255,255,255,0.7);
                    border: 1px solid {cfg['color']};
                    border-radius: 3px;
                    padding: 2px 5px;
                    width: fit-content;
                    white-space: nowrap;
                    font-weight: bold;
                    ">ID:{h_id} {h_name}</div>'''
            )
        ).add_to(m)

    # 5. 保存
    m.save(OUTPUT_HTML)
    print("\n" + "=" * 40)
    print(f"🏆 极简清晰版布局图已生成！")
    print(f"特点：自带南山区行政边界，常驻显示 ID 和名称。")
    print(f"文件位置：{OUTPUT_HTML}")
    print("=" * 40)


if __name__ == "__main__":
    generate_clean_map()