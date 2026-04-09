import pandas as pd
import numpy as np
from geopy.distance import geodesic
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# ==============================================
# 【第一步】导入文件 & 【关键】先查看数据的真实构成
# ==============================================
print("=" * 80)
print("第一步：导入文件 & 查看数据真实构成")
print("=" * 80)

sample_file_path = "../目标枢纽午高峰抽样订单_200个.csv"
df = pd.read_csv(sample_file_path, encoding="utf-8-sig")
print(f"✅ 成功导入抽样订单！订单数：{len(df)}")

# 【关键】先看看 uav_priority 字段里到底有什么值
print("\n📊 数据中 uav_priority（订单等级）的真实分布：")
print(df["uav_priority"].value_counts(dropna=False))
print("-" * 80)

# ==============================================
# 【第二步】执行你提供的清洗代码
# ==============================================
print("\n" + "=" * 80)
print("第二步：执行你提供的清洗代码")
print("=" * 80)

# 1. 确保经纬度、包裹价值是数值型
df["pickup_latitude"] = pd.to_numeric(df["pickup_latitude"], errors="coerce")
df["pickup_longitude"] = pd.to_numeric(df["pickup_longitude"], errors="coerce")
df["dropoff_latitude"] = pd.to_numeric(df["dropoff_latitude"], errors="coerce")
df["dropoff_longitude"] = pd.to_numeric(df["dropoff_longitude"], errors="coerce")
df["package_price"] = pd.to_numeric(df["package_price"], errors="coerce")

# 2. 时间格式优化（增加容错，避免重复拼接日期）
# 先检查是否已经是datetime格式
if not pd.api.types.is_datetime64_any_dtype(df["customer_pay_time"]):
    # 如果是字符串，尝试直接转
    df["customer_pay_time"] = pd.to_datetime(df["customer_pay_time"], errors="coerce")
    # 如果转出来有问题，再拼接日期
    if df["customer_pay_time"].isna().any():
        df["customer_pay_time"] = "2023-01-01 " + df["customer_pay_time"].astype(str)
        df["customer_pay_time"] = pd.to_datetime(df["customer_pay_time"], format="%Y-%m-%d %H:%M", errors="coerce")

# 同样处理 fetch_time 和 arrived_time（如果有的话）
for col in ["fetch_time", "arrived_time"]:
    if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
        df[col] = pd.to_datetime(df[col], errors="coerce")
        if df[col].isna().any():
            df[col] = "2023-01-01 " + df[col].astype(str)
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M", errors="coerce")

# 3. 重置索引
df = df.reset_index(drop=True)
print("✅ 数据清洗完成！")

# ==============================================
# 【第三步】构建VRP模型（如实处理S/A/B级）
# ==============================================
print("\n" + "=" * 80)
print("第三步：构建VRP模型（如实处理各等级）")
print("=" * 80)

# --- 核心参数 ---
HUB_LAT = 22.530976
HUB_LNG = 113.948541
DRONE_SPEED = 80
DRONE_CAPACITY = 5
MAX_VEHICLES = 200

# --- 分级时效约束（你可以在这里定义B级的时间）---
TIME_LIMIT_S = 15  # S级：15分钟
TIME_LIMIT_A = 30  # A级：30分钟
TIME_LIMIT_B = 45  # B级：我们也给它设一个约束，比如45分钟


def create_vrptw_data_model(df_subset):
    data = {}
    data["num_nodes"] = len(df_subset) + 1
    data["num_vehicles"] = MAX_VEHICLES
    data["depot"] = 0

    # 1. 坐标列表 & 旅行时间矩阵
    coordinates = [(HUB_LAT, HUB_LNG)]
    for idx, row in df_subset.iterrows():
        coordinates.append((row["dropoff_latitude"], row["dropoff_longitude"]))

    travel_time_matrix = np.zeros((data["num_nodes"], data["num_nodes"]), dtype=int)
    for i in range(data["num_nodes"]):
        for j in range(data["num_nodes"]):
            if i == j:
                continue
            dist_km = geodesic(coordinates[i], coordinates[j]).km
            time_min = (dist_km / DRONE_SPEED) * 60 + 2#加两分钟的降落服务时间
            travel_time_matrix[i][j] = int(round(time_min))
    data["travel_time_matrix"] = travel_time_matrix.tolist()

    # 2. 【关键】如实构建时间窗：先判断等级，再对应时间
    time_windows = [(0, 1000)]  # 枢纽
    order_priorities = ["枢纽"]  # 记录每个节点的等级，方便后面验证

    for idx, row in df_subset.iterrows():
        priority = str(row["uav_priority"]).strip()
        order_priorities.append(priority)

        # 根据真实等级设置时间
        if "S级" in priority or "医疗" in priority:
            end_time = TIME_LIMIT_S
        elif "A级" in priority or "外卖" in priority:
            end_time = TIME_LIMIT_A
        elif "B级" in priority:
            end_time = TIME_LIMIT_B
        else:
            end_time = TIME_LIMIT_B  # 其他未知等级归为B级

        time_windows.append((0, end_time))

    data["time_windows"] = time_windows
    data["order_priorities"] = order_priorities  # 存起来，后面验证用

    # 3. 容量约束
    data["vehicle_capacities"] = [DRONE_CAPACITY] * data["num_vehicles"]
    data["demands"] = [0] + [1] * len(df_subset)

    return data, coordinates


# 创建模型
data, coordinates = create_vrptw_data_model(df)

# 【关键】再打印一遍我们识别到的等级分布，确保和原始数据一致
print("\n📊 模型识别到的订单等级分布：")
for p in ["S级", "A级", "B级"]:
    cnt = data["order_priorities"].count(p)
    print(f"   {p}：{cnt} 单")
print(f"   （枢纽：1 个节点）")

# ==============================================
# 【第四步】求解VRP
# ==============================================
print("\n" + "=" * 80)
print("第四步：求解VRP模型")
print("=" * 80)

manager = pywrapcp.RoutingIndexManager(
    data["num_nodes"], data["num_vehicles"], data["depot"]
)
routing = pywrapcp.RoutingModel(manager)


# 时间回调
def time_callback(from_index, to_index):
    from_node = manager.IndexToNode(from_index)
    to_node = manager.IndexToNode(to_index)
    return data["travel_time_matrix"][from_node][to_node]


transit_callback_index = routing.RegisterTransitCallback(time_callback)
routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

# 时间窗约束
time_dimension_name = "Time"
routing.AddDimension(
    transit_callback_index,
    1000,
    1000,
    False,
    time_dimension_name
)
time_dimension = routing.GetDimensionOrDie(time_dimension_name)

for location_idx in range(1, data["num_nodes"]):
    index = manager.NodeToIndex(location_idx)
    start, end = data["time_windows"][location_idx]
    time_dimension.CumulVar(index).SetRange(start, end)


# 容量约束
def demand_callback(from_index):
    from_node = manager.IndexToNode(from_index)
    return data["demands"][from_node]


demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
routing.AddDimensionWithVehicleCapacity(
    demand_callback_index,
    0,
    data["vehicle_capacities"],
    True,
    "Capacity"
)

# 求解设置
search_parameters = pywrapcp.DefaultRoutingSearchParameters()
search_parameters.first_solution_strategy = (
    routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
)
search_parameters.local_search_metaheuristic = (
    routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
)
search_parameters.time_limit.seconds = 60

print("⏳ 正在求解中（最多60秒）...")
solution = routing.SolveWithParameters(search_parameters)

# ==============================================
# 【第五步】【关键】如实输出结果，不隐瞒
# ==============================================
print("\n" + "=" * 80)
print("第五步：结果统计（如实输出）")
print("=" * 80)

if solution:
    num_drones_used = 0
    # 初始化计数器
    violations = {
        "S级": {"total": 0, "violate": 0, "limit": TIME_LIMIT_S},
        "A级": {"total": 0, "violate": 0, "limit": TIME_LIMIT_A},
        "B级": {"total": 0, "violate": 0, "limit": TIME_LIMIT_B},
    }

    print("\n🚁 无人机配送路径（前5架）：")
    print("-" * 80)

    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        route_load = 0
        route_nodes = []
        arrival_times = []

        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            route_load += data["demands"][node_index]
            route_nodes.append(node_index)
            time_var = time_dimension.CumulVar(index)
            arrival_times.append(solution.Min(time_var))
            previous_index = index
            index = solution.Value(routing.NextVar(index))

        route_nodes.append(manager.IndexToNode(index))
        time_var = time_dimension.CumulVar(index)
        arrival_times.append(solution.Min(time_var))

        if route_load > 0:
            num_drones_used += 1
            if num_drones_used <= 5:
                print(f"✅ 无人机 {vehicle_id:2d}：")
                print(f"   路径：{' -> '.join(map(str, route_nodes))}")
                print(f"   载货量：{route_load} 单")
                print(f"   到达时间：{arrival_times} 分钟\n")

            # 【关键】逐单验证时效
            for i in range(1, len(route_nodes) - 1):
                node_idx = route_nodes[i]
                if node_idx == 0:
                    continue

                arrival_time = arrival_times[i]
                priority_str = data["order_priorities"][node_idx]

                # 确定是哪个等级
                level = "B级"  # 默认
                if "S级" in priority_str:
                    level = "S级"
                elif "A级" in priority_str:
                    level = "A级"

                violations[level]["total"] += 1
                if arrival_time > violations[level]["limit"]:
                    violations[level]["violate"] += 1

    print("=" * 80)
    print(f"🎯 【核心结论】")
    print(f"1. 午间高峰无人机高峰需求：{num_drones_used} 架")
    print(f"2. 时效约束验证（如实统计）：")
    for level in ["S级", "A级", "B级"]:
        v = violations[level]
        pass_rate = (v["total"] - v["violate"]) / v["total"] * 100 if v["total"] > 0 else 0
        print(f"   {level}：共 {v['total']} 单，约束≤{v['limit']}分钟，"
              f"超时 {v['violate']} 单，通过率 {pass_rate:.1f}%")



    # 定义函数：计算单个订单的商户到用户直线距离（km）
    def calc_order_distance(row):
        pickup_point = (row["pickup_latitude"], row["pickup_longitude"])
        dropoff_point = (row["dropoff_latitude"], row["dropoff_longitude"])
        return geodesic(pickup_point, dropoff_point).km


    # 应用到数据
    df["order_distance_km"] = df.apply(calc_order_distance, axis=1)

    # 计算总里程
    total_km = df["order_distance_km"].sum()

    # 打印结果
    print("\n" + "=" * 80)
    print(f"📊 这 {len(df)} 个订单的总直线里程：{total_km:.2f} 公里")
    print(f"   平均每单里程：{total_km / len(df):.2f} 公里")
    print("=" * 80)
    print("=" * 80)

else:
    print("❌ 未找到可行解！")
    print("   建议：")
    print("   1. 查看第一步打印的‘真实等级分布’，确认数据是否正确")
    print("   2. 适当放宽 TIME_LIMIT_S / TIME_LIMIT_A / TIME_LIMIT_B")