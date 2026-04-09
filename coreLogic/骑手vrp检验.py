import pandas as pd
import numpy as np
from geopy.distance import geodesic
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# ==============================================
# 1. 基础配置
# ==============================================
SAMPLE_FILE = "../目标枢纽午高峰抽样订单_200个.csv"
HUB_LAT, HUB_LNG = 22.530976, 113.948541
FOLDING_RATE = 1.4387
SERVICE_TIME = 3
RIDER_CAPACITY = 6
MAX_RIDERS = 50

# 时效等级硬编码
LIMITS = {"S级": 15, "A级": 30, "B级": 45}


def get_priority_level(priority_str):
    """统一优先级的提取逻辑"""
    priority_str = str(priority_str)
    if "S" in priority_str or "医疗" in priority_str: return "S级"
    if "A" in priority_str or "外卖" in priority_str: return "A级"
    return "B级"


# ==============================================
# 2. 求解核心 (含分级统计逻辑)
# ==============================================
def solve_iteration(current_speed, df_input):
    num_nodes = len(df_input) + 1
    coordinates = [(HUB_LAT, HUB_LNG)]
    for _, row in df_input.iterrows():
        coordinates.append((row["dropoff_latitude"], row["dropoff_longitude"]))

    time_matrix = np.zeros((num_nodes, num_nodes), dtype=int)
    dist_matrix = np.zeros((num_nodes, num_nodes))

    for i in range(num_nodes):
        for j in range(num_nodes):
            if i == j: continue
            d = geodesic(coordinates[i], coordinates[j]).km * FOLDING_RATE
            dist_matrix[i][j] = d
            t = (d / current_speed) * 60 + SERVICE_TIME
            time_matrix[i][j] = int(round(t))

    manager = pywrapcp.RoutingIndexManager(num_nodes, MAX_RIDERS, 0)
    routing = pywrapcp.RoutingModel(manager)

    def time_callback(from_index, to_index):
        return time_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]

    transit_idx = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)
    routing.AddDimension(transit_idx, 0, 3000, False, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")

    # 设置软时间窗
    for i in range(1, num_nodes):
        index = manager.NodeToIndex(i)
        level = get_priority_level(df_input.iloc[i - 1]["uav_priority"])
        limit = LIMITS[level]
        time_dimension.SetCumulVarSoftUpperBound(index, limit, 100)

    # 载荷约束
    def demand_callback(from_index):
        return 1 if manager.IndexToNode(from_index) != 0 else 0

    demand_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_idx, 0, [RIDER_CAPACITY] * MAX_RIDERS, True, "Capacity")

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_params.time_limit.seconds = 20
    solution = routing.SolveWithParameters(search_params)

    if solution:
        actual_total_dist = 0
        actual_total_time = 0
        # 初始化分级统计字典
        stats = {lvl: {"total": 0, "over": 0} for lvl in LIMITS.keys()}

        for v in range(MAX_RIDERS):
            index = routing.Start(v)
            prev_node = -1
            while not routing.IsEnd(index):
                node_idx = manager.IndexToNode(index)
                if prev_node != -1:
                    actual_total_dist += dist_matrix[prev_node][node_idx]

                if node_idx != 0:
                    arrival = solution.Min(time_dimension.CumulVar(index))
                    level = get_priority_level(df_input.iloc[node_idx - 1]["uav_priority"])
                    stats[level]["total"] += 1
                    if arrival > LIMITS[level]:
                        stats[level]["over"] += 1

                prev_node = node_idx
                index = solution.Value(routing.NextVar(index))

            actual_total_dist += dist_matrix[prev_node][0]
            actual_total_time += solution.Min(time_dimension.CumulVar(index))

        new_v = (actual_total_dist / (actual_total_time / 60)) if actual_total_time > 0 else current_speed
        return new_v, stats, actual_total_dist, actual_total_time
    return None


# ==============================================
# 3. 运行迭代
# ==============================================
df = pd.read_csv(SAMPLE_FILE)
df.columns = df.columns.str.strip()
iter_speed = 18.0

print("=" * 80)
print("骑手 VRP 仿真：分级时效统计 + 速度反馈迭代")
print("=" * 80)

for i in range(1, 4):
    print(f"\n▶️ 迭代 {i} | 参考速度: {iter_speed:.2f} km/h")
    res = solve_iteration(iter_speed, df)

    if res:
        new_v, stats, total_d, total_t = res
        print(f"✅ 求解成功！")
        # 打印分级统计结果
        for lvl, data in stats.items():
            if data["total"] > 0:
                rate = (data["over"] / data["total"]) * 100
                print(f"   [{lvl}] 规模: {data['total']:3d} | 超时: {data['over']:3d} | 超时率: {rate:5.1f}%")

        iter_speed = new_v
    else:
        print("❌ 求解失败")
        break

print("\n" + "=" * 80)
print(f"🏁 仿真最终结论")
print("-" * 80)
print(f"1. 最终作业速度: {iter_speed:.2f} km/h")
print(f"2. 最终分级表现 (速度为 {iter_speed:.2f} km/h 时):")
for lvl, data in stats.items():
    if data["total"] > 0:
        rate = (data["over"] / data["total"]) * 100
        print(f"   - {lvl}: 共 {data['total']} 单，超时 {data['over']} 单，超时率 {rate:.1f}%")
print("=" * 80)