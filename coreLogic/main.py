import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
# 1. 读取数据
file_path = "/Users/zhangyt/Desktop/美团比赛/深圳市南山区运单抽样_已脱敏.xlsx"
df = pd.read_excel(file_path)
print(f"1. 原始数据形状：{df.shape}")

# 清理列名空格
df.columns = df.columns.str.strip()

# 2. 核心逻辑：将 1-7 的数字转化为虚拟日期，并与时间拼接
# 假设 1 代表 2026-01-01，以此类推
def convert_to_full_time(df, time_col):
    # 构造 "2026-01-0X HH:MM" 格式的字符串
    # dt_seq_id 是 1-7，直接拼在日期后面
    full_time_str = "2026-01-0" + df['dt_seq_id'].astype(str) + " " + df[time_col].astype(str)
    return pd.to_datetime(full_time_str, errors='coerce')

print("正在处理时间列...")
df["customer_pay_time"] = convert_to_full_time(df, "customer_pay_time")
df["fetch_time"] = convert_to_full_time(df, "fetch_time")
df["arrived_time"] = convert_to_full_time(df, "arrived_time")

# 3. 处理跨午夜逻辑
# 如果送达时间比取货时间“早”，说明送达是在第二天的凌晨，日期需要加 1 天
# 例如：Day 1 的 23:55 取货，Day 1 的 00:05 送达（逻辑错误），需修正为 Day 2 的 00:05
mask = (df["arrived_time"] < df["fetch_time"])
df.loc[mask, "arrived_time"] = df.loc[mask, "arrived_time"] + pd.Timedelta(days=1)

# 4. 统一转数值类型
cols_to_fix = ["pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "package_price"]
for col in cols_to_fix:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# 5. 删除核心列中包含空值的行（保证11列数据完整）
core_columns = [
    'dt_seq_id', 'order_seq_id', 'pickup_latitude', 'pickup_longitude',
    'dropoff_latitude', 'dropoff_longitude', 'customer_pay_time',
    'fetch_time', 'arrived_time', 'package_price', 'biz_line'
]
df = df.dropna(subset=core_columns)
print(f"2. 转换并处理空值后形状：{df.shape}")

# 6. 经纬度宽松过滤（深圳范围）
df = df[
    (df["pickup_latitude"] > 22) & (df["pickup_latitude"] < 23) &
    (df["pickup_longitude"] > 113) & (df["pickup_longitude"] < 115) &
    (df["dropoff_latitude"] > 22) & (df["dropoff_latitude"] < 23) &
    (df["dropoff_longitude"] > 113) & (df["dropoff_longitude"] < 115)
]

# 7. 计算配送耗时并过滤
# 耗时公式：$T_{diff} = T_{arrived} - T_{fetch}$
df["duration_min"] = (df["arrived_time"] - df["fetch_time"]).dt.total_seconds() / 60
df = df[(df["duration_min"] > 0) & (df["duration_min"] <= 90)]

# 8. 价格清洗与去重
df = df[df["package_price"] >= 0]
df = df.drop_duplicates(subset=["order_seq_id"])

# 9. 整理并重置索引
df = df[core_columns].reset_index(drop=True)

print(f"清洗完成！最终数据形状：{df.shape}")
print(df.info())


# ----------------------
# 1. 计算配送时长（分钟）
# ----------------------
df["delivery_minutes"] = (df["arrived_time"] - df["fetch_time"]).dt.total_seconds() / 60

# 过滤异常时长（小于0 或 大于90分钟都删掉）
df = df[(df["delivery_minutes"] > 0) & (df["delivery_minutes"] < 90)]

# ----------------------
# 2. 定义“超时订单” = 高成本/低效率区域
# ----------------------
# 行业通用：平均时长的 1.5 倍 算作超时
mean_time = df["delivery_minutes"].mean()
timeout_threshold = mean_time * 1.5

df["is_timeout"] = df["delivery_minutes"] >= timeout_threshold

# 查看结果
print("平均配送时长：", round(mean_time, 2), "分钟")
print("超时判定线：", round(timeout_threshold, 2), "分钟")
print("超时订单数量：", df["is_timeout"].sum())
print("超时订单比例：", round(df["is_timeout"].mean()*100, 2), "%")

from sklearn.cluster import KMeans
import numpy as np

# ----------------------
# 1. 筛选出所有超时订单（只拿它们的经纬度）
# ----------------------
timeout_orders = df[df["is_timeout"] == 1].copy()

# 聚类用的坐标
X = timeout_orders[["dropoff_longitude", "dropoff_latitude"]].values

# ----------------------
# 2. K-Means 空间聚类（自动找出热点区域）
# ----------------------
# 你要找几个重灾区？一般 4~6 个最合适
n_clusters = 15
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
timeout_orders["cluster"] = kmeans.fit_predict(X)

# ----------------------
# 3. 输出每个重灾区的中心经纬度（最重要！）
# ----------------------
cluster_centers = kmeans.cluster_centers_
centers_df = pd.DataFrame(cluster_centers, columns=["经度", "纬度"])
centers_df["区域编号"] = range(1, n_clusters+1)

print("\n===== 南山区 配送超时重灾区（聚类中心）=====")
print(centers_df.round(6))