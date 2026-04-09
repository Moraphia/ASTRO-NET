import pandas as pd

# ==============================================
# 【第一步】导入文件 & 【关键】先看一眼原始时间长什么样
# ==============================================
print("=" * 80)
print("第一步：导入文件 & 检查原始时间格式")
print("=" * 80)

file_path = "/Users/zhangyt/Desktop/美团比赛/无人机高价值订单_严格约束版.csv"
df = pd.read_csv(file_path, encoding="utf-8-sig")

# 【关键】打印前5行的 customer_pay_time，看看原始数据到底长什么样
print("📊 原始数据 customer_pay_time 样本（前5行）：")
print(df["customer_pay_time"].head().to_string())
print("-" * 80)
print("📊 原始数据等级分布：")
print(df["uav_priority"].value_counts(dropna=False))

# ==============================================
# 【第二步】【修复】更健壮的时间清洗代码
# ==============================================
print("\n" + "=" * 80)
print("第二步：执行健壮的数据清洗")
print("=" * 80)

# 1. 数值型转换（保留你的代码）
df["pickup_latitude"] = pd.to_numeric(df["pickup_latitude"], errors="coerce")
df["pickup_longitude"] = pd.to_numeric(df["pickup_longitude"], errors="coerce")
df["dropoff_latitude"] = pd.to_numeric(df["dropoff_latitude"], errors="coerce")
df["dropoff_longitude"] = pd.to_numeric(df["dropoff_longitude"], errors="coerce")
df["package_price"] = pd.to_numeric(df["package_price"], errors="coerce")


# 2. 【修复】时间格式优化（不强制拼接，先试直接转，不行再处理）
def parse_time_safely(series):
    # 先尝试直接转 datetime
    temp = pd.to_datetime(series, errors="coerce")
    # 如果成功率超过80%，说明格式没问题，直接返回
    if temp.notna().mean() > 0.8:
        return temp

    # 如果不行，尝试提取时间部分再拼接
    # 先转成字符串，提取 HH:MM 部分
    series_str = series.astype(str)
    # 正则提取 HH:MM
    time_only = series_str.str.extract(r'(\d{1,2}:\d{2})')[0]
    # 拼接日期
    full_time_str = "2023-01-01 " + time_only
    # 再转 datetime
    return pd.to_datetime(full_time_str, format="%Y-%m-%d %H:%M", errors="coerce")


# 应用安全的时间解析
df["customer_pay_time"] = parse_time_safely(df["customer_pay_time"])

# 【关键】打印清洗后的时间，看看是不是成功了
print("✅ 时间清洗完成！清洗后 customer_pay_time 样本（前5行）：")
print(df["customer_pay_time"].head().to_string())
print(f"   非空值数量：{df['customer_pay_time'].notna().sum()} / {len(df)}")

# 3. 重置索引
df = df.reset_index(drop=True)

# ==============================================
# 【第三步】筛选S级订单
# ==============================================
print("\n" + "=" * 80)
print("第三步：筛选S级订单")
print("=" * 80)

df_s = df[df["uav_priority"].str.contains("S级|医疗", na=False)].copy()
df_s = df_s.reset_index(drop=True)
print(f"✅ 筛选出S级订单数：{len(df_s)}")

# 保存S级订单
df_s.to_csv("S级医疗健康订单_筛选结果.csv", index=False, encoding="utf-8-sig")
print(f"🎉 S级订单已保存")

# ==============================================
# 【第四步】分析时间段（增加了更多检查）
# ==============================================
print("\n" + "=" * 80)
print("第四步：分析S级订单集中时间段")
print("=" * 80)

if len(df_s) == 0:
    print("❌ 没有S级订单")
elif df_s["customer_pay_time"].notna().sum() == 0:
    print("❌ S级订单的 customer_pay_time 全是空值，无法分析")
    print("   请检查第一步打印的‘原始时间格式’")
else:
    # 提取小时
    df_s["order_hour"] = df_s["customer_pay_time"].dt.hour

    print("📊 S级订单每小时分布：")
    hourly_counts = df_s["order_hour"].value_counts().sort_index()
    for hour in sorted(hourly_counts.index):
        print(f"   {hour:02d}:00 - {hour + 1:02d}:00：{hourly_counts[hour]} 单")

    print("\n" + "-" * 80)
    print("🎯 【核心结论】S级订单最集中的三个时间段：")
    top3 = df_s["order_hour"].value_counts().head(3)
    for i, (hour, cnt) in enumerate(top3.items(), 1):
        print(f"   第{i}名：{hour:02d}:00 - {hour + 1:02d}:00，共 {cnt} 单")
    print("=" * 80)