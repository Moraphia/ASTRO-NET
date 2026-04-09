import pandas as pd
import numpy as np

# ==============================================
# 【第一步】导入文件
# ==============================================
print("=" * 80)
print("第一步：导入文件")
print("=" * 80)

file_path = "/Users/zhangyt/Desktop/美团比赛/无人机高价值订单_严格约束版.csv"
df = pd.read_csv(file_path, encoding="utf-8-sig")
print(f"✅ 文件导入成功！原始订单数：{len(df)}")

# ==============================================
# 【第二步】【修复】自适应时间清洗
# ==============================================
print("\n" + "=" * 80)
print("第二步：自适应时间清洗")
print("=" * 80)

# 1. 数值型转换（保留）
df["pickup_latitude"] = pd.to_numeric(df["pickup_latitude"], errors="coerce")
df["pickup_longitude"] = pd.to_numeric(df["pickup_longitude"], errors="coerce")
df["dropoff_latitude"] = pd.to_numeric(df["dropoff_latitude"], errors="coerce")
df["dropoff_longitude"] = pd.to_numeric(df["dropoff_longitude"], errors="coerce")
df["package_price"] = pd.to_numeric(df["package_price"], errors="coerce")


# 2. 【核心修复】自适应时间解析
def parse_time(col):
    # 先看看是不是已经是 datetime 了
    if pd.api.types.is_datetime64_any_dtype(col):
        return col

    # 尝试直接转
    temp = pd.to_datetime(col, errors="coerce")
    if temp.notna().mean() > 0.5:
        return temp

    # 如果是纯数字（比如 "1130" 代表 11:30）
    col_str = col.astype(str).str.zfill(4)
    hours = col_str.str[:2]
    mins = col_str.str[2:]
    time_str = hours + ":" + mins
    return pd.to_datetime("2023-01-01 " + time_str, errors="coerce")


df["customer_pay_time"] = parse_time(df["customer_pay_time"])

# 【验证】打印结果
print("✅ 时间清洗结果：")
print(f"   非空数量：{df['customer_pay_time'].notna().sum()} / {len(df)}")
if df['customer_pay_time'].notna().sum() > 0:
    print("   样本：", df["customer_pay_time"].dropna().iloc[0])

# 3. 重置索引
df = df.reset_index(drop=True)

# ==============================================
# 【第三步】筛选时间段
# ==============================================
print("\n" + "=" * 80)
print("第三步：筛选午间高峰")
print("=" * 80)

if df["customer_pay_time"].notna().sum() == 0:
    print("❌ 时间列全空，请先运行上面的‘诊断代码’看看原始数据！")
else:
    # 提取小时和分钟
    df["hour"] = df["customer_pay_time"].dt.hour
    df["minute"] = df["customer_pay_time"].dt.minute

    # 筛选 11:30 - 12:30
    # 逻辑：(11点且>=30分) 或者 (12点且<=30分)
    mask = (
            ((df["hour"] == 11) & (df["minute"] >= 30)) |
            ((df["hour"] == 12) & (df["minute"] <= 30))
    )
    df_peak = df[mask].copy().reset_index(drop=True)

    print(f"✅ 筛选出 11:30-12:30 订单数：{len(df_peak)}")

    # ==============================================
    # 【第四步】抽样
    # ==============================================
    print("\n" + "=" * 80)
    print("第四步：抽样（20个S级 + 180个其他）")
    print("=" * 80)

    if len(df_peak) < 200:
        print(f"⚠️  该时间段只有 {len(df_peak)} 单，不足200，将全部抽取")
        df_sample = df_peak
    else:
        # 分开S级和非S级
        is_s = df_peak["uav_priority"].str.contains("S级|医疗", na=False)
        df_s = df_peak[is_s]
        df_other = df_peak[~is_s]

        print(f"   该时间段内 S级：{len(df_s)} 单，非S级：{len(df_other)} 单")

        # 抽S级（最多20）
        n_s = min(20, len(df_s))
        sample_s = df_s.sample(n=n_s, random_state=42)

        # 抽非S级（补够200）
        n_other = 200 - n_s
        sample_other = df_other.sample(n=n_other, random_state=42)

        # 合并打乱
        df_sample = pd.concat([sample_s, sample_other]).sample(frac=1, random_state=42).reset_index(drop=True)

    print("\n✅ 抽样完成！结果分布：")
    print(df_sample["uav_priority"].value_counts())

    # ==============================================
    # 【第五步】保存
    # ==============================================
    output_path = "../目标枢纽午高峰抽样订单_200个.csv"
    df_sample.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n🎉 已保存至：{output_path}")