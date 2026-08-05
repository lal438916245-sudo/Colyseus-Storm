import os
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

def analyze_locust_data(history_file, stats_file, failures_file, output_dir="reports"):
    """
    分析 Locust 压测数据
    history_file:  *_stats_history.csv  (每秒粒度的时序数据)
    stats_file:    *_stats.csv          (汇总数据)
    failures_file: *_failures.csv       (失败汇总)
    output_dir:    输出目录
    """
    os.makedirs(output_dir, exist_ok=True)

    output_csv = os.path.join(output_dir, "colyseus_performance_report.csv")
    output_png = os.path.join(output_dir, "colyseus_performance_report.png")

    # ========== 1. 读取数据 ==========
    df = pd.read_csv(history_file, skipinitialspace=True, encoding='utf-8')
    df_stats = pd.read_csv(stats_file, skipinitialspace=True, encoding='utf-8')
    df_fail = pd.read_csv(failures_file, skipinitialspace=True, encoding='gbk')

    # ========== 2. 数据清洗：剔除前15%爬坡期和后5%断连期 ==========
    start_idx = int(len(df) * 0.15)
    end_idx = int(len(df) * 0.95)
    clean_df = df.iloc[start_idx:end_idx].copy()

    # ========== 收集所有结果行 ==========
    result_rows = []

    print("=" * 60)
    print("  Colyseus-Storm 性能诊断报告")
    print("=" * 60)

    # --- 各接口汇总 ---
    for _, row in df_stats.iterrows():
        rtype = row.get('Type', '')
        name = row.get('Name', '')
        if pd.isna(name) or name == '' or name == 'Aggregated':
            continue
        label = f"[{rtype}] {name}" if rtype else name
        print(f"\n--- {label} ---")
        data = {
            "类别": label,
            "请求总数": int(row['Request Count']),
            "失败数": int(row['Failure Count']),
            "平均RPS": round(row['Requests/s'], 2),
            "失败RPS": round(row['Failures/s'], 2),
            "中位响应(ms)": round(row['Median Response Time'], 0),
            "平均响应(ms)": round(row['Average Response Time'], 0),
            "最小响应(ms)": round(row['Min Response Time'], 0),
            "最大响应(ms)": round(row['Max Response Time'], 0),
            "P50(ms)": row.get('50%', 'N/A'),
            "P90(ms)": row.get('90%', 'N/A'),
            "P95(ms)": row.get('95%', 'N/A'),
            "P99(ms)": row.get('99%', 'N/A'),
        }
        result_rows.append(data)
        for k, v in data.items():
            print(f"  {k}: {v}")

    # --- 稳定期聚合指标 ---
    max_users = clean_df['User Count'].max()
    agg_clean = clean_df[clean_df['Name'] == 'Aggregated']
    max_rps = agg_clean['Requests/s'].max()
    avg_rps = agg_clean['Requests/s'].mean()
    max_fail_rps = agg_clean['Failures/s'].max()

    for col in ['50%', '95%', '99%']:
        agg_clean[col] = pd.to_numeric(agg_clean[col], errors='coerce')

    median_p50 = agg_clean['50%'].median()
    max_p95 = agg_clean['95%'].max()

    print(f"\n{'=' * 60}")
    print(f"  稳定期聚合指标 (剔除前15%爬坡 + 后5%断连)")
    print(f"{'=' * 60}")

    summary_data = {
        "类别": "稳定期聚合指标",
        "并发用户数": max_users,
        "平均吞吐量(req/s)": round(avg_rps, 2),
        "峰值吞吐量(req/s)": round(max_rps, 2),
        "峰值失败率(req/s)": round(max_fail_rps, 2),
        "中位数响应P50(ms)": round(median_p50, 0),
        "长尾响应P95(ms)": round(max_p95, 0),
    }
    result_rows.append(summary_data)
    for k, v in summary_data.items():
        print(f"  {k}: {v}")
    print(f"{'=' * 60}")

    # --- 失败分布 ---
    if len(df_fail) > 0:
        print(f"\n--- 失败请求分布 ---")
        for _, row in df_fail.iterrows():
            err_info = str(row.get('Error', ''))[:200]
            fail_data = {
                "类别": "失败分布",
                "接口": f"[{row.get('Method', '')}] {row.get('Name', '')}",
                "发生次数": int(row['Occurrences']),
                "错误信息": err_info,
            }
            result_rows.append(fail_data)
            print(f"  [{row.get('Method', '')}] {row.get('Name', '')}: {int(row['Occurrences'])} 次")
            print(f"    错误: {err_info}")

    # ========== 保存 CSV ==========
    df_out = pd.DataFrame(result_rows)
    df_out.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"\nCSV 报告已保存为: {output_csv}")

    # ========== 图表 ==========
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    ax1 = axes[0][0]
    ax1_twin = ax1.twinx()
    ax1.plot(clean_df.index, agg_clean['Requests/s'], color='#2ca02c', linewidth=1, label='RPS')
    ax1_twin.plot(clean_df.index, agg_clean['Failures/s'], color='#d62728', linewidth=1, label='Failures/s')
    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('Requests/s', color='#2ca02c')
    ax1_twin.set_ylabel('Failures/s', color='#d62728')
    ax1.set_title('Throughput & Failure Rate (Stable Period)')
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_twin.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    ax2 = axes[0][1]
    ax2.plot(clean_df.index, clean_df['User Count'], color='#1f77b4', linewidth=1)
    ax2.set_xlabel('Time (s)')
    ax2.set_ylabel('Users')
    ax2.set_title('Concurrent Users (Stable Period)')

    ax3 = axes[1][0]
    ax3.plot(clean_df.index, agg_clean['50%'], color='#2ca02c', linewidth=1, label='P50')
    ax3.plot(clean_df.index, agg_clean['95%'], color='#ff7f0e', linewidth=1, label='P95')
    ax3.plot(clean_df.index, agg_clean['99%'], color='#d62728', linewidth=1, label='P99')
    ax3.set_xlabel('Time (s)')
    ax3.set_ylabel('Response Time (ms)')
    ax3.set_title('Response Time Percentiles (Stable Period)')
    ax3.legend(loc='upper left')

    ax4 = axes[1][1]
    ax4.scatter(clean_df['User Count'], agg_clean['Requests/s'], alpha=0.4, s=10, color='#1f77b4')
    ax4.set_xlabel('Concurrent Users')
    ax4.set_ylabel('Requests/s')
    ax4.set_title('RPS vs Concurrent Users')

    plt.tight_layout()
    plt.savefig(output_png, dpi=150)
    plt.show()
    print(f"图表已保存为: {output_png}")


if __name__ == "__main__":
    analyze_locust_data(
        history_file="reports/colyseus_test_run_stats_history.csv",
        stats_file="reports/colyseus_test_run_stats.csv",
        failures_file="reports/colyseus_test_run_failures.csv",
        output_dir="reports"
    )
