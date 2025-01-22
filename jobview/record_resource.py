#!/usr/bin/env python3

import click
import subprocess
import json
import time
import os
import re
from datetime import datetime
import matplotlib.pyplot as plt
import sys
import matplotlib as mpl
import plotly.graph_objects as go
import plotly.io as pio
import humanfriendly
# 全局设置字体属性
import matplotlib.pyplot as plt
from matplotlib import rcParams
import os
from matplotlib.font_manager import fontManager

# 获取当前脚本所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 构建 SimHei.ttf 字体文件的路径
font_path = os.path.join(current_dir, 'src', 'SimHei.ttf')
# font_path = '/TJPROJ6/SC/personal_dir/chenming/research/SGE_manager/scripts/src/SimHei.ttf'

# 打印字体路径以确保路径正确
print(f"Font path: {font_path}")

# 在 rcParams 中设置全局字体
rcParams['font.sans-serif'] = ['SimHei']  # 使用 SimHei 字体
rcParams['font.family'] = 'sans-serif'
rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 手动添加字体路径到 Matplotlib 字体管理器中
fontManager.addfont(font_path)


def parse_usage_line(job_id,qstat_j_output):
    """
    解析 qstat -j 输出中的资源使用信息，返回包含当前时间的字典。
    """
    usage = {}
    for line in qstat_j_output.split('\n'):
        usage_match = re.search(r'usage\s+1:\s+(.+)', line)
        if usage_match: 
            # usage 1: cpu=00:01:21, mem=3.16686 GB s, io=0.31564 GB, vmem=151.582M, maxvmem=220.480M
            # 提取 'usage' 后的部分 : 
            usage_part = usage_match.group(1)
            print(usage_part)
            # 按逗号分割各项资源
            parts = usage_part.split(',')
            for part in parts:
                key_value = part.strip().split('=')
                if len(key_value) == 2:
                    key, value = key_value
                    key = key.strip().lower()
                    value = value.strip()
                    if key == 'cpu':
                        usage['cpu_time_sec_str'] = value
                        if value.lower() == 'n/a':
                            usage['cpu_time_sec'] = 0
                        else:
                            # 解析 CPU 时间，例如 "03:58:38"
                            cpu_match = re.match(r'(\d+):(\d+):(\d+)', value)
                            if cpu_match:
                                hours, minutes, seconds = map(int, cpu_match.groups())
                                cpu_time_sec = hours * 3600 + minutes * 60 + seconds
                                usage['cpu_time_sec'] = cpu_time_sec

                    elif key == 'mem':
                        # 时间和内存的乘积：mem=26565228.46810 GB s 表示你的作业在执行过程中内存使用和时间的积累值。
                        # 例如，如果你的作业运行了1000秒，并且在这段时间内平均使用了26.565 GB的内存，
                        # 那么 26565228.46810 GB s = 26.565 GB * 1000秒。
                        usage['accumulate_memory_gbs_str'] = value
                        if value.lower() == 'n/a':
                            usage['accumulate_memory_gbs'] = 0 # 累计内存使用量
                        else:
                            accumulate_memory_bite = humanfriendly.parse_size(value,binary=False)
                            usage['accumulate_memory_gbs'] = accumulate_memory_bite /1000 ** 3

                    elif key == 'io': # io 是一个累计值，表示作业在整个执行期间的总 I/O 量。
                        usage['io_gb_str'] = value
                        if value.lower() == 'n/a':
                            usage['io_gb'] = 0
                        else:
                            io_bite = humanfriendly.parse_size(value,binary=False)
                            usage['io_gb'] = io_bite / 1000 ** 3
                    elif key == 'vmem':
                        usage['vmem_gb_str'] = value
                        if value.lower() == 'n/a':
                            usage['vmem_gb'] = 0
                        else:
                            vmem_bite = humanfriendly.parse_size(value,binary=False)
                            usage['vmem_gb'] = vmem_bite / 1000 ** 3
                    elif key == 'maxvmem':
                        usage['max_vmem_gb_str'] = value
                        if value.lower() == 'n/a':
                            usage['max_vmem_gb'] = 0
                        else:
                            max_vmem_bite = humanfriendly.parse_size(value,binary=False)
                            usage['max_vmem_gb'] = max_vmem_bite / 1000 ** 3
            break
    if usage:
        # 添加当前时间
        usage['timestamp'] = datetime.now().isoformat()
        usage['job_id'] = job_id
    
    return usage

def plot_usage(json_file,output_prefix):
     # 任务完成后，生成图表
    # try:
    if not os.path.exists(json_file):
        click.echo("未收集到数据，跳过绘图。")
        return

    with open(json_file, 'r') as f:
        data = json.load(f)
    if len(data) == 0:
        click.echo("未收集到数据点，跳过绘图。")
        return
    job_id = data[0].get("job_id")
    # 将时间戳转换为 datetime 对象
    times = [datetime.fromisoformat(d['timestamp']) for d in data]
    cpu_times = [d.get('cpu_time_sec', 0) for d in data]
    mem_usage = [d.get('vmem_gb', 0) for d in data]
    io_usage = [d.get('io_gb', 0) for d in data]
    cpu_times_hours = [t / 3600 for t in cpu_times]
    # 绘制 CPU 时间图
    plt.figure(figsize=(10,5))
    plt.plot(times, cpu_times_hours, label='CPU 时间 (小时)', marker='o')
    plt.xlabel('时间')
    plt.ylabel('CPU 时间 (小时)')
    plt.title(f'任务 {job_id} 的 CPU 时间使用情况')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    cpu_plot = f'cpu_usage_{job_id}.png'
    plt.savefig(cpu_plot)
    plt.close()

    # 绘制内存使用图
    plt.figure(figsize=(10,5))
    plt.plot(times, mem_usage, label='内存使用 (GB)', color='orange', marker='o')
    plt.xlabel('时间')
    plt.ylabel('内存使用 (GB)')
    plt.title(f'任务 {job_id} 的内存使用情况')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    mem_plot = f'memory_usage_{job_id}.png'
    plt.savefig(mem_plot)
    plt.close()

    # 绘制内存使用图
    plt.figure(figsize=(10,5))
    plt.plot(times, io_usage, label='IO累计使用 (GB)', color='green', marker='o')
    plt.xlabel('时间')
    plt.ylabel('IO累计使用 (GB)')
    plt.title(f'任务 {job_id} 的IO使用情况')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    io_plot = f'IO_usage_{job_id}.png'
    plt.savefig(io_plot)
    plt.close()

    click.echo(f"图表已保存为 {cpu_plot}, {mem_plot} 和 {io_plot}")
    # 生成 Plotly 报告
    generate_plotly_report(data, job_id, output_prefix)

    # except Exception as e:
    #     click.echo(f"生成图表失败: {e}")



def generate_plotly_report(data, job_id, output_prefix):
    """
    使用 Plotly 生成交互式报告，包含：
      1. 一个汇总表格：展示任务时长、累计 CPU 时间、最大内存使用、平均 CPU 时间等信息
      2. CPU 使用趋势图
      3. 内存使用趋势图
    最终合并为一个 HTML 文件输出
    """

    # === 数据准备部分 ===
    # 将 timestamp 转为 datetime 方便画图
    times = [datetime.fromisoformat(d['timestamp']) for d in data]
    cpu_times = [d.get('cpu_time_sec', 0) for d in data]
    mem_usage = [d.get('vmem_gb', 0) for d in data]
    io_usage = [d.get('io_gb', 0) for d in data]
    total_cpu_times_str = data[-1].get("cpu_time_sec_str",0) 
    last_acc_mem_str = data[-1].get('accumulate_memory_gbs_str', 0)
    last_acc_mem = data[-1].get('accumulate_memory_gbs', 0)
    last_cpu_sec = data[-1].get('cpu_time_sec', 0)
    
     # 如果 cpu_time_sec 为0，则避免除以0的错误
    if last_cpu_sec > 0:
        avg_mem_usage_gb = last_acc_mem / last_cpu_sec
    else:
        avg_mem_usage_gb = 0

    cpu_times_hours = [t / 3600 for t in cpu_times]
    # 计算任务时长（第一个点到最后一个点）
    if len(times) > 1:
        job_duration_seconds = (times[-1] - times[0]).total_seconds()
    else:
        job_duration_seconds = 0

    # 将秒数转成人类友好的 "HH:MM:SS" 形式
    job_duration_str = time.strftime('%H:%M:%S', time.gmtime(job_duration_seconds))

    # 累计 CPU 时间、最大/平均内存
    max_memory_usage_gb = data[-1].get('max_vmem_gb_str',0)

     # --- 1) 生成表格 ---
    table_data = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[
                        '指标', 
                        '值'
                    ],
                    fill_color='paleturquoise',
                    align='left'
                ),
                cells=dict(
                    values=[
                        [  
                            '任务时长 (小时)',
                            'CPU 时间 (小时)',
                            '累计内存 (GB·s)',
                            '最大内存使用 (GB)',
                            '平均内存 (GB)'
                        ],
                        [
                            f'{job_duration_str}',
                            f'{total_cpu_times_str}',
                            f'{last_acc_mem_str}',
                            f'{max_memory_usage_gb}',
                            f'{avg_mem_usage_gb:.2f}'
                        ]
                    ],
                    fill_color='lavender',
                    align='left'
                )
            )
        ]
    )

    table_data.update_layout(
    title=f'任务 {job_id} 的资源使用汇总',            # 若不需要标题，可以去掉
    height=200,           # 适当调小图表整体高度
    margin=dict(l=10, r=10, t=50, b=10),  # 四边外边距都设为 0
)

    # === 2) CPU 使用趋势图 ===
    fig_cpu = go.Figure()
    fig_cpu.add_trace(
        go.Scatter(
            x=times,
            y=cpu_times_hours,
            mode='lines+markers',
            name='CPU 时间 (小时)'
        )
    )
    fig_cpu.update_layout(
        title=f'任务 {job_id} 的 CPU 时间变化',
        xaxis_title='时间',
        yaxis_title='CPU 时间 (小时)',
        margin=dict(l=40, r=20, t=40, b=20),
        height=400
    )

    # === 3) 内存使用趋势图 ===
    fig_mem = go.Figure()
    fig_mem.add_trace(
        go.Scatter(
            x=times,
            y=mem_usage,
            mode='lines+markers',
            name='内存使用 (GB)',
            line=dict(color='orange')
        )
    )
    fig_mem.update_layout(
        title=f'任务 {job_id} 的内存使用变化',
        xaxis_title='时间',
        yaxis_title='内存使用 (GB)',
        margin=dict(l=40, r=20, t=40, b=20),
        height=400
    )

    # === 4) IO使用趋势图 ===
    fig_io = go.Figure()
    fig_io.add_trace(
        go.Scatter(
            x=times,
            y=io_usage,
            mode='lines+markers',
            name='IO使用 (GB)',
            line=dict(color='green')
        )
    )
    fig_io.update_layout(
        title=f'任务 {job_id} 的IO累计变化',
        xaxis_title='时间',
        yaxis_title='IO累计变化图 (GB)',
        margin=dict(l=40, r=20, t=40, b=20),
        height=400
    )

    table_html = pio.to_html(
                table_data, 
                full_html=False, 
            )
    cpu_html =  pio.to_html(
                fig_cpu, 
                full_html=False, 
                include_plotlyjs=False
            )
    mem_html =  pio.to_html(
                fig_mem, 
                full_html=False, 
                include_plotlyjs=False
            )
    io_html =  pio.to_html(
                fig_io, 
                full_html=False, 
                include_plotlyjs=False
            )
    bootstrap_css = r"""
        html {
        line-height: 1.15;
        -webkit-text-size-adjust: 100%;
        }
        body {
        margin: 0;
        font-family: "Helvetica Neue", Arial, sans-serif;
        font-size: 1rem;
        font-weight: 400;
        line-height: 1.5;
        color: #212529;
        background-color: #f8f9fa;
        }
        .container {
        width: 100%;
        padding-right: 12px;
        padding-left: 12px;
        margin-right: auto;
        margin-left: auto;
        }
        @media (min-width: 576px) {
        .container {
            max-width: 540px;
        }
        }
        @media (min-width: 768px) {
        .container {
            max-width: 720px;
        }
        }
        @media (min-width: 992px) {
        .container {
            max-width: 960px;
        }
        }
        @media (min-width: 1200px) {
        .container {
            max-width: 1140px;
        }
        }
        .row {
        display: flex;
        flex-wrap: wrap;
        margin-right: -12px;
        margin-left: -12px;
        }
        [class*="col-"] {
        position: relative;
        width: 100%;
        padding-right: 12px;
        padding-left: 12px;
        }
        .col-12 {
        flex: 0 0 100%;
        max-width: 100%;
        }
        .col-md-6 {
        flex: 0 0 50%;
        max-width: 50%;
        }
        .my-4 {
        margin-top: 1.5rem!important;
        margin-bottom: 1.5rem!important;
        }
        .mb-4 {
        margin-bottom: 1.5rem!important;
        }
        .text-center {
        text-align: center!important;
        }
        .text-start {
        text-align: start!important;
        }
        h2 {
        margin-top: 24px;
        margin-bottom: 12px;
        }
        p {
        margin-top: 0;
        margin-bottom: 1rem;
        }
            """

    html_template = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Resource Report</title>
    <style>
    {bootstrap_css}
    </style>
</head>
<body>
<div class="container">
<h2 id="bar-part" class="my-4">任务资源消耗统计</h2>
    <div class="row">
        <div class="col-md-12">{table_html}</div>
        <div class="col-md-12">{cpu_html}</div>
        <div class="col-md-12">{mem_html}</div>
        <div class="col-md-12">{io_html}</div>
    </div>
</div>
</body>
</html>
"""

    # 定义输出文件
    if output_prefix:
        report_file = f"{output_prefix}_resource_report_{job_id}.html"
    else:
        report_file = f"resource_report_{job_id}.html"

    with open(report_file, 'w', encoding='utf-8') as f:
        # 1. 写入 HTML 头部
        f.write(html_template)

    click.echo(f"Plotly 报告已保存为 {report_file}")


def track_rs(interval, cmd, plot_only,output_prefix=None):
    """
    跟踪 qsub 提交的任务的资源消耗。

    使用方法：track_rs [OPTIONS] <CMD>...
    例如：
        track_rs "qsub -l vf=2G work.sh"
        track_rs -i 60 "qsub -l vf=2G work.sh"
        track_rs -p track.json # 只绘图

    """
    retry = 20
    if plot_only:
        plot_usage(plot_only,output_prefix)
        return

    if len(cmd) == 0:
        click.echo("未提供要执行的命令。")
        sys.exit(1)

    # 提交任务并获取输出
    # try:
    bash_cmd = f"source ~/.bash_profile && {' '.join(cmd)}"
    # bash_cmd = f"{' '.join(cmd)}"
    print(bash_cmd)
    result = subprocess.run(bash_cmd, capture_output=True,text=True, shell=True)
    cmd_output = result.stdout
    # 尝试从输出中提取任务 ID: Your job 6885284 ("work.sh") has been submitted
    print(cmd_output)
    job_id_match = re.search(r'\b(\d+)\b', cmd_output)
    if job_id_match:
        job_id = job_id_match.group(1)
        click.echo(f"已提交任务，任务 ID: {job_id}")
    else:
        click.echo("无法从命令输出中解析出任务 ID。请确保提交命令输出包含任务 ID。")
        click.echo(f"命令输出: {cmd_output}")
        sys.exit(1)

    # except subprocess.CalledProcessError as e:
    #     click.echo(f"提交任务失败: {e.stderr.strip()}")
    #     sys.exit(1)

    # 定义 JSON 文件名
    json_file = f"track_{job_id}.json"
    data = []

    click.echo(f"开始跟踪任务 {job_id} 的资源消耗，每 {interval} 秒检查一次...")

    try:
        while True:
            # 检查任务是否仍在运行
            try:
                qstat_output = subprocess.run(['qstat'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True).stdout
                if not re.search(rf'\b{job_id}\b', qstat_output):
                    click.echo(f"任务 {job_id} 已完成。")
                    break
            except subprocess.CalledProcessError as e:
                click.echo(f"运行 qstat 失败: {e.stderr.strip()}")
                retry -= 1
                time.sleep(interval)
                if retry < 0:
                    click.secho(f"在qstat运行失败{retry}次后退出...",fg="red")
                    break

            # 获取任务资源使用情况
            try:
                qstat_j_output = subprocess.run(['qstat', '-j', job_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True).stdout
                usage = parse_usage_line(job_id,qstat_j_output)
                if usage:
                    data.append(usage)
                    # 保存到 JSON 文件
                    with open(json_file, 'w') as f:
                        json.dump(data, f, indent=2)
                    click.echo(f"收集到数据于 {usage['timestamp']}: CPU {usage.get('cpu_time_sec', 'N/A')} 秒, 内存 {usage.get('accumulate_memory_gbs', 'N/A')} GB")
            except subprocess.CalledProcessError as e:
                click.echo(f"运行 qstat -j {job_id} 失败: {e.stderr.strip()}")
                if "Following jobs do not exist" in e.stderr:
                    break
                
            time.sleep(interval)
    except KeyboardInterrupt:
        click.echo("跟踪被用户中断。")

    plot_usage(json_file)

if __name__ == '__main__':
    track_rs(10,'ls',plot_only="/TJPROJ6/SC/personal_dir/chenming/test/cellranger_test/test20250121/track_8465466.json")
