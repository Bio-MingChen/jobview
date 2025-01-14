## jobview

这个工具主要用于 SGE 系统的节点资源查看，任务资源显示和消耗记录的工具
简单来说，它是基于`qstat`, `qhost`, `qselect`等命令的信息，提供资源消耗的跟踪和更好的可视化

1. `jobview nodes` 可以展示用户所属节点的任务情况，资源使用，是否负载高等
2. `jobview status`以更好的可视化方式展示任务情况，提供资源消耗展示
3. `jobview track` 跟踪任务的资源消耗情况，并记录和生成一个可视化报告

### Installation

```shell
pip install jobview
```

### Usage

主要有三个子命令，如下所示:

```python
Usage: jobview [OPTIONS] COMMAND [ARGS]...

  用于SGE系统的节点资源查看，任务资源显示和消耗记录的工具
  Tools for better recording and visualizing computer resources of Sun Grid Engine(SGE) System

Options:
  --help  Show this message and exit.

Commands:
  nodes   展示指定用户所有可用节点的资源使用情况
  status  更全面和方便的展示投递任务情况
  track   跟踪任务资源消耗情况，并生成图片和总结报告

```

#### 查看所有节点

查看用户所属节点情况
`jobview nodes` 输出如下：
![nodes](./docs/image.png)

投递一个任务并在后台监控其资源消耗
`jobview track "qsub -V -l vf=2G -q SC1.q -cwd work.sh" &> run.log &`

得到如下报告：
![report](./docs/image_report.png)

### 子命令参数

```python
$jobview nodes --help
Usage: jobview nodes [OPTIONS]

  展示指定用户所有可用节点的资源使用情况

Options:
  -u, --user TEXT  指定用户(默认为当前用户)
  --help           Show this message and exit.

$jobview status --help
Usage: jobview status [OPTIONS]

  更全面和方便的展示投递任务情况
  使用示例：
      SGEViwer status
      SGEViwer status -u user_name
      SGEViwer status -u user_name -d

Options:
  -u, --user TEXT    指定用户
  -j, --job_id TEXT  指定作业ID
  -d, --detail       启用详细输出格式
  -o, --one_line     一行展示信息
  -q, --queue TEXT   要过滤的队列，可指定多个，示例：-q gpu.q,bigmem.q
  -n, --node TEXT    要过滤的节点，可指定多个，示例：-n tjcompute001,tjcompute002
  --help             Show this message and exit.

$jobview track --help
Usage: jobview track [OPTIONS] [CMD]...

  跟踪任务资源消耗情况，并生成图片和总结报告
  示例：
      执行命令:
      SGEViewer track "qsub -V -l vf=20G -q SC1.q -cwd  work.sh"
      后台执行:
      SGEViewer track "qsub -V -l vf=20G -q SC1.q -cwd  work.sh" &> run.log &
      仅绘图:
      SGEViewer track -p track_7355217.json

Options:
  -i, --interval INTEGER  检查任务状态的时间间隔（秒），默认为10秒。
  -p, --plot-only TEXT    制定一个json文件，只绘图
  --help                  Show this message and exit.

```
