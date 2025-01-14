import click
from .record_resource import track_rs
from .show_nodes import show_nodes
from .show_status import show_status

@click.group('SGEViewer')
def cli():
    '''\b
    用于SGE系统的节点资源查看，任务资源显示和消耗记录的工具
    Tools for better recording and visualizing computer resources of Sun Grid Engine(SGE) System
    '''
    pass

@cli.command('track')
@click.argument('cmd', nargs=-1, required=False)
@click.option('--interval', '-i', default=10, help='检查任务状态的时间间隔（秒），默认为10秒。')
@click.option('--plot-only','-p', help='制定一个json文件，只绘图')
def track(**kwargs):
    '''\b
    跟踪任务资源消耗情况，并生成图片和总结报告
    示例：
        执行命令:
        SGEViewer track "qsub -V -l vf=20G -q SC1.q -cwd  work.novoauto_run.sh" 
        后台执行:
        SGEViewer track "qsub -V -l vf=20G -q SC1.q -cwd  work.novoauto_run.sh" &> run.log &
        仅绘图:
        SGEViewer track -p track_7355217.json
    '''
    track_rs(**kwargs)


@cli.command('nodes')
@click.option("--user","-u",default=None,help="指定用户(默认为当前用户)")
def show_nodes_cmd(**kwargs):
    """\b
    展示指定用户所有可用节点的资源使用情况
    """
    show_nodes(**kwargs)

@cli.command('status')
@click.option('-u', '--user', default=None, help="指定用户")
@click.option('-j', '--job_id', default=None, help="指定作业ID")
@click.option('-d','--detail', is_flag=True, help="启用详细输出格式")
@click.option('-o','--one_line', is_flag=True, help="一行展示信息")
@click.option("-q", "--queue", help="要过滤的队列，可指定多个，示例：-q gpu.q,bigmem.q")
@click.option("-n", "--node", help="要过滤的节点，可指定多个，示例：-n tjcompute001,tjcompute002")
def show_status_cmd(**kwargs):
    """\b
    更全面和方便的展示投递任务情况
    使用示例：
        SGEViwer status  
        SGEViwer status -u user_name
        SGEViwer status -u user_name -d

    """
    show_status(**kwargs)

def main():
    cli()

if __name__ == "__main__":
    cli()