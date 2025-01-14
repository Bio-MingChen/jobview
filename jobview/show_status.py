import subprocess
import click
import os
import re
import datetime as dt
from .show_nodes import parse_qhost_q_j_F

def get_current_user():
    """ 获取当前用户的用户名 """
    return os.getenv("USER")


def execute_command(command):
    """ 执行命令并返回输出 """
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
    if result.returncode != 0:
        raise Exception(f"命令执行失败: {result.stderr}")
    return result.stdout


def get_job_info_by_user(user):
    """ 获取用户所有作业的 job_id 和 queue 信息 """
    cmd = f"qstat -u {user}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    job_info = {}
    for line in result.stdout.splitlines():
        # 跳过表头和空行
        if line.startswith("job-ID") or not line.strip():
            continue
        
        # 提取 job_id 和 queue 信息
        match = re.search(r'(\d+)\s+.+\s+(\S+@[\S]+)', line)
        if match:
            job_id, queue = match.groups()
            job_info[job_id] = queue
            # print(f"Job ID: {job_id}, Queue: {queue}")  # 可选: 输出以调试
    # print(job_info)
    return job_info




def get_job_details(job_ids):
    """ 获取指定作业ID的详细信息 """
    job_ids_str = ",".join(job_ids)
    command = f"qstat -j {job_ids_str}"
    print(command)
    output = execute_command(command)
    return output

def print_item(item,one_line=False):
    """
    打印有颜色的结果
    """
    if one_line:
        one_line_fmt = f"{item['job_num']} {item.get('exec_host')} {item['job_id']} {item['owner']} {item['job_name']} {item['submit_time']} {item['usage']}"
        click.secho(one_line_fmt,fg='magenta')
    else:
        click.secho(f'[{item["job_num"]}]' + "=" * 60)
        click.secho(f"Jobinfo:      {item['owner']} {item['job_id']} {item['job_name']}",fg='yellow')
        click.secho(f"Useage:       {item['usage']}",fg='cyan')
        click.secho(f"Submit_time:  {item['submit_time']}",fg='magenta')
        click.secho(f"Directory:        {item['cwd']} {item['shell']}",fg='green')
        click.secho(f"Queue:        {item.get('exec_host')}",fg='blue')

def format_qstat_output(output,job_info,detail=False,one_line=False):
    """ 格式化 qstat -j 输出并添加中文字段名称 """
    lines = output.splitlines()
    formatted_output = []
    job_num = 0
    if not detail:
        # 精简输出格式
        item = {
            'job_id':'',
            'owner':'',
            'job_name':'',
            'usage':'',
            'submit_time':'',
            'cmd':'',
            'shell':'',
            'exec_host':'',
        }

        for line in lines:

            # 提取作业信息
            job_id_match = re.search(r'job_number:\s+(\d+)', line)
            owner_re = re.search(r'owner:\s+(\S+)', line)
            job_name_match = re.search(r'job_name:\s+(\S+)', line)
            usage_match = re.search(r'usage\s+1:\s+(.+)', line)
            submit_time_match = re.search(r'submission_time:\s+(.+)', line)
            cwd_re = re.search(r'cwd:\s+(.+)', line)
            job_args_match = re.search(r"job_args:.+\s+(\S+)",line)

            if job_id_match:
                item['job_id'] = job_id_match.group(1) 
            elif owner_re:
                item['owner'] = owner_re.group(1)
            elif job_name_match:
                item['job_name'] = job_name_match.group(1) 
            elif usage_match:
                item['usage'] = usage_match.group(1) 
            elif submit_time_match:
                item['submit_time'] = submit_time_match.group(1) 
                if item['submit_time'] != '未知':
                    datetime_obj = dt.datetime.strptime(item['submit_time'], "%a %b %d %H:%M:%S %Y")
                    item['submit_time'] = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
            
            elif cwd_re:
                item['cwd'] =cwd_re.group(1)
            elif job_args_match:
                item['shell'] =job_args_match.group(1)

            # 获取作业所在的队列
            if ('=' * 20 in line) and (item.get('job_id')):
                if item.get('job_id'):
                    item['exec_host'] = job_info.get(item['job_id'], '未找到节点')
                    job_num += 1
                    item['job_num'] = job_num 
                    print_item(item,one_line=one_line) 
                    
        item['exec_host'] = job_info.get(item['job_id'], '未找到节点')
        job_num += 1
        item['job_num'] = job_num
        print_item(item,one_line=one_line) 
        
    else:
        # 添加中文字段名称
        field_mapping = {
            'job_number': '作业编号',
            # 'exec_file': '作业脚本路径',
            'job_name': '作业名称',
            'submission_time': '提交时间',
            'hard resource_list': '硬资源请求',
            'usage': '资源使用情况',
            'owner': '提交者',
            'uid': '用户ID',
            'group': '用户组',
            'gid': '组ID',
            # 'sge_o_home': '用户主目录',
            # 'sge_o_log_name': '日志名称',
            # 'sge_o_path': '路径',
            # 'sge_o_shell': 'shell',
            'sge_o_host': '主机',
            # 'sge_o_workdir': '工作目录',
            # 'account': '账户',
            'cwd': '当前工作目录',
            'stderr_path_list': '错误输出路径',
            # 'mail_list': '邮件通知列表',
            # 'notify': '邮件通知',
            'stdout_path_list': '标准输出路径',
            # 'jobshare': '作业共享',
            'hard_queue_list': '硬队列',
            'restart': '是否支持重启',
            # 'env_list': '环境变量',
            'job_args': '作业参数',
            # 'script_file': '脚本文件',
            # 'verify_suitable_queues': '验证适用队列',
            # 'binding': '绑定资源',
            # 'job_type': '作业类型',
        }

        job_data_list = []
        current_job_data = {}

        # 解析每一行并根据字段名填充 job_data
        for line in lines:
            if not line.strip():
                continue
            
            # 每个作业记录之间通过分隔符行（'==============================================================')分隔
            if "=" * 60 in line:
                if current_job_data:
                    job_data_list.append(current_job_data)
                current_job_data = {}
                continue
            
            for key, cn_name in field_mapping.items():
                if line.startswith(key):
                    current_job_data[key] = line.split(':', 1)[1].strip()

        # 如果最后一个作业数据没有添加，需要补充
        if current_job_data:
            job_data_list.append(current_job_data)

        # 格式化输出
        for job_data in job_data_list:
            formatted_output.append("=" * 60)
            for key, cn_name in field_mapping.items():
                if key in job_data:
                    combine_name = f'{key}({cn_name}):'
                    formatted_output.append(f"{combine_name:<35} {job_data[key]:<}")
            # 添加节点信息
            job_id = job_data.get('job_number', '')
            exec_host = job_info.get(job_id, '未找到节点')
            formatted_output.append(f"{'computer_node(所在节点):':<35} {exec_host}")

    return "\n".join(formatted_output)

# ================ 新增的辅助函数，调用 parse_qhost_q_j_F 得到节点/队列/作业信息 =================

def get_nodes_info():
    """
    调用 qhost -F -q -j 并用 parse_qhost_q_j_F 函数解析返回数据
    返回的数据结构类似:
    {
      'tjcompute327': {
        'cpu_used': 19.0,
        'cpu_total': 40.0,
        'vf': 80.503,
        'memtot': 188.3,
        'memuse': 59.8,
        'queues': {'cog1_dadou.q': {'over': False}, 'SC1.q': {'over': False}},
        'jobs': [{'job_id': '6586240', 'user': 'zhangkaijian'}, ...],
        'load_str': '196.0',
        'memf': 128.444
      },
      'tjcompute328': {...},
      ...
    }
    """
    cmd = "qhost -F -q -j"
    output = execute_command(cmd)
    lines = output.strip().split('\n')
    results = parse_qhost_q_j_F(lines)
    return results

# ================ 在主入口 show_status 里添加对 queue / node 的处理逻辑 =================

def show_status(user=None, job_id=None, detail=False, queue=None, node=None, one_line=None):
    """
    1. 如果指定 queue 或 node，则通过 get_nodes_info() 获取全部节点信息，
       根据 queue/node 做筛选，收集到需要查看的 job_id 列表，
       然后将这些 job_id 交给 get_job_details() + format_qstat_output() 输出即可。
       
    2. 如果 queue 和 node 均未指定，则走原先以 user/job_id 为中心的逻辑。
    """
    # 如果 queue 或 node 任意一个被指定，则从 qhost -F -q -j 的结构中筛选 job_id
    if queue or node:
        # 获取所有节点及其上挂载队列/作业信息
        nodes_info = get_nodes_info()

        # 用 set 存放匹配到的 job_id，避免重复
        matched_job_ids = set()
        jobid_queue_dict = {}
        # 如果指定了 queue(支持逗号分隔)
        if queue:
            queue_names = [q.strip() for q in queue.split(',') if q.strip()]
            # 遍历所有节点
            for hostname, info in nodes_info.items():
                # info['queues'] -> 形如 {'cog1_dadou.q': {'over': False}, ...}
                node_queues = info.get('queues', {})
                # 判断该节点有没有与我们指定列表相交的队列
                intersection = set(node_queues.keys()) & set(queue_names)
                if intersection:
                    # print(f'{node_queues.keys()} -- {queue_names}')
                    # print(f'{hostname} -- {info}')
                    # 若有交集, 则把该节点上的所有job_id都收集起来
                    for job in info.get('jobs', []):
                        matched_job_ids.add(job['job_id'])
                        jobid_queue_dict[job['job_id']] = f'{"".join([f"{i}@{hostname}" for i in list(intersection)])}'

        # 如果指定了 node
        if node:
            # 如果 user 传了多个节点？(可以自行决定只支持单个node还是多个node)
            # 这里示例只支持指定单个节点
            node_info = nodes_info.get(node)
            if node_info:
                related_queues = list(node_info.get('queues', {}).keys())
                for job_info in node_info.get('jobs', []):
                    matched_job_ids.add(job_info['job_id'])
                    jobid_queue_dict[job_info['job_id']] = ",".join([f'{q}@{node}' for q in related_queues])
        # matched_job_ids 即为我们需要查看的所有作业ID
        if not matched_job_ids:
            # 如果一个作业ID都没找到，提示一下就行
            print("没有在指定的队列/节点上找到任何作业。")
            return
        else:
            # 调用 get_job_details() + format_qstat_output() 进行可视化
            job_ids_list = list(matched_job_ids)  # 转回列表

            # 获取作业详细信息
            output = get_job_details(job_ids_list)
            # 可视化
            formatted_output = format_qstat_output(output, jobid_queue_dict, detail,one_line)
            print(formatted_output)
            return

    # ------------------------------------------------------------
    # 如果没有指定 queue 或 node，走原先逻辑
    # ------------------------------------------------------------
    if not user:
        user = get_current_user()
    
    # 获取用户的作业信息（包括 job_id 和所在节点）
    user_job_info = get_job_info_by_user(user)

    # 如果没有指定作业ID，获取该用户的所有作业ID
    if not job_id:
        job_ids = list(user_job_info.keys())
        if not job_ids:
            print(f"没有找到用户 {user} 的作业。")
            return
        print(f"找到以下作业ID: {', '.join(job_ids)}")
    else:
        job_ids = [job_id]

    # 获取作业详细信息
    output = get_job_details(job_ids)
    # 可视化
    formatted_output = format_qstat_output(output, user_job_info, detail, one_line)
    print(formatted_output)

if __name__ == '__main__':
    show_status()
