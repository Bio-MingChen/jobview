import subprocess
import click
import json
import os
import re
import datetime as dt

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
    output = execute_command(command)
    return output

def print_item(item):
    """
    打印有颜色的结果
    """
    click.secho("=" * 60)
    click.secho(f"Jobinfo:      {item['job_id']} {item['job_name']}",fg='yellow')
    click.secho(f"Useage:       {item['usage']}",fg='cyan')
    click.secho(f"Submit_time:  {item['submit_time']}",fg='magenta')
    click.secho(f"Shell:        {item['shell']}",fg='green')
    click.secho(f"Queue:        {item.get('exec_host')}",fg='blue')

def format_qstat_output(output,job_info,detail=False):
    """ 格式化 qstat -j 输出并添加中文字段名称 """
    lines = output.splitlines()
    formatted_output = []
    if not detail:
        # 精简输出格式
        item = {}
        for line in lines:
            # 提取作业信息
            job_id_match = re.search(r'job_number:\s+(\d+)', line)
            job_name_match = re.search(r'job_name:\s+(\S+)', line)
            usage_match = re.search(r'usage\s+1:\s+(.+)', line)
            submit_time_match = re.search(r'submission_time:\s+(.+)', line)
            shell_match = re.search(r'job_args:.+\s+(.+)', line)

            if job_id_match:
                item['job_id'] = job_id_match.group(1) if job_id_match else '未知'
            elif job_name_match:
                item['job_name'] = job_name_match.group(1) if job_name_match else '未知'
            elif usage_match:
                item['usage'] = usage_match.group(1) if usage_match else '未知'
            elif submit_time_match:
                item['submit_time'] = submit_time_match.group(1) if submit_time_match else '未知'
                if item['submit_time'] != '未知':
                    datetime_obj = dt.datetime.strptime(item['submit_time'], "%a %b %d %H:%M:%S %Y")
                    item['submit_time'] = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
                
            elif shell_match:
                item['shell'] = shell_match.group(1) if shell_match else '未知'
            
            # 获取作业所在的队列
            if ('=' * 20 in line) and (item.get('job_id')):
                if item.get('job_id'):
                    print(job_info)
                    item['exec_host'] = job_info.get(item['job_id'], '未找到节点')
                    print_item(item)
                    
        item['exec_host'] = job_info.get(item['job_id'], '未找到节点')
        print_item(item)           
        
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


@click.command()
@click.option('-u', '--user', default=None, help="指定用户")
@click.option('-j', '--job_id', default=None, help="指定作业ID")
@click.option('-d','--detail', is_flag=True, help="启用详细输出格式")
def main(user, job_id,detail):
    """ 主程序入口 """
    # 如果没有指定用户，获取当前用户
    if not user:
        user = get_current_user()
    
    # 获取用户的作业信息（包括 job_id 和所在节点）
    job_info = get_job_info_by_user(user)

    # 如果没有指定作业ID，获取该用户的所有作业ID
    if not job_id:
        job_ids = list(job_info.keys())
        if not job_ids:
            print(f"没有找到用户 {user} 的作业。")
            return
        print(f"找到以下作业ID: {', '.join(job_ids)}")
    else:
        job_ids = [job_id]

    # 获取作业详细信息
    output = get_job_details(job_ids)
    formatted_output = format_qstat_output(output, job_info, detail)
    
    # 将结果传递给 less 进行查看
    if detail:
        subprocess.run(['less','-SN'], input=formatted_output, text=True)

if __name__ == '__main__':
    main()
