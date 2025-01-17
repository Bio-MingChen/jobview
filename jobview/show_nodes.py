#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import click
import re
import subprocess
import getpass
from collections import defaultdict

def is_digit(s):
    try:
        float(s)
        return True
    except:
        return False

def run_command(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        click.echo(f"[Warning] 命令执行失败: {cmd}\n{result.stderr}")
        return []
    return result.stdout.splitlines()

def parse_qselect_user(lines):
    suffix_pat = re.compile(r"\.(hpc|local)$")
    user_nodes = defaultdict(list)
    for ln in lines:
        ln = ln.strip()
        if not ln or "@" not in ln:
            continue
        qname, host = ln.split("@", 1)
        host = suffix_pat.sub("", host)
        user_nodes[host].append(qname)
    return dict(user_nodes)

def check_overload(load_str, stat_str):
    if load_str == "-":
        return True
    if re.search(r"o", stat_str, re.IGNORECASE):
        return True
    return False

def parse_humanfriendly_size(s):
    if s in ("-", "N/A", ""):
        return 0.0
    import humanfriendly
    try:
        bytes_val = humanfriendly.parse_size(s,binary=True)
        gb_val = bytes_val/(1024**3)
        return round(gb_val,3)
    except:
        return 0.0

def parse_qhost_q_j_F(lines):
    results = {}
    current_host = None

    host_line_regex = re.compile(
        r"^(?P<host>\S+)\s+\S+\s+(?P<cpu_used>\d+)\s+\d+\s+\d+\s+\d+\s+(?P<load>[\d\.]+|\-)\s+(?P<memtot>\S+)\s+(?P<memuse>\S+)"
    )
    num_proc_regex = re.compile(r"hc:num_proc\s*=\s*([\d\.]+)")
    vf_regex       = re.compile(r"h[cl]:virtual_free\s*=\s*([\d\.]+)([KMGTP])?", re.IGNORECASE)
    queue_line_regex = re.compile(
        r"^\s+(?P<queue>\S+\.q)\s+BIP\s+(?P<slotinfo>\S+)(\s+(?P<stat>\S+))?"
    )
    mem_free_regex = re.compile(r"hl:mem_free\s*=\s*([\d\.]+)([KMGTP])?", re.IGNORECASE)
   
    for line in lines:
        ls = line.rstrip()
        if not ls:
            continue

       
        m_host = host_line_regex.match(ls)
        if m_host:
            hostname = m_host.group("host")
            cpu_used_str = m_host.group("cpu_used")
            load_str     = m_host.group("load")
            memt_str     = m_host.group("memtot")
            memu_str     = m_host.group("memuse")

            cpu_used_val = float(cpu_used_str)
            memt_gb = parse_humanfriendly_size(memt_str)
            memu_gb = parse_humanfriendly_size(memu_str)

            results[hostname] = {
                "cpu_used": cpu_used_val,
                "cpu_total": 0.0,
                "vf": 0.0,
                "memtot": memt_gb,
                "memuse": memu_gb,
                "queues": {},
                "jobs": [],
                "load_str": load_str
            }
            current_host = hostname
            continue

        if not current_host:
            continue
        # if current_host == "tjcompute008":
        #     print(ls)

        m_mfree = mem_free_regex.search(ls)
        if m_mfree:
            val_str = m_mfree.group(1)
            unit    = m_mfree.group(2) if m_mfree.group(2) else ""
            free_gb = parse_humanfriendly_size(val_str+unit)
            results[current_host]["memf"] = free_gb

        m_np = num_proc_regex.search(ls)
        if m_np:
            np_val = float(m_np.group(1))
            if np_val > results[current_host]["cpu_used"]:
                results[current_host]["cpu_used"] = np_val

        m_vf = vf_regex.search(ls)
        if m_vf:
            val_str = m_vf.group(1)
            unit    = m_vf.group(2) if m_vf.group(2) else ""
            # if current_host == "tjcompute008":
            #     print(f"{current_host}:{val_str+unit}")

            vf_gb   = parse_humanfriendly_size(val_str+unit)

            # if current_host == "tjcompute008":
            #     print(f"{current_host}:{vf_gb}")
                
            results[current_host]["vf"] = vf_gb

        qm = queue_line_regex.match(ls)
        if qm:
            qn = qm.group("queue")
            slotinfo = qm.group("slotinfo")
            stat     = qm.group("stat") or ""
            parts_slt= slotinfo.split("/")
            if len(parts_slt)==3:
                try:
                    total_v = float(parts_slt[2])
                    if total_v>results[current_host]["cpu_total"]:
                        results[current_host]["cpu_total"] = total_v
                except:
                    pass
            is_over = check_overload(results[current_host]["load_str"], stat)
            results[current_host]["queues"][qn] = { "over": is_over }
            continue

        parts = ls.split()
        if parts and is_digit(parts[0]):
            job_id = parts[0]
            user_  = parts[3] if len(parts)>3 else "?"
            results[current_host]["jobs"].append({"job_id": job_id, "user": user_})

    return results


def show_nodes(user):
    if not user:
        user = getpass.getuser()

    sel_cmd = f"qselect -U {user}"
    sel_lines = run_command(sel_cmd)
    user_nodes = parse_qselect_user(sel_lines)
    if not user_nodes:
        click.secho(f"用户 {user} 在 qselect -U 下没有可用节点.", fg="red")
        return

    qhost_cmd = "qhost -q -j -F"
    lines = run_command(qhost_cmd)
    if not lines:
        click.secho("[Warning] 无法获取 qhost -q -j -F 输出.", fg="red")
        return

    host_info = parse_qhost_q_j_F(lines)

    final_map = {}
    for host, info in host_info.items():
        if host in user_nodes:
            final_map[host] = info

    rows = []
    for host, info in final_map.items():
        used_cpu = info.get("cpu_used",'-')
        total_cpu= info.get("cpu_total",'-')
        vf_   = info.get("vf",'-')
        memF  = info.get("memf",'-')  # 物理剩余(来自 hl:mem_free)
        memT  = info.get("memtot",'-')  # 物理总

        mem_str = f"{vf_:.1f} ({memF:.1f})/{memT:.1f}"

        jobList = info.get("jobs",0)
        jobCount= len(jobList)
        userJob = sum(1 for j in jobList if j["user"]==user)

        over_flag= False
        qlist=[]
        for qn,qd in info["queues"].items():
            if qn in user_nodes[host]:
                qlist.append(qn)
                if qd["over"]:
                    over_flag = True
        over_str = "YES" if over_flag else "NO"
        q_str    = ",".join(qlist)

        cpu_str  = f"{used_cpu:.0f}/{total_cpu:.0f}"

        rows.append({
            "host": host,
            "cpu_val": used_cpu,
            "cpu_str": cpu_str,
            "vf_val" : vf_,
            "mem_str": mem_str,
            "jobc": jobCount,
            "ujob": userJob,
            "over": over_str,
            "queues": q_str
        })

    rows.sort(key=lambda x: x["host"])

    # 打印说明（黄色）
    click.secho("列说明：", fg="yellow", bold=True)
    click.secho("  Hostname            节点名称", fg="yellow")
    click.secho("  CPU(Used/Total)     已使用NCPU + 最大队列slot(BIP最后一个数字)", fg="yellow")
    click.secho("  Mem(vf(MemF)/Tot)   剩余虚拟内存(vf), 物理剩余(MemF), 物理总(MemT)", fg="yellow")
    click.secho("  JobCount            节点全部作业数", fg="yellow")
    click.secho("  UserJob             当前用户作业数", fg="yellow")
    click.secho("  Overload            若队列含 'o' => YES", fg="yellow")
    click.secho("  Queues              节点所属队列&用户可用队列\n", fg="yellow")

    # 设置列宽
    col_h  = 20
    col_cpu= 15
    col_mem= 25
    col_jc = 9
    col_uj = 9
    col_ov = 10
    col_q  = 25

    # 表头（用空格对齐，而非 * ）
    header_host = f"{'Hostname':^{col_h}}"
    header_cpu  = f"{'CPU':^{col_cpu}}"
    header_mem  = f"{'Mem(vf(MemF)/Tot)':^{col_mem}}"
    header_jc   = f"{'JobCount':^{col_jc}}"
    header_uj   = f"{'UserJob':^{col_uj}}"
    header_ov   = f"{'Overload':^{col_ov}}"
    header_q    = f"{'Queues':^{col_q}}"

    header = f"{header_host} {header_cpu} {header_mem} {header_jc} {header_uj} {header_ov} {header_q}"

    click.secho(header, fg="bright_white", bold=True)
    sep_len = col_h+col_cpu+col_mem+col_jc+col_uj+col_ov+col_q+6
    click.secho("-"*sep_len, fg="bright_white")

    # 逐行输出
    for r in rows:
        # 构造纯文本对齐
        host_str = f"{r['host']:^{col_h}}"
        cpu_txt  = f"{r['cpu_str']:^{col_cpu}}"
        mem_txt  = f"{r['mem_str']:^{col_mem}}"
        jc_txt   = f"{r['jobc']:^{col_jc}}"
        uj_txt   = f"{r['ujob']:^{col_uj}}"
        ov_txt   = f"{r['over']:^{col_ov}}"
        qs_txt   = f"{r['queues']:^{col_q}}"

        # 然后再根据逻辑对 CPU, Mem, Overload 局部染色
        # Host, jobcount, userjob, queues 全部绿色
        host_colored = click.style(host_str, fg="green")
        jc_colored   = click.style(jc_txt,   fg="green")
        uj_colored   = click.style(uj_txt,   fg="green")
        qs_colored   = click.style(qs_txt,   fg="green")

        # CPU => if <10 => red
        if r["cpu_val"] < 10:
            cpu_colored = click.style(cpu_txt, fg="red")
        else:
            cpu_colored = click.style(cpu_txt, fg="green")

        # Mem => if vf<10 => red
        if r["vf_val"] < 10:
            mem_colored = click.style(mem_txt, fg="red")
        else:
            mem_colored = click.style(mem_txt, fg="green")

        # Overload => if == "YES" => red
        if r["over"] == "YES":
            ov_colored = click.style(ov_txt, fg="red")
        else:
            ov_colored = click.style(ov_txt, fg="green")

        line = f"{host_colored} {cpu_colored} {mem_colored} {jc_colored} {uj_colored} {ov_colored} {qs_colored}"
        click.echo(line)

    click.secho("\n查询完成。", fg="green")


if __name__ == "__main__":
    show_nodes()
