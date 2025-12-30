#!/usr/bin/env python3
# bili_fan_v4.1_fixed.py - 修复版
import os
import signal
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any
import random

import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Microsoft YaHei"
plt.rcParams["axes.unicode_minus"] = False
from apscheduler.schedulers.blocking import BlockingScheduler
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------- 配置 ----------
UIDS = [
    63231,
    288374925,
    730732,
]

INTERVAL = 300  # X秒采集一次
PLOT_GAP = 600  # X秒绘图一次
CSV_HEADER = "ts_utc,ts_cn,fans\n"
TZ = timezone(timedelta(hours=8))

BASE_DIR = Path(__file__).parent / "bili_fan_data"
os.makedirs(BASE_DIR, exist_ok=True)

# ---------- 改进的日志 ----------
# 创建自定义格式化类
class UIDLogFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'uid'):
            record.uid = 'SYSTEM'
        return super().format(record)

# 设置日志
log = logging.getLogger("bili_fan")
log.setLevel(logging.INFO)

# 创建处理器
file_handler = logging.FileHandler(BASE_DIR / "run.log", encoding="utf-8")
stream_handler = logging.StreamHandler()

# 创建格式化器
formatter = UIDLogFormatter(
    "%(asctime)s | %(levelname)s | UID:%(uid)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# 设置处理器格式
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# 添加处理器
log.addHandler(file_handler)
log.addHandler(stream_handler)

# ---------- 简化请求函数 ----------
def get_fans_safe(uid: int) -> int:
    """安全获取粉丝数，避免反爬"""
    # 方法1: 主API
    urls = [
        f"https://api.bilibili.com/x/relation/stat?vmid={uid}",
        f"https://api.bilibili.com/x/space/acc/info?mid={uid}&jsonp=jsonp",
        f"https://api.bilibili.com/x/space/upstat?mid={uid}&jsonp=jsonp",
    ]
    
    headers_list = [
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://space.bilibili.com/{uid}",
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.bilibili.com",
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
    ]
    
    for url_idx, url in enumerate(urls):
        for header_idx, headers in enumerate(headers_list):
            try:
                # 添加随机延迟
                delay = random.uniform(2, 5)
                time.sleep(delay)
                
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=10,
                    verify=False  # 跳过SSL验证，如果遇到证书问题
                )
                
                log.info(f"尝试 URL{url_idx+1} 方法{header_idx+1}: 状态码 {response.status_code}", extra={'uid': uid})
                
                # 检查响应内容
                if response.status_code == 200:
                    # 尝试解析JSON
                    try:
                        data = response.json()
                        log.info(f"响应内容类型: {type(data)}", extra={'uid': uid})
                        
                        # 不同API的不同数据位置
                        if "data" in data:
                            if "follower" in data["data"]:
                                fans = int(data["data"]["follower"])
                                log.info(f"从主API获取粉丝数: {fans}", extra={'uid': uid})
                                return fans
                            elif "follower" in str(data["data"]):
                                # 尝试从字符串中提取
                                import re
                                match = re.search(r'"follower":\s*(\d+)', str(data["data"]))
                                if match:
                                    fans = int(match.group(1))
                                    log.info(f"从字符串提取粉丝数: {fans}", extra={'uid': uid})
                                    return fans
                    
                    except json.JSONDecodeError as e:
                        # 响应不是JSON，可能是HTML
                        log.warning(f"JSON解析失败，响应可能是HTML，前500字符: {response.text[:500]}", extra={'uid': uid})
                        continue
                    
                elif response.status_code == 412:
                    log.warning(f"遇到412错误，继续尝试其他方法", extra={'uid': uid})
                    continue
                    
            except requests.exceptions.RequestException as e:
                log.warning(f"请求异常: {e}", extra={'uid': uid})
                continue
            except Exception as e:
                log.warning(f"其他异常: {e}", extra={'uid': uid})
                continue
    
    # 如果所有方法都失败，尝试备用方案：直接访问空间页面并解析
    log.info("尝试备用方案：解析空间页面", extra={'uid': uid})
    try:
        space_url = f"https://space.bilibili.com/{uid}"
        response = requests.get(
            space_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            # 在页面中查找粉丝数
            import re
            # 查找可能的粉丝数模式
            patterns = [
                r'"follower":\s*(\d+)',
                r'粉丝.*?(\d+)',
                r'关注者.*?(\d+)',
                r'<span[^>]*>(\d+)</span>\s*粉丝'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, response.text)
                if match:
                    try:
                        fans = int(match.group(1))
                        log.info(f"从页面解析粉丝数: {fans}", extra={'uid': uid})
                        return fans
                    except ValueError:
                        continue
    except Exception as e:
        log.error(f"备用方案也失败: {e}", extra={'uid': uid})
    
    raise RuntimeError(f"无法获取UID {uid}的粉丝数，所有方法都失败了")

# ---------- 用户配置 ----------
def load_user_config() -> List[Dict[str, Any]]:
    config_file = BASE_DIR / "users_config.json"
    
    if not config_file.exists():
        default_config = []
        for uid in UIDS:
            default_config.append({
                "uid": uid,
                "name": f"用户{uid}",
                "enabled": True,
                "interval": INTERVAL,
                "last_check": None
            })
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, ensure_ascii=False, indent=2)
        return default_config
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        config_uids = {user['uid'] for user in config}
        for uid in UIDS:
            if uid not in config_uids:
                config.append({
                    "uid": uid,
                    "name": f"用户{uid}",
                    "enabled": True,
                    "interval": INTERVAL,
                    "last_check": None
                })
        
        return config
    except Exception as e:
        log.error(f"加载配置文件失败: {e}", extra={'uid': 'SYSTEM'})
        return []

def save_user_config(config: List[Dict[str, Any]]):
    config_file = BASE_DIR / "users_config.json"
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"保存配置文件失败: {e}", extra={'uid': 'SYSTEM'})

# ---------- 文件路径 ----------
def get_csv_path(uid: int) -> Path:
    return BASE_DIR / f"{uid}_fans.csv"

def get_png_path(uid: int) -> Path:
    return BASE_DIR / f"{uid}_trend.png"

# ---------- 数据存储 ----------
def save_csv(uid: int, ts: datetime, fans: int):
    csv_file = get_csv_path(uid)
    
    if not csv_file.exists():
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_file, "w", encoding="utf-8") as f:
            f.write(CSV_HEADER)
    
    # 生成UTC标准时间字符串
    ts_utc = ts.isoformat()
    
    # 生成北京时间字符串
    ts_cn = ts.astimezone(timezone(timedelta(hours=8))).strftime('%Y/%m/%d %H:%M:%S')
    
    row = f"{ts_utc},{ts_cn},{fans}\n"
    with open(csv_file, "a", encoding="utf-8") as f:
        f.write(row)

# ---------- 绘图 ----------
_last_plot_times = {}

def plot(uid: int) -> None:
    global _last_plot_times
    
    now = time.time()
    last_plot = _last_plot_times.get(uid, 0)
    if now - last_plot < PLOT_GAP:
        return
    
    _last_plot_times[uid] = now
    
    csv_file = get_csv_path(uid)
    png_file = get_png_path(uid)
    
    if not csv_file.exists():
        log.warning(f"CSV文件不存在: {csv_file}", extra={'uid': uid})
        return
    
    try:
    # 智能读取CSV：先读取第一行判断列数
        with open(csv_file, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
    
    # 根据列数确定读取方式
        column_count = len(first_line.split(','))
    
        if column_count == 3:
        # 新格式：ts_utc,ts_cn,fans
            df = pd.read_csv(csv_file, names=["ts_utc", "ts_cn", "fans"], header=0)
            df['ts'] = pd.to_datetime(df['ts_utc'])
            log.info(f"读取到新格式CSV（3列）", extra={'uid': uid})
        elif column_count == 2:
        # 旧格式：ts,fans
            df = pd.read_csv(csv_file, names=["ts", "fans"], header=0)
            df['ts'] = pd.to_datetime(df['ts'])
            log.info(f"读取到旧格式CSV（2列）", extra={'uid': uid})
        else:
            raise ValueError(f"CSV列数异常：{column_count}列")
    
        df = df.set_index('ts')
        
    except Exception as e:
        log.error(f"读取CSV失败: {e}", extra={'uid': uid})
        return
    
    df = df.sort_index().drop_duplicates()
    if df.empty or len(df) < 2:
        log.warning(f"数据点不足，跳过绘图", extra={'uid': uid})
        return
    
    plt.figure(figsize=(12, 4))
    ax = plt.gca()
    ax.ticklabel_format(useOffset=False, style='plain')
    
    # 生成时间标签
    time_labels = df.index.strftime("%m-%d %H:%M")
    
    # 绘制趋势线
    plt.plot(df.index, df.fans, color="#FB7299", lw=2, label="粉丝数")
    
    # 设置x轴刻度
    if len(df) > 10:
        # 数据点多时，只显示部分刻度
        step = len(df) // 10
        plt.xticks(df.index[::step], time_labels[::step], rotation=45)
    else:
        plt.xticks(df.index, time_labels, rotation=45)
    
    # 标注极值点
    if len(df) > 1:
        mx, mn = df.fans.max(), df.fans.min()
        mx_idx = df.fans.idxmax()
        mn_idx = df.fans.idxmin()
        
        plt.scatter([mx_idx], [mx], color="g", zorder=5)
        plt.text(mx_idx, mx, f"峰值 {mx}", va="bottom", ha="right", fontsize=10)
        
        plt.scatter([mn_idx], [mn], color="r", zorder=5)
        plt.text(mn_idx, mn, f"谷值 {mn}", va="top", ha="right", fontsize=10)
        
        # 计算并显示累计变化
        delta = df.fans.iloc[-1] - df.fans.iloc[0]
        color = "g" if delta >= 0 else "r"
        
        # 在图表内部显示变化
        plt.text(
            0.02, 0.98,
            f"累计变化: {delta:+}",
            transform=ax.transAxes,
            fontsize=12,
            color=color,
            weight="bold",
            va="top",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8)
        )
    
    plt.title(f"B站粉丝趋势 (UID:{uid})", fontsize=14)
    plt.xlabel("时间")
    plt.ylabel("粉丝数")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    png_file.parent.mkdir(exist_ok=True)
    plt.savefig(png_file, dpi=150)
    plt.close()
    
    log.info(f"图表已更新 → {png_file.name}", extra={'uid': uid})

def plot_all():
    config = load_user_config()
    for user in config:
        if user.get('enabled', True):
            try:
                plot(user['uid'])
            except Exception as e:
                log.error(f"绘制图表失败: {e}", extra={'uid': user['uid']})

# ---------- 主任务 ----------
def job():
    config = load_user_config()
    
    for user in config:
        if not user.get('enabled', True):
            continue
            
        uid = user['uid']
        try:
            fans = get_fans_safe(uid)
            save_csv(uid, datetime.now(tz=TZ), fans)
            
            user['last_check'] = datetime.now(tz=TZ).isoformat()
            
            log.info(f"成功获取粉丝数: {fans}", extra={'uid': uid})
            
            # 每个用户采集后延迟
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            log.error(f"采集失败: {e}", extra={'uid': uid})
    
    save_user_config(config)

# ---------- 简单测试函数 ----------
def test_api():
    """测试API是否可用"""
    log.info("开始测试API连接...", extra={'uid': 'SYSTEM'})
    
    for uid in UIDS:
        try:
            log.info(f"测试UID: {uid}", extra={'uid': 'SYSTEM'})
            fans = get_fans_safe(uid)
            log.info(f"✓ UID {uid} 测试成功，粉丝数: {fans}", extra={'uid': 'SYSTEM'})
            return True
        except Exception as e:
            log.error(f"✗ UID {uid} 测试失败: {e}", extra={'uid': 'SYSTEM'})
    
    log.error("所有UID测试都失败，请检查网络或API状态", extra={'uid': 'SYSTEM'})
    return False

# ---------- 主入口 ----------
def main():
    config = load_user_config()
    log.info(f"已加载 {len(config)} 个用户配置", extra={'uid': 'SYSTEM'})
    
    # 先测试API
    if not test_api():
        log.error("API测试失败，程序退出", extra={'uid': 'SYSTEM'})
        return
    
    for user in config:
        if user.get('enabled', True):
            csv_file = get_csv_path(user['uid'])
            if not csv_file.exists():
                csv_file.parent.mkdir(parents=True, exist_ok=True)
                with open(csv_file, "w", encoding="utf-8") as f:
                    f.write(CSV_HEADER)
                log.info(f"创建CSV文件: {csv_file.name}", extra={'uid': user['uid']})
    
    log.info("开始首次数据采集...", extra={'uid': 'SYSTEM'})
    job()
    plot_all()
    
    sched = BlockingScheduler()
    
    # 只添加一个全局采集任务，而不是每个用户一个
    sched.add_job(
        job,
        "interval",
        seconds=INTERVAL,  # 使用配置中的INTERVAL作为统一间隔
        id="collect_all_users",
        name="采集所有用户"
    )
    
    # 添加绘图任务
    sched.add_job(
        plot_all,
        "interval",
        seconds=PLOT_GAP,
        id="plot_all",
        name="绘制所有图表"
    )
    
    log.info(f"已安排采集任务，统一间隔{INTERVAL}秒", extra={'uid': 'SYSTEM'})
    log.info(f"已安排绘图任务，间隔{PLOT_GAP}秒", extra={'uid': 'SYSTEM'})
    
    def shutdown(signum, frame):
        log.info("收到停止信号，正在关闭...", extra={'uid': 'SYSTEM'})
        sched.shutdown(wait=False)
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    log.info("多用户粉丝监控已启动", extra={'uid': 'SYSTEM'})
    log.info(f"数据目录: {BASE_DIR}", extra={'uid': 'SYSTEM'})
    log.info(f"监控UID: {[u['uid'] for u in config]}", extra={'uid': 'SYSTEM'})
    log.info("按 Ctrl+C 停止", extra={'uid': 'SYSTEM'})
    
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("程序正常退出", extra={'uid': 'SYSTEM'})
    except Exception as e:
        log.error(f"调度器错误: {e}", extra={'uid': 'SYSTEM'})

if __name__ == "__main__":
    main()