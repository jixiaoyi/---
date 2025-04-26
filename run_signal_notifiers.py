#!/usr/bin/env python
#  -*- coding: utf-8 -*-
__author__ = 'AI Assistant'

import os
import sys
import subprocess
import signal
import time
import logging
from datetime import datetime

# 设置日志
def setup_logger(name, log_file, level=logging.INFO):
    """设置日志配置"""
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    
    handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(console_handler)
    
    return logger

# 获取当前文件所在目录的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 构建日志文件的完整路径
log_file = os.path.join(current_dir, 'run_signal_notifiers.log')
# 设置日志记录器
logger = setup_logger('run_signal_notifiers', log_file)

# 获取策略文件的路径
TREND_FOLLOW_PATH = os.path.join(current_dir, "TrendFollowSignalNotifier.py")
TRADE_SIGNAL_PATH = os.path.join(current_dir, "TradeSignalNotifier.py")  # 两个文件都在运行程序目录下

def check_strategy_files():
    """
    检查策略文件是否存在
    :return: bool - 是否所有文件都存在
    """
    files_ok = True
    
    # 检查趋势跟踪策略文件
    if not os.path.exists(TREND_FOLLOW_PATH):
        logger.error(f"找不到趋势跟踪策略文件: {TREND_FOLLOW_PATH}")
        files_ok = False
    
    # 检查交易信号策略文件
    if not os.path.exists(TRADE_SIGNAL_PATH):
        logger.error(f"找不到交易信号策略文件: {TRADE_SIGNAL_PATH}")
        files_ok = False
    
    if files_ok:
        logger.info("所有策略文件检查通过")
    
    return files_ok

def run_strategy(strategy_path):
    """
    运行策略
    :param strategy_path: 策略文件路径
    :return: subprocess.Popen对象
    """
    try:
        # 使用Python解释器运行策略文件
        process = subprocess.Popen([sys.executable, strategy_path],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 universal_newlines=True)
        logger.info(f"成功启动策略: {os.path.basename(strategy_path)}")
        return process
    except Exception as e:
        logger.error(f"启动策略失败 {os.path.basename(strategy_path)}: {str(e)}")
        return None

def monitor_process(process, strategy_name):
    """
    监控进程状态
    :param process: subprocess.Popen对象
    :param strategy_name: 策略名称
    :return: 进程是否需要重启
    """
    if process.poll() is not None:
        # 进程已结束，检查返回码
        return_code = process.poll()
        stdout, stderr = process.communicate()
        
        if return_code != 0:
            logger.error(f"{strategy_name} 异常退出，返回码: {return_code}")
            if stderr:
                logger.error(f"错误信息: {stderr}")
            if stdout:
                logger.error(f"输出信息: {stdout}")
        else:
            logger.info(f"{strategy_name} 正常退出")
        
        return True
    return False

def cleanup(processes):
    """
    清理所有进程
    :param processes: 进程字典
    """
    for name, process in processes.items():
        if process and process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=5)
                logger.info(f"已终止 {name}")
            except subprocess.TimeoutExpired:
                process.kill()
                logger.info(f"已强制终止 {name}")
            except Exception as e:
                logger.error(f"终止 {name} 时发生错误: {str(e)}")

def main():
    """主函数"""
    # 检查策略文件是否存在
    if not check_strategy_files():
        logger.error("策略文件检查失败，程序退出")
        return
    
    # 存储进程对象
    processes = {
        "趋势跟踪信号": None,
        "交易信号": None
    }
    
    try:
        # 启动策略
        processes["趋势跟踪信号"] = run_strategy(TREND_FOLLOW_PATH)
        processes["交易信号"] = run_strategy(TRADE_SIGNAL_PATH)
        
        # 主循环
        while True:
            try:
                # 检查趋势跟踪信号进程
                if processes["趋势跟踪信号"] and monitor_process(processes["趋势跟踪信号"], "趋势跟踪信号"):
                    logger.info("正在重启趋势跟踪信号进程...")
                    processes["趋势跟踪信号"] = run_strategy(TREND_FOLLOW_PATH)
                
                # 检查交易信号进程
                if processes["交易信号"] and monitor_process(processes["交易信号"], "交易信号"):
                    logger.info("正在重启交易信号进程...")
                    processes["交易信号"] = run_strategy(TRADE_SIGNAL_PATH)
                
                # 休眠一段时间
                time.sleep(5)
                
            except KeyboardInterrupt:
                logger.info("收到终止信号，正在关闭所有进程...")
                cleanup(processes)
                break
            except Exception as e:
                logger.error(f"监控进程时发生错误: {str(e)}")
                continue
    
    except Exception as e:
        logger.error(f"程序运行时发生错误: {str(e)}")
    finally:
        cleanup(processes)
        logger.info("程序已安全退出")

if __name__ == "__main__":
    main() 