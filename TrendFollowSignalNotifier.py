#!/usr/bin/env python
#  -*- coding: utf-8 -*-
__author__ = 'AI Assistant'

from tqsdk import TqApi, TqAuth, TqBacktest, TargetPosTask, BacktestFinished
from tqsdk.tafunc import ma
from tqsdk.ta import MACD, KDJ
import numpy as np
from datetime import date, datetime
import pytz
import logging
import os
import time
import hmac
import hashlib
import base64
import requests
import json
from collections import defaultdict
from operator import itemgetter
import traceback

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
log_file = os.path.join(current_dir, 'TrendFollowSignalNotifier.log')
# 设置日志记录器
logger = setup_logger('trend_follow', log_file)

# 策略参数设置
SYMBOLS = [
        #   'DCE.eb2506', 'DCE.eg2509', 'DCE.j2509', 'DCE.i2509',
        #    'DCE.a2507', 'DCE.b2509', 'DCE.c2507', 'DCE.cs2507',
        #     'DCE.jd2506', 'DCE.jm2509', 'DCE.l2509', 'DCE.lg2507',
        #     'DCE.lh2509','DCE.m2509', 'DCE.p2509', 'DCE.pg2506',
        #    'DCE.pp2509', 'DCE.v2509', 'DCE.y2509',

        #    'CZCE.AP510', 'CZCE.CF509', 'CZCE.CJ509', 'CZCE.FG509',
        #    'CZCE.MA509', 'CZCE.OI509', 'CZCE.PF509', 'CZCE.PK510',
        #    'CZCE.PR509', 'CZCE.PX509', 'CZCE.RM509','CZCE.SA509', 'CZCE.SF509', 'CZCE.SH509', 'CZCE.SM509',
        #    'CZCE.SR509', 'CZCE.TA509', 'CZCE.UR509',

        #    'SHFE.ag2506', 'SHFE.al2506', 'SHFE.ao2509', 'SHFE.au2506',
        #    'SHFE.br2506', 'SHFE.bu2506', 'SHFE.cu2506', 'SHFE.zn2506',
        #    'SHFE.fu2507', 'SHFE.hc2510', 'SHFE.ni2506',
        #    'SHFE.pb2506', 'SHFE.rb2510', 'SHFE.ru2509',
        #    'SHFE.sn2506','SHFE.sp2507', 'SHFE.ss2506',

        #    'INE.sc2506',  'INE.nr2506', 'INE.ec2506', 'INE.lu2506', 

        #    'GFEX.lc2507', 'GFEX.si2506', 'GFEX.ps2506'
    # "DCE.m2505",   # 豆粕2505
    # "SHFE.ag2505", # 沪银2505
    "SHFE.au2506",  # 黄金2506
    # "GFEX.ps2506",  # 多晶硅2506
    'SHFE.ru2509'
]

# 手动维护的周级别看多行情库
# 格式说明：
# - 使用集合（set）存储合约代码
# - 合约代码格式：交易所.品种+合约月份，例如：SHFE.au2406
# - 可以直接在这里添加或删除合约
WEEKLY_LONG_SYMBOLS = {
    "SHFE.au2506",  # 黄金2506
    # "GFEX.ps2506",  # 多晶硅2506
    # "DCE.m2405",    # 豆粕2405
    # "CZCE.TA405",   # PTA405
}

# 手动维护的周级别看空行情库
WEEKLY_SHORT_SYMBOLS = {
    # "SHFE.au2406",  # 黄金2406
    # "DCE.m2405",    # 豆粕2405
    # "CZCE.TA405",   # PTA405
    # "GFEX.ps2506",  # 多晶硅2506
}

# 程序自动维护的120分钟级别看多行情库
MIN120_LONG_SYMBOLS = set()

# 程序自动维护的120分钟级别看空行情库
MIN120_SHORT_SYMBOLS = set()

# 回测参数设置
BACKTEST_CONFIG = {
    "enabled": True,  # 是否启用回测模式
    "start_dt": date(2025, 4, 14),  # 回测开始日期
    "end_dt": date(2025, 4, 25)    # 回测结束日期
}

# 均线参数设置
MA20 = 20  # 分钟级别MA20
MA60 = 60  # 分钟级别MA60

# MACD参数设置
MACD_FAST = 10    # MACD快线周期
MACD_SLOW = 23    # MACD慢线周期
MACD_SIGNAL = 8   # MACD信号线周期


class FeishuBot:
    def __init__(self, strategy=None):
        """
        初始化飞书机器人
        :param strategy: 策略实例的引用，用于获取回测时间等信息
        """
        self.webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/abbadd71-3573-4dc8-8b32-9113fdc17133"
        self.secret = "YCmjC25sSNAnMuugDklaIe"
        self.strategy = strategy
        
    def _generate_sign(self, timestamp):
        """生成签名"""
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        return sign

    def get_current_time(self):
        """获取当前时间（回测模式返回回测时间，实盘模式返回实际时间）"""
        if BACKTEST_CONFIG["enabled"] and self.strategy:
            # 回测模式：使用策略实例的K线时间
            first_symbol = SYMBOLS[0]
            current_dt = self.strategy.klines[first_symbol]["1min"].datetime.iloc[-1]
            dt = datetime.fromtimestamp(current_dt / 1e9)
        else:
            # 实盘模式：使用系统当前时间
            dt = datetime.fromtimestamp(int(time.time()))
        
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = dt.astimezone(beijing_tz)
        return beijing_time.strftime('%Y-%m-%d %H:%M:%S')

    def send_weekly_long_signal(self, symbol, signal_type, signal_details, timestamp=None):
        """
        发送周级别多头信号
        :param symbol: 交易品种
        :param signal_type: 信号类型（'entry' 或 'exit'）
        :param signal_details: 信号详情字典
        :param timestamp: 时间戳（纳秒级）
        """
        # 如果没有提供时间戳，则根据模式获取时间戳
        if timestamp is None:
            if BACKTEST_CONFIG["enabled"] and self.strategy:
                # 回测模式：使用策略实例的K线时间
                first_symbol = SYMBOLS[0]
                timestamp = self.strategy.klines[first_symbol]["1min"].datetime.iloc[-1]
            else:
                # 实盘模式：使用系统当前时间
                timestamp = int(time.time() * 1e9)  # 转换为纳秒时间戳
        
        # 将纳秒时间戳转换为分钟时间戳（向下取整到分钟）
        minute_timestamp = (timestamp // (60 * 1e9)) * (60 * 1e9)
        
        # 生成签名用的毫秒时间戳
        timestamp_ms = int(minute_timestamp // 1e6)
        sign = self._generate_sign(timestamp_ms)
        
        exchange_symbol_map = {
    # 上海期货交易所 (SHFE)
    'SHFE.cu': '铜',
    'SHFE.ag': '白银',
    'SHFE.au': '黄金',
    'SHFE.sn': '锡',
    'SHFE.al': '铝',
    'SHFE.zn': '锌',
    'SHFE.fu': '燃料油',
    'SHFE.bu': '沥青',
    'SHFE.hc': '热卷',
    'SHFE.ni': '镍',
    'SHFE.pb': '铅',
    'SHFE.rb': '螺纹钢',
    'SHFE.ru': '天然橡胶',
    'SHFE.sp': '纸浆',
    'SHFE.ss': '不锈钢',
    'SHFE.ao': '氧化铝',
    'SHFE.br': '丁二烯橡胶',
    'SHFE.bc': '国际铜',

    # 大连商品交易所 (DCE)
    'DCE.m': '豆粕',
    'DCE.p': '棕榈油',
    'DCE.a': '豆一',
    'DCE.b': '豆二',
    'DCE.c': '玉米',
    'DCE.cs': '淀粉',
    'DCE.j': '焦炭',
    'DCE.jm': '焦煤',
    'DCE.i': '铁矿石',
    'DCE.eg': '乙二醇',
    'DCE.eb': '苯乙烯',
    'DCE.l': '聚乙烯',
    'DCE.v': '聚氯乙烯',
    'DCE.pp': '聚丙烯',
    'DCE.jd': '鸡蛋',
    'DCE.lh': '生猪',
    'DCE.y': '豆油',
    'DCE.lg': 'LPG',
    
    # 郑州商品交易所 (CZCE)
    'CZCE.FG': '玻璃',
    'CZCE.OI': '菜油',
    'CZCE.AP': '苹果',
    'CZCE.CF': '棉花',
    'CZCE.CJ': '红枣',
    'CZCE.MA': '甲醇',
    'CZCE.PF': '短纤',
    'CZCE.PK': '花生',
    'CZCE.RM': '菜粕',
    'CZCE.SA': '纯碱',
    'CZCE.SF': '硅铁',
    'CZCE.SM': '锰硅',
    'CZCE.SR': '白糖',
    'CZCE.TA': 'PTA',
    'CZCE.UR': '尿素',
    'CZCE.SH': '烧碱',
    
    # 上海国际能源交易中心 (INE)
    'INE.sc': '原油',
    'INE.nr': '20号胶',
    'INE.lu': '低硫燃料油',
    'INE.ec': '欧线',
    
    # 广州期货交易所 (GFEX)
    'GFEX.lc': '碳酸锂',
    'GFEX.si': '工业硅',
        }
        
        # 转换合约显示名称
        display_name = None
        for exchange_code, name in exchange_symbol_map.items():
            if symbol.startswith(exchange_code):
                contract_num = symbol.split('.')[-1]
                if exchange_code == 'TA.TA':
                    display_name = f"{name}{contract_num[2:]}"  # 对于PTA特殊处理，去掉前两位
                else:
                    display_name = f"{name}{contract_num}"
                break
        if not display_name:
            display_name = symbol

        # 获取时间字符串（使用传入的时间戳）
        dt = datetime.fromtimestamp(minute_timestamp / 1e9)
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = dt.astimezone(beijing_tz)
        time_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建卡片
        card = {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"周级别：看多 📈      {display_name}      {time_str}"
                },
                "template": "red" if signal_type == 'entry' else "green"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "---"  # 分割线
                    }
                }
            ]
        }
        
        # 构建内容
        content = []
        if signal_type == 'entry':
            # 建仓信号内容
            content.append("**📊 信号类型：建仓**\n")
            
            entry_points = [
                ("进场点一", "10分钟均线金叉", signal_details.get('long_entry_1', False)),
                ("进场点二", "1分钟均线金叉", signal_details.get('long_entry_2', False)),
                ("进场点三", "1分钟均线金叉", signal_details.get('long_entry_3', False))
            ]
            min1_dev = signal_details.get('min1_deviation', 0)
            min10_dev = signal_details.get('min10_deviation', 0)
            for point_name, desc, status in entry_points:
                # 看多信号全部用红色，满足√，不满足×
                mark = "✅" if status else "❌"
                color = "<font color='red'>"
                content.append(
                    f"{color}**{point_name}** {mark}</font>\n"
                    f"└─ {desc}\n"
                    f"└─ *1分钟偏离：{min1_dev:+.2f}%  |  10分钟偏离：{min10_dev:+.2f}%*\n"
                )
        else:
            # 平仓信号内容
            content.append("**⚠️ 信号类型：平仓**\n")
            
            exit_points = [
                ("出场点一", "10分钟破Ma60线", signal_details.get('exit_point_1', False)),
                ("出场点二", "10分钟均线死叉", signal_details.get('exit_point_2', False)),
                ("出场点三", "120分钟破大趋势线", signal_details.get('exit_point_3', False))
            ]
            
            for point_name, desc, status in exit_points:
                # 使用表情符号和颜色标记
                mark = "🔴" if status else "⚪"
                color = "<font color='red'>" if status else "<font color='grey'>"
                
                if point_name == "出场点三":
                    min120_dev = signal_details.get('min120_deviation', 0)
                    content.append(
                        f"{color}**{point_name}** {mark}</font>\n"
                        f"└─ {desc}\n"
                        f"└─ *120分钟偏离：{min120_dev:+.2f}%*\n"
                    )
                else:
                    min10_dev = signal_details.get('min10_deviation', 0)
                    content.append(
                        f"{color}**{point_name}** {mark}</font>\n"
                        f"└─ {desc}\n"
                        f"└─ *10分钟偏离：{min10_dev:+.2f}%*\n"
                    )
        
        # 添加内容到卡片
        card["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "\n".join(content)
            }
        })

        # 添加底部分割线
        card["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "---"
            }
        })
        
        # 添加当前多头池信息
        card["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": self.strategy.get_long_pool_info(for_feishu=True)
            }
        })
        
        # 添加时间戳信息
        card["elements"].append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": f"信号时间：{time_str}"
                }
            ]
        })
        
        # 发送请求
        data = {
            "msg_type": "interactive",
            "card": card,
            "timestamp": timestamp_ms,
            "sign": sign
        }
        
        try:
            response = requests.post(
                self.webhook,
                headers={"Content-Type": "application/json"},
                data=json.dumps(data)
            )
            if response.status_code != 200:
                logger.error(f"发送飞书消息失败: {response.text}")
            else:
                logger.info(f"成功发送周级别{'建仓' if signal_type == 'entry' else '平仓'}信号到飞书")
        except Exception as e:
            logger.error(f"发送飞书消息出错: {str(e)}")
            logger.error(traceback.format_exc())

    def send_weekly_short_signal(self, symbol, signal_type, signal_details, timestamp=None):
        """
        发送周级别空头信号
        :param symbol: 交易品种
        :param signal_type: 信号类型（'entry' 或 'exit'）
        :param signal_details: 信号详情字典
        :param timestamp: 时间戳（纳秒级）
        """
        # 如果没有提供时间戳，则根据模式获取时间戳
        if timestamp is None:
            if BACKTEST_CONFIG["enabled"] and self.strategy:
                # 回测模式：使用策略实例的K线时间
                first_symbol = SYMBOLS[0]
                timestamp = self.strategy.klines[first_symbol]["1min"].datetime.iloc[-1]
            else:
                # 实盘模式：使用系统当前时间
                timestamp = int(time.time() * 1e9)  # 转换为纳秒时间戳
        
        # 将纳秒时间戳转换为分钟时间戳（向下取整到分钟）
        minute_timestamp = (timestamp // (60 * 1e9)) * (60 * 1e9)
        
        # 生成签名用的毫秒时间戳
        timestamp_ms = int(minute_timestamp // 1e6)
        sign = self._generate_sign(timestamp_ms)
        
        exchange_symbol_map = {
    # 上海期货交易所 (SHFE)
    'SHFE.cu': '铜',
    'SHFE.ag': '白银',
    'SHFE.au': '黄金',
    'SHFE.sn': '锡',
    'SHFE.al': '铝',
    'SHFE.zn': '锌',
    'SHFE.fu': '燃料油',
    'SHFE.bu': '沥青',
    'SHFE.hc': '热卷',
    'SHFE.ni': '镍',
    'SHFE.pb': '铅',
    'SHFE.rb': '螺纹钢',
    'SHFE.ru': '天然橡胶',
    'SHFE.sp': '纸浆',
    'SHFE.ss': '不锈钢',
    'SHFE.ao': '氧化铝',
    'SHFE.br': '丁二烯橡胶',
    'SHFE.bc': '国际铜',

    # 大连商品交易所 (DCE)
    'DCE.m': '豆粕',
    'DCE.p': '棕榈油',
    'DCE.a': '豆一',
    'DCE.b': '豆二',
    'DCE.c': '玉米',
    'DCE.cs': '淀粉',
    'DCE.j': '焦炭',
    'DCE.jm': '焦煤',
    'DCE.i': '铁矿石',
    'DCE.eg': '乙二醇',
    'DCE.eb': '苯乙烯',
    'DCE.l': '聚乙烯',
    'DCE.v': '聚氯乙烯',
    'DCE.pp': '聚丙烯',
    'DCE.jd': '鸡蛋',
    'DCE.lh': '生猪',
    'DCE.y': '豆油',
    'DCE.lg': 'LPG',
    
    # 郑州商品交易所 (CZCE)
    'CZCE.FG': '玻璃',
    'CZCE.OI': '菜油',
    'CZCE.AP': '苹果',
    'CZCE.CF': '棉花',
    'CZCE.CJ': '红枣',
    'CZCE.MA': '甲醇',
    'CZCE.PF': '短纤',
    'CZCE.PK': '花生',
    'CZCE.RM': '菜粕',
    'CZCE.SA': '纯碱',
    'CZCE.SF': '硅铁',
    'CZCE.SM': '锰硅',
    'CZCE.SR': '白糖',
    'CZCE.TA': 'PTA',
    'CZCE.UR': '尿素',
    'CZCE.SH': '烧碱',
    
    # 上海国际能源交易中心 (INE)
    'INE.sc': '原油',
    'INE.nr': '20号胶',
    'INE.lu': '低硫燃料油',
    'INE.ec': '欧线',
    
    # 广州期货交易所 (GFEX)
    'GFEX.lc': '碳酸锂',
    'GFEX.si': '工业硅',
        }
        
        # 转换合约显示名称
        display_name = None
        for exchange_code, name in exchange_symbol_map.items():
            if symbol.startswith(exchange_code):
                contract_num = symbol.split('.')[-1]
                if exchange_code == 'TA.TA':
                    display_name = f"{name}{contract_num[2:]}"  # 对于PTA特殊处理，去掉前两位
                else:
                    display_name = f"{name}{contract_num}"
                break
        if not display_name:
            display_name = symbol

        # 获取时间字符串（使用传入的时间戳）
        dt = datetime.fromtimestamp(minute_timestamp / 1e9)
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = dt.astimezone(beijing_tz)
        time_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建卡片
        card = {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"周级别：看空 📉      {display_name}      {time_str}"
                },
                "template": "green" if signal_type == 'entry' else "red"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "---"  # 分割线
                    }
                }
            ]
        }
        
        # 构建内容
        content = []
        if signal_type == 'entry':
            # 建仓信号内容
            content.append("**📊 信号类型：建空仓**\n")
            
            entry_points = [
                ("进场点一", "10分钟均线死叉", signal_details.get('short_entry_1', False)),
                ("进场点二", "1分钟均线死叉", signal_details.get('short_entry_2', False)),
                ("进场点三", "1分钟均线死叉", signal_details.get('short_entry_3', False))
            ]
            min1_dev = signal_details.get('min1_deviation', 0)
            min10_dev = signal_details.get('min10_deviation', 0)
            for point_name, desc, status in entry_points:
                # 看空信号全部用绿色，满足√，不满足×
                mark = "✅" if status else "❌"
                color = "<font color='green'>"
                content.append(
                    f"{color}**{point_name}** {mark}</font>\n"
                    f"└─ {desc}\n"
                    f"└─ *1分钟偏离：{min1_dev:+.2f}%  |  10分钟偏离：{min10_dev:+.2f}%*\n"
                )
        else:
            # 平仓信号内容
            content.append("**⚠️ 信号类型：平仓**\n")
            
            exit_points = [
                ("出场点一", "10分钟破Ma60线", signal_details.get('exit_point_1', False)),
                ("出场点二", "10分钟均线死叉", signal_details.get('exit_point_2', False)),
                ("出场点三", "120分钟破大趋势线", signal_details.get('exit_point_3', False))
            ]
            
            for point_name, desc, status in exit_points:
                # 使用表情符号和颜色标记
                mark = "🔴" if status else "⚪"
                color = "<font color='red'>" if status else "<font color='grey'>"
                
                if point_name == "出场点三":
                    min120_dev = signal_details.get('min120_deviation', 0)
                    content.append(
                        f"{color}**{point_name}** {mark}</font>\n"
                        f"└─ {desc}\n"
                        f"└─ *120分钟偏离：{min120_dev:+.2f}%*\n"
                    )
                else:
                    min10_dev = signal_details.get('min10_deviation', 0)
                    content.append(
                        f"{color}**{point_name}** {mark}</font>\n"
                        f"└─ {desc}\n"
                        f"└─ *10分钟偏离：{min10_dev:+.2f}%*\n"
                    )
        
        # 添加内容到卡片
        card["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "\n".join(content)
            }
        })

        # 添加底部分割线
        card["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "---"
            }
        })
        
        # 添加当前空头池信息
        card["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": self.strategy.get_short_pool_info(for_feishu=True)
            }
        })
        
        # 添加时间戳信息
        card["elements"].append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": f"信号时间：{time_str}"
                }
            ]
        })
        
        # 发送请求
        data = {
            "msg_type": "interactive",
            "card": card,
            "timestamp": timestamp_ms,
            "sign": sign
        }
        
        try:
            response = requests.post(
                self.webhook,
                headers={"Content-Type": "application/json"},
                data=json.dumps(data)
            )
            if response.status_code != 200:
                logger.error(f"发送飞书消息失败: {response.text}")
            else:
                logger.info(f"成功发送周级别{'建空仓' if signal_type == 'entry' else '平空仓'}信号到飞书")
        except Exception as e:
            logger.error(f"发送飞书消息出错: {str(e)}")
            logger.error(traceback.format_exc())

class TrendFollowStrategy:
    def __init__(self):
        """初始化策略"""
        # 初始化API，设置回测参数
        if BACKTEST_CONFIG["enabled"]:
            self.api = TqApi(
                backtest=TqBacktest(
                    start_dt=BACKTEST_CONFIG["start_dt"],
                    end_dt=BACKTEST_CONFIG["end_dt"]
                ),
                auth=TqAuth("jixiaoyi123", "jixiaoyi1988A"),
                web_gui=True
            )
            logger.info(f"回测模式已启用 - 开始时间: {BACKTEST_CONFIG['start_dt']}, 结束时间: {BACKTEST_CONFIG['end_dt']}")
        else:
            self.api = TqApi(auth=TqAuth("jixiaoyi123", "jixiaoyi1988A"))
            logger.info("实盘模式已启用")
        
        # 初始化飞书机器人，传入策略实例的引用
        self.feishu_bot = FeishuBot(self)
        
        # 初始化数据存储
        self.klines = {}  # K线数据
        self.quotes = {}  # 行情数据
        
        # 使用全局定义的周级别和120分钟级别看多行情库
        self.weekly_long_symbols = WEEKLY_LONG_SYMBOLS.copy()
        self.weekly_short_symbols = WEEKLY_SHORT_SYMBOLS.copy()
        self.min120_long_symbols = MIN120_LONG_SYMBOLS.copy()
        self.min120_short_symbols = MIN120_SHORT_SYMBOLS.copy()
        
        logger.info(f"初始化周级别看多行情库: {self.weekly_long_symbols}")
        logger.info(f"初始化周级别看空行情库: {self.weekly_short_symbols}")
        logger.info(f"初始化120分钟级别看多行情库: {self.min120_long_symbols}")
        logger.info(f"初始化120分钟级别看空行情库: {self.min120_short_symbols}")
        
        # 记录已触发的进场点（多空分开记录，变量名彻底区分）
        self.long_entry_status = defaultdict(lambda: {'long_entry_1': False, 'long_entry_2': False, 'long_entry_3': False})
        self.short_entry_status = defaultdict(lambda: {'short_entry_1': False, 'short_entry_2': False, 'short_entry_3': False})
        
        try:
            # 初始化各个品种的数据
            for symbol in SYMBOLS:
                # 获取K线数据
                self.klines[symbol] = {
                    "1min": self.api.get_kline_serial(symbol, duration_seconds=60),
                    "10min": self.api.get_kline_serial(symbol, duration_seconds=10*60),
                    "120min": self.api.get_kline_serial(symbol, duration_seconds=120*60),
                    "week": self.api.get_kline_serial(symbol, duration_seconds=7*24*60*60),
                }
                
                # 获取实时行情
                self.quotes[symbol] = self.api.get_quote(symbol)
                
                # 等待K线数据初始化完成
                self.api.wait_update()
            
            logger.info("数据初始化完成")
            
            # 启动时立即进行一次合约池检查
            logger.info("开始进行启动时的合约池初始检查...")
            for symbol in SYMBOLS:
                if self.check_data_ready(symbol, "120min"):
                    try:
                        # 检查多头池
                        long_pool_condition = self.check_min120_long_pool(symbol)
                        if long_pool_condition and symbol not in self.min120_long_symbols:
                            self.min120_long_symbols.add(symbol)
                            logger.info(f"\n时间: {datetime.fromtimestamp(self.klines[symbol]['120min'].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                            logger.info(f"{symbol} 在启动检查时加入120分钟多头池\n{self.get_long_pool_info()}")
                        
                        # 检查空头池
                        short_pool_condition = self.check_min120_short_pool(symbol)
                        if short_pool_condition and symbol not in self.min120_short_symbols:
                            self.min120_short_symbols.add(symbol)
                            logger.info(f"\n时间: {datetime.fromtimestamp(self.klines[symbol]['120min'].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                            logger.info(f"{symbol} 在启动检查时加入120分钟空头池\n{self.get_short_pool_info()}")
                    except Exception as e:
                        logger.error(f"启动时检查{symbol}合约池时发生错误: {str(e)}")
            
            logger.info("启动时合约池检查完成")
            logger.info(f"当前多头池状态:\n{self.get_long_pool_info()}")
            logger.info(f"当前空头池状态:\n{self.get_short_pool_info()}")
            
            logger.info("策略初始化完成")
        except Exception as e:
            logger.error(f"初始化数据时发生错误: {str(e)}")
            raise

    def check_data_ready(self, symbol, kline_type):
        """
        检查K线数据是否准备就绪
        :param symbol: 交易品种
        :param kline_type: K线类型
        :return: bool
        """
        try:
            if symbol not in self.klines:
                logger.warning(f"{symbol} 的K线数据未初始化")
                return False
            
            if kline_type not in self.klines[symbol]:
                logger.warning(f"{symbol} 的 {kline_type} K线数据未初始化")
                return False
            
            if len(self.klines[symbol][kline_type]) < 60:  # 确保有足够的K线数据
                logger.warning(f"{symbol} 的 {kline_type} K线数据不足")
                return False
            
            return True
        except Exception as e:
            logger.error(f"检查数据就绪状态时发生错误: {str(e)}")
            return False

    def calculate_slope(self, data, period):
        """
        使用线性回归计算斜率
        :param data: 数据序列
        :param period: 计算周期
        :return: 斜率值
        """
        if len(data) < period:
            return 0
        y = data.iloc[-period:].values
        x = np.arange(period)
        slope, _ = np.polyfit(x, y, 1)
        return slope
    
    def calculate_deviation(self, price, ma_value):
        """
        计算价格偏离度
        :param price: 当前价格
        :param ma_value: 均线值
        :return: 偏离度百分比
        """
        return (price - ma_value) / ma_value * 100
    
    def check_entry_point_1(self, symbol):
        """
        检查进场点一（10分钟进场点）
        :param symbol: 交易品种
        :return: bool, dict - 是否满足条件，详细信息
        """
        klines_10min = self.klines[symbol]["10min"]
        ma20 = ma(klines_10min.close, MA20)
        ma60 = ma(klines_10min.close, MA60)
        
        # 计算偏离度
        min10_deviation = self.calculate_deviation(klines_10min.close.iloc[-1], ma60.iloc[-1])
        min1_deviation = self.calculate_deviation(
            self.klines[symbol]["1min"].close.iloc[-1],
            ma(self.klines[symbol]["1min"].close, MA60).iloc[-1]
        )
        
        # 检查是否需要重置进场点一的状态
        if ma20.iloc[-1] < ma60.iloc[-1]:
            self.long_entry_status[symbol]['long_entry_1'] = False
            self.long_entry_status[symbol]['long_entry_2'] = False
            self.long_entry_status[symbol]['long_entry_3'] = False
            return False, {}
        
        # 精确的进场点一条件
        condition = (
            ma20.iloc[-3] < ma60.iloc[-3] and  # -3根k线Ma20<=ma60
            ma20.iloc[-2] >= ma60.iloc[-2]      # -2根k线ma20>=ma60
        )
        
        return condition, {
            'min10_deviation': min10_deviation,
            'min1_deviation': min1_deviation
        }
    
    def check_entry_points_2_and_3(self, symbol):
        """
        同时检查进场点二和三（1分钟进场点）
        :param symbol: 交易品种
        :return: dict - 包含两个进场点的检查结果和详细信息
        """
        # 如果进场点一未触发，直接返回False
        if not self.long_entry_status[symbol]['long_entry_1']:
            return {'long_entry_2': False, 'long_entry_3': False, 'details': {}}
            
        # 检查10分钟级别的偏离度
        klines_10min = self.klines[symbol]["10min"]
        ma60_10min = ma(klines_10min.close, MA60)
        price_10min = klines_10min.close.iloc[-1]
        
        # 多头：10分钟偏离度 = (价格-ma60)/ma60
        deviation_10min = (price_10min - ma60_10min.iloc[-1]) / ma60_10min.iloc[-1] * 100
        
        klines_1min = self.klines[symbol]["1min"]
        ma20 = ma(klines_1min.close, MA20)
        ma60 = ma(klines_1min.close, MA60)
        macd = MACD(klines_1min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        
        # 多头：1分钟偏离度 = (价格-ma60)/ma60
        min1_deviation = (klines_1min.close.iloc[-1] - ma60.iloc[-1]) / ma60.iloc[-1] * 100
        
        # 进场点二的条件
        entry_point_2 = False
        # 只有当10分钟偏离值<=0.5%时才检查进场点二
        if deviation_10min <= 0.5:
            entry_point_2_condition = (
                ma20.iloc[-3] < ma60.iloc[-3] and  # -3根k线Ma20<ma60
                ma20.iloc[-2] >= ma60.iloc[-2] and  # -2根k线ma20>=ma60
                macd["bar"].iloc[-2] < 0            # -2根k线MACD<0
            )
            # 只在进场点一刚触发时检查进场点二（确保是第一个1分钟进场点）
            if entry_point_2_condition and not self.long_entry_status[symbol]['long_entry_2']:
                entry_point_2 = True
        
        # 进场点三的条件
        entry_point_3 = False
        # 检查10分钟级别的偏离度是否在0-3%之间
        if 0 <= deviation_10min <= 0.3:
            entry_point_3_condition = (
                ma20.iloc[-3] < ma60.iloc[-3] and  # -3根k线Ma20<ma60
                ma20.iloc[-2] >= ma60.iloc[-2]      # -2根k线ma20>=ma60
            )
            # 只在首次满足条件时触发进场点三
            if entry_point_3_condition and not self.long_entry_status[symbol]['long_entry_3']:
                entry_point_3 = True
        
        details = {
            'min1_deviation': min1_deviation,
            'min10_deviation': deviation_10min
        }
        
        return {
            'long_entry_2': entry_point_2,
            'long_entry_3': entry_point_3,
            'details': details
        }

    def check_short_entry_point_1(self, symbol):
        """
        检查做空进场点一（10分钟进场点）
        :param symbol: 交易品种
        :return: bool, dict - 是否满足条件，详细信息
        """
        klines_10min = self.klines[symbol]["10min"]
        ma20 = ma(klines_10min.close, MA20)
        ma60 = ma(klines_10min.close, MA60)
        macd = MACD(klines_10min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        
        # 计算偏离度
        min10_deviation = self.calculate_deviation(ma60.iloc[-1], klines_10min.close.iloc[-1])  # 注意这里是反过来计算的
        min1_deviation = self.calculate_deviation(
            ma(self.klines[symbol]["1min"].close, MA60).iloc[-1],
            self.klines[symbol]["1min"].close.iloc[-1]
        )
        
        # 检查是否需要重置进场点一的状态
        # 当MA20 > MA60时，重置进场点一的状态
        if ma20.iloc[-1] > ma60.iloc[-1]:
            self.short_entry_status[symbol]['short_entry_1'] = False
            self.short_entry_status[symbol]['short_entry_2'] = False
            self.short_entry_status[symbol]['short_entry_3'] = False
            return False, {}
        
        # 精确的进场点一条件
        condition = (
            ma20.iloc[-3] > ma60.iloc[-3] and  # -3根k线Ma20>ma60
            ma20.iloc[-2] <= ma60.iloc[-2]   # -2根k线ma20<=ma60
            # and macd["bar"].iloc[-2] < 0            # -2根k线MACD<0
        )
        
        return condition, {
            'min10_deviation': min10_deviation,
            'min1_deviation': min1_deviation
        }

    def check_short_entry_points_2_and_3(self, symbol):
        """
        同时检查做空进场点二和三（1分钟进场点）
        :param symbol: 交易品种
        :return: dict - 包含两个进场点的检查结果和详细信息
        """
        # 如果进场点一未触发，直接返回False
        if not self.short_entry_status[symbol]['short_entry_1']:
            return {'short_entry_2': False, 'short_entry_3': False, 'details': {}}
            
        # 检查10分钟级别的偏离度
        klines_10min = self.klines[symbol]["10min"]
        ma60_10min = ma(klines_10min.close, MA60)
        price_10min = klines_10min.close.iloc[-1]
        
        # 空头：10分钟偏离度 = (ma60-价格)/ma60
        deviation_10min = (ma60_10min.iloc[-1] - price_10min) / ma60_10min.iloc[-1] * 100
        
        klines_1min = self.klines[symbol]["1min"]
        ma20 = ma(klines_1min.close, MA20)
        ma60 = ma(klines_1min.close, MA60)
        macd = MACD(klines_1min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        
        # 空头：1分钟偏离度 = (ma60-价格)/ma60
        min1_deviation = (ma60.iloc[-1] - klines_1min.close.iloc[-1]) / ma60.iloc[-1] * 100
        
        # 进场点二的条件
        entry_point_2 = False
        # 只有当10分钟偏离值<=0.5%时才检查进场点二
        if deviation_10min <= 0.5:
            entry_point_2_condition = (
                ma20.iloc[-3] > ma60.iloc[-3] and  # -3根k线Ma20>ma60
                ma20.iloc[-2] <= ma60.iloc[-2] and  # -2根k线ma20<=ma60
                macd["bar"].iloc[-2] < 0            # -2根k线MACD<0
            )

            # 只在进场点一刚触发时检查进场点二（确保是第一个1分钟进场点）
            if entry_point_2_condition and not self.short_entry_status[symbol]['short_entry_2']:
                entry_point_2 = True
        
        # 进场点三的条件
        entry_point_3 = False
        # 检查10分钟级别的偏离度是否在0-0.3%之间
        if deviation_10min <= 0.3:
            entry_point_3_condition = (
                ma20.iloc[-3] > ma60.iloc[-3] and  # -3根k线Ma20>ma60
                ma20.iloc[-2] <= ma60.iloc[-2]      # -2根k线ma20<=ma60
            )

            # 只在首次满足条件时触发进场点三
            if entry_point_3_condition and not self.short_entry_status[symbol]['short_entry_3']:
                entry_point_3 = True
        
        details = {
            'min1_deviation': min1_deviation,
            'min10_deviation': deviation_10min
        }
        
        return {
            'short_entry_2': entry_point_2,
            'short_entry_3': entry_point_3,
            'details': details
        }

    def get_long_pool_info(self, for_feishu=False):
        """
        获取当前多头池信息
        :param for_feishu: 是否用于飞书消息（需要特殊转义）
        :return: 格式化的合约池信息
        """
        # 使用set()和update来合并两个集合
        all_long_symbols = set()
        all_long_symbols.update(self.weekly_long_symbols)
        all_long_symbols.update(self.min120_long_symbols)
        
        if not all_long_symbols:
            return "当前多头池为空"
        
        # 简单格式化输出
        symbols = sorted(all_long_symbols)
        newline = "\\n" if for_feishu else "\n"
        return f"当前多头池：{newline}{'、'.join(symbols)}"

    def get_short_pool_info(self, for_feishu=False):
        """
        获取当前空头池信息
        :param for_feishu: 是否用于飞书消息（需要特殊转义）
        :return: 格式化的合约池信息
        """
        # 使用set()和update来合并两个集合
        all_short_symbols = set()
        all_short_symbols.update(self.weekly_short_symbols)
        all_short_symbols.update(self.min120_short_symbols)
        
        if not all_short_symbols:
            return "当前空头池为空"
        
        # 简单格式化输出
        symbols = sorted(all_short_symbols)
        newline = "\\n" if for_feishu else "\n"
        return f"当前空头池：{newline}{'、'.join(symbols)}"

    def check_min120_long_pool(self, symbol):
        """
        检查是否符合120分钟级别多头池条件
        :param symbol: 交易品种
        :return: bool - 是否应该加入多头池
        """
        klines_120min = self.klines[symbol]["120min"]
        klines_week = self.klines[symbol]["week"]
        
        # 计算120分钟级别的指标
        ma20 = ma(klines_120min.close, MA20)
        ma60 = ma(klines_120min.close, MA60)
        
        # 计算周级别的MACD
        week_macd = MACD(klines_week, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        
        # 计算斜率
        ma60_slope = self.calculate_slope(ma60, 5)
        ma20_slope = self.calculate_slope(ma20, 5)
        week_macd_slope = self.calculate_slope(week_macd["bar"], 3)
        
        # 获取当前时间
        current_time = datetime.fromtimestamp(self.klines[symbol]["120min"].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')
        
        # 检查是否需要清除出合约池（只清除120分钟级别的池）
        if klines_120min.close.iloc[-1] < ma60.iloc[-1]:
            if symbol in self.min120_long_symbols:  # 只从120分钟池中移除
                self.min120_long_symbols.remove(symbol)
                # 重置多头入场点状态
                self.long_entry_status[symbol]['long_entry_1'] = False
                self.long_entry_status[symbol]['long_entry_2'] = False
                self.long_entry_status[symbol]['long_entry_3'] = False
                logger.info(f"\n时间: {current_time}")
                logger.info(f"{symbol} 从120分钟多头池中移除：120分钟K线收盘价 < MA60\n{self.get_long_pool_info()}")
            return False
        
        # 检查是否符合入池条件
        condition = (
            ma60_slope > 0 and  # 120分钟Ma60斜率>0
            klines_120min.close.iloc[-1] > ma60.iloc[-1] and  # 120分钟价格>Ma60
            ma20.iloc[-1] > ma60.iloc[-1] and  # 120分钟ma20>120分钟ma60
            (
                ma20_slope > 0 or  # ma20斜率>0
                (
                    week_macd["bar"].iloc[-2] > 0 and  # 周级别-2根k线MACD>0
                    week_macd_slope > 0  # 周级别MACD斜率>0
                )
            )
        )
        
        # 如果满足条件且不在池中，添加并记录时间
        if condition and symbol not in self.min120_long_symbols:
            logger.info(f"\n时间: {current_time}")
            logger.info(f"{symbol} 加入120分钟多头池\n{self.get_long_pool_info()}")
        
        return condition

    def check_min120_short_pool(self, symbol):
        """
        检查是否符合120分钟级别空头池条件
        :param symbol: 交易品种
        :return: bool - 是否应该加入空头池
        """
        klines_120min = self.klines[symbol]["120min"]
        klines_week = self.klines[symbol]["week"]
        
        # 计算120分钟级别的指标
        ma20 = ma(klines_120min.close, MA20)
        ma60 = ma(klines_120min.close, MA60)
        
        # 计算周级别的MACD
        week_macd = MACD(klines_week, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        
        # 计算斜率
        ma60_slope = self.calculate_slope(ma60, 5)
        ma20_slope = self.calculate_slope(ma20, 5)
        week_macd_slope = self.calculate_slope(week_macd["bar"], 3)
        
        # 获取当前时间
        current_time = datetime.fromtimestamp(self.klines[symbol]["120min"].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')
        
        # 检查是否需要清除出合约池（只清除120分钟级别的池）
        if klines_120min.close.iloc[-1] > ma60.iloc[-1]:
            if symbol in self.min120_short_symbols:  # 只从120分钟池中移除
                self.min120_short_symbols.remove(symbol)
                # 重置空头入场点状态
                self.short_entry_status[symbol]['short_entry_1'] = False
                self.short_entry_status[symbol]['short_entry_2'] = False
                self.short_entry_status[symbol]['short_entry_3'] = False
                logger.info(f"\n时间: {current_time}")
                logger.info(f"{symbol} 从120分钟空头池中移除：120分钟K线收盘价 > MA60\n{self.get_short_pool_info()}")
            return False
        
        # 检查是否符合入池条件
        condition = (
            ma60_slope < 0 and  # 120分钟Ma60斜率<0
            klines_120min.close.iloc[-1] < ma60.iloc[-1] and  # 120分钟价格<Ma60
            ma20.iloc[-1] < ma60.iloc[-1] and  # 120分钟ma20<120分钟ma60
            (
                ma20_slope < 0 or  # ma20斜率<0
                (
                    week_macd["bar"].iloc[-2] < 0 and  # 周级别-2根k线MACD<0
                    week_macd_slope < 0  # 周级别MACD斜率<0
                )
            )
        )
        
        # 如果满足条件且不在池中，添加并记录时间
        if condition and symbol not in self.min120_short_symbols:
            logger.info(f"\n时间: {current_time}")
            logger.info(f"{symbol} 加入120分钟空头池\n{self.get_short_pool_info()}")
        
        return condition

    def run(self):
        """运行策略"""
        try:
            logger.info("策略开始运行")
            
            while True:
                try:
                    self.api.wait_update()
                    
                    for symbol in SYMBOLS:
                        in_long = symbol in self.weekly_long_symbols or symbol in self.min120_long_symbols
                        in_short = symbol in self.weekly_short_symbols or symbol in self.min120_short_symbols

                        if in_long and in_short:
                            logger.error(f"{symbol} 同时在多头和空头池，逻辑错误，已跳过！")
                            continue

                        if in_long:
                            # 多头逻辑
                            # 检查120分钟级别合约池（不变）
                            if self.check_data_ready(symbol, "120min") and self.api.is_changing(self.klines[symbol]["120min"].iloc[-1], "datetime"):
                                try:
                                    long_pool_condition = self.check_min120_long_pool(symbol)
                                    if long_pool_condition and symbol not in self.min120_long_symbols:
                                        self.min120_long_symbols.add(symbol)
                                        logger.info(f"\n时间: {datetime.fromtimestamp(self.klines[symbol]['120min'].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        logger.info(f"{symbol} 加入120分钟多头池\n{self.get_long_pool_info()}")
                                    short_pool_condition = self.check_min120_short_pool(symbol)
                                    if short_pool_condition and symbol not in self.min120_short_symbols:
                                        self.min120_short_symbols.add(symbol)
                                        logger.info(f"\n时间: {datetime.fromtimestamp(self.klines[symbol]['120min'].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        logger.info(f"{symbol} 加入120分钟空头池\n{self.get_short_pool_info()}")
                                except Exception as e:
                                    logger.error(f"处理120分钟级别合约池时发生错误: {str(e)}")
                            # 检查做多信号 - 10分钟K线变化时只检测进场点一
                            if self.check_data_ready(symbol, "10min") and self.api.is_changing(self.klines[symbol]["10min"].iloc[-1], "datetime"):
                                try:
                                    entry_point_1, details_1 = self.check_entry_point_1(symbol)
                                    if entry_point_1:
                                        self.long_entry_status[symbol]['long_entry_1'] = True
                                        current_time = self.klines[symbol]["1min"].datetime.iloc[-1]
                                        logger.info(f"\n时间: {datetime.fromtimestamp(current_time / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        logger.info(f"{symbol} 触发多头进场点一:")
                                        logger.info(f"10分钟偏离: {details_1['min10_deviation']:.2f}%")
                                        self.feishu_bot.send_weekly_long_signal(
                                            symbol, 
                                            'entry', 
                                            {
                                                'long_entry_1': True,
                                                'long_entry_2': False,
                                                'long_entry_3': False,
                                                **details_1
                                            }, 
                                            timestamp=current_time
                                        )
                                except Exception as e:
                                    logger.error(f"处理做多信号时发生错误: {str(e)}")
                            # 检查做多信号 - 进场点1已触发时，每次1分钟K线变化都检测进场点2和3
                            if self.long_entry_status[symbol]['long_entry_1'] and self.api.is_changing(self.klines[symbol]["1min"].iloc[-1], "datetime"):
                                try:
                                    result = self.check_entry_points_2_and_3(symbol)
                                    entry_point_2 = result['long_entry_2']
                                    entry_point_3 = result['long_entry_3']
                                    details = result['details']
                                    if (entry_point_2 and not self.long_entry_status[symbol]['long_entry_2']) or \
                                       (entry_point_3 and not self.long_entry_status[symbol]['long_entry_3']):
                                        if entry_point_2:
                                            self.long_entry_status[symbol]['long_entry_2'] = True
                                        if entry_point_3:
                                            self.long_entry_status[symbol]['long_entry_3'] = True
                                        current_time = self.klines[symbol]["1min"].datetime.iloc[-1]
                                        logger.info(f"\n时间: {datetime.fromtimestamp(current_time / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        if entry_point_2:
                                            logger.info(f"{symbol} 触发多头进场点2:")
                                            logger.info(f"10分钟偏离: {details['min10_deviation']:.2f}%")
                                        if entry_point_3:
                                            logger.info(f"{symbol} 触发多头进场点3:")
                                            logger.info(f"10分钟偏离: {details['min10_deviation']:.2f}%")
                                        self.feishu_bot.send_weekly_long_signal(
                                            symbol, 
                                            'entry', 
                                            {
                                                'long_entry_1': self.long_entry_status[symbol]['long_entry_1'],
                                                'long_entry_2': self.long_entry_status[symbol]['long_entry_2'],
                                                'long_entry_3': self.long_entry_status[symbol]['long_entry_3'],
                                                **details
                                            }, 
                                            timestamp=current_time
                                        )
                                except Exception as e:
                                    logger.error(f"处理做多信号时发生错误: {str(e)}")
                        elif in_short:
                            # 空头逻辑
                            # 检查120分钟级别合约池（不变）
                            if self.check_data_ready(symbol, "120min") and self.api.is_changing(self.klines[symbol]["120min"].iloc[-1], "datetime"):
                                try:
                                    long_pool_condition = self.check_min120_long_pool(symbol)
                                    if long_pool_condition and symbol not in self.min120_long_symbols:
                                        self.min120_long_symbols.add(symbol)
                                        logger.info(f"\n时间: {datetime.fromtimestamp(self.klines[symbol]['120min'].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        logger.info(f"{symbol} 加入120分钟多头池\n{self.get_long_pool_info()}")
                                    short_pool_condition = self.check_min120_short_pool(symbol)
                                    if short_pool_condition and symbol not in self.min120_short_symbols:
                                        self.min120_short_symbols.add(symbol)
                                        logger.info(f"\n时间: {datetime.fromtimestamp(self.klines[symbol]['120min'].datetime.iloc[-1] / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        logger.info(f"{symbol} 加入120分钟空头池\n{self.get_short_pool_info()}")
                                except Exception as e:
                                    logger.error(f"处理120分钟级别合约池时发生错误: {str(e)}")
                            # 检查做空信号 - 10分钟K线变化时只检测进场点一
                            if self.check_data_ready(symbol, "10min") and self.api.is_changing(self.klines[symbol]["10min"].iloc[-1], "datetime"):
                                try:
                                    entry_point_1, details_1 = self.check_short_entry_point_1(symbol)
                                    if entry_point_1:
                                        self.short_entry_status[symbol]['short_entry_1'] = True
                                        current_time = self.klines[symbol]["1min"].datetime.iloc[-1]
                                        logger.info(f"\n时间: {datetime.fromtimestamp(current_time / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        logger.info(f"{symbol} 触发空头进场点一:")
                                        logger.info(f"10分钟偏离: {details_1['min10_deviation']:.2f}%")
                                        self.feishu_bot.send_weekly_short_signal(
                                            symbol, 
                                            'entry', 
                                            {
                                                'short_entry_1': True,
                                                'short_entry_2': False,
                                                'short_entry_3': False,
                                                **details_1
                                            }, 
                                            timestamp=current_time
                                        )
                                except Exception as e:
                                    logger.error(f"处理做空信号时发生错误: {str(e)}")
                            # 检查做空信号 - 进场点1已触发时，每次1分钟K线变化都检测进场点2和3
                            if self.short_entry_status[symbol]['short_entry_1'] and self.api.is_changing(self.klines[symbol]["1min"].iloc[-1], "datetime"):
                                try:
                                    result = self.check_short_entry_points_2_and_3(symbol)
                                    entry_point_2 = result['short_entry_2']
                                    entry_point_3 = result['short_entry_3']
                                    details = result['details']
                                    if (entry_point_2 and not self.short_entry_status[symbol]['short_entry_2']) or \
                                       (entry_point_3 and not self.short_entry_status[symbol]['short_entry_3']):
                                        if entry_point_2:
                                            self.short_entry_status[symbol]['short_entry_2'] = True
                                        if entry_point_3:
                                            self.short_entry_status[symbol]['short_entry_3'] = True
                                        current_time = self.klines[symbol]["1min"].datetime.iloc[-1]
                                        logger.info(f"\n时间: {datetime.fromtimestamp(current_time / 1e9).strftime('%Y-%m-%d %H:%M:%S')}")
                                        if entry_point_2:
                                            logger.info(f"{symbol} 触发空头进场点2:")
                                            logger.info(f"10分钟偏离: {details['min10_deviation']:.2f}%")
                                        if entry_point_3:
                                            logger.info(f"{symbol} 触发空头进场点3:")
                                            logger.info(f"10分钟偏离: {details['min10_deviation']:.2f}%")
                                        self.feishu_bot.send_weekly_short_signal(
                                            symbol, 
                                            'entry', 
                                            {
                                                'short_entry_1': self.short_entry_status[symbol]['short_entry_1'],
                                                'short_entry_2': self.short_entry_status[symbol]['short_entry_2'],
                                                'short_entry_3': self.short_entry_status[symbol]['short_entry_3'],
                                                **details
                                            }, 
                                            timestamp=current_time
                                        )
                                except Exception as e:
                                    logger.error(f"处理做空信号时发生错误: {str(e)}")
                        else:
                            # 不在任何池，跳过
                            continue
                
                except KeyboardInterrupt:
                    logger.info("收到键盘中断信号，正在退出...")
                    break
                except Exception as e:
                    if str(e) == '回测结束':
                        logger.info("回测正常结束")
                        break
                    else:
                        logger.error(f"策略运行时发生错误: {str(e)}")
                        continue
            
        except Exception as e:
            logger.error(f"策略运行时发生严重错误: {str(e)}")
        finally:
            try:
                self.api.close()
                logger.info("策略已安全退出")
            except:
                pass

if __name__ == "__main__":
    strategy = TrendFollowStrategy()
    strategy.run() 