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
log_file = os.path.join(current_dir, 'TradeSignalNotifier.log')
# 设置日志记录器
logger = setup_logger('macd_signal', log_file)

# 策略参数设置
SYMBOLS = [
        #     'DCE.eb2506', 'DCE.eg2509', 'DCE.j2509', 'DCE.i2509',
        #    'DCE.a2507', 'DCE.b2509', 'DCE.c2507', 'DCE.cs2507',
        #     'DCE.jd2506', 'DCE.jm2509', 'DCE.l2509', 'DCE.lg2507',
        #     'DCE.lh2509','DCE.m2509', 'DCE.p2509', 'DCE.pg2506',
        #    'DCE.pp2509', 'DCE.v2509', 'DCE.y2509',

        #    'CZCE.AP510', 'CZCE.CF509', 'CZCE.CJ509', 'CZCE.FG509',
        #    'CZCE.MA509', 'CZCE.OI509', 'CZCE.PF509', 'CZCE.PK510',
        #    'CZCE.PR509', 'CZCE.PX509', 'CZCE.RM509','CZCE.SA509', 'CZCE.SF509', 'CZCE.SH509', 'CZCE.SM509',
        #    'CZCE.SR509', 'CZCE.TA509', 'CZCE.UR509',

           'SHFE.ag2506', 'SHFE.al2506', 'SHFE.ao2509', 'SHFE.au2506',
           'SHFE.br2506', 'SHFE.bu2506', 'SHFE.cu2506', 'SHFE.zn2506',
           'SHFE.fu2507', 'SHFE.hc2510', 'SHFE.ni2506',
           'SHFE.pb2506', 'SHFE.rb2510', 'SHFE.ru2509',
           'SHFE.sn2506','SHFE.sp2507', 'SHFE.ss2506',

        #    'INE.sc2506',  'INE.nr2506', 'INE.ec2506', 'INE.lu2506', 

        #    'GFEX.lc2507', 'GFEX.si2506', 'GFEX.ps2506'

    # "CZCE.FG505",  # 玻璃2505
    # "DCE.m2505",   # 豆粕2505
    # "SHFE.ag2505", # 沪银2505
    # "DCE.p2505",   # 棕榈2505
    # "SHFE.cu2506", # 沪铜2506
    # "SHFE.au2506", # 沪金2506
    # "SHFE.sn2505", # 沪锡2505
    # "CZCE.OI505",  # 菜油2505
    # "SHFE.al2505", # 氧化铝2505
    # "CZCE.CF505",  # 棉花2505
    # "DCE.eb2505",  # 苯乙烯2505
    # "CZCE.SH505",  # 烧碱2505
    # # "CZCE.AP505",  # 苹果2505
    # # "DCE.v2505",   # PVC2505
    # "SHFE.ru2509", # 橡胶2505
    # # "DCE.lh2505",  # 生猪2505
    # "GFEX.ps2506"
]
# 回测参数设置
BACKTEST_CONFIG = {
    "enabled": True,  # 是否启用回测模式
    "start_dt": date(2025, 1, 1),  # 回测开始日期
    "end_dt": date(2025, 1, 26)    # 回测结束日期
}

# 偏离度参数设置（抽离出来方便调整）
MA20_DEVIATION_THRESHOLD = 0.003   # MA20偏离度阈值（0.3%）

# 均线参数设置
MA_SHORT = 20   # 短期均线周期
MA_LONG = 60    # 长期均线周期
MA20_SLOPE_PERIOD = 5  # MA20斜率计算周期（回取5个周期）
MA60_SLOPE_PERIOD = 3  # MA60斜率计算周期（回取3个周期）

# MACD参数设置
MACD_FAST = 10    # MACD快线周期
MACD_SLOW = 23    # MACD慢线周期
MACD_SIGNAL = 8   # MACD信号线周期

# KDJ参数设置
KDJ_N = 9     # KDJ的N参数
KDJ_M1 = 3    # KDJ的M1参数
KDJ_M2 = 3    # KDJ的M2参数

# 资金控制参数
MAX_CAPITAL_PER_SYMBOL = 500000  # 每个品种最大资金限制（50万）

class FeishuBot:
    def __init__(self):
        """初始化飞书机器人"""
        self.webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/a3a70ac6-21d8-47c9-8e34-9307046fcc18"
        self.secret = "YCmjC25sSNAnMuugDklaIe"
        self.signal_buffer = {}  # 按分钟时间戳分组的信号缓冲区
        self.last_send_minute = -1  # 上次发送的分钟
    
    def _generate_sign(self, timestamp):
        """生成签名"""
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        return sign
    
    def add_signal(self, timestamp, period, signal_info):
        """添加信号到缓冲区"""
        try:
            # 将纳秒时间戳转换为分钟时间戳（向下取整到分钟）
            minute_timestamp = (timestamp // (60 * 1e9)) * (60 * 1e9)
            
            if minute_timestamp not in self.signal_buffer:
                self.signal_buffer[minute_timestamp] = []
            
            # 添加调试日志
            logger.info(f"添加信号到缓冲区 - 时间戳: {minute_timestamp}, 周期: {period}")
            
            self.signal_buffer[minute_timestamp].append({
                'timestamp': minute_timestamp,
                'period': period,
                'info': signal_info
            })
        except Exception as e:
            logger.error(f"添加信号到缓冲区时出错: {str(e)}")
            logger.error(traceback.format_exc())
    
    def _send_signals(self, timestamp):
        """发送指定时间戳的信号"""
        try:
            # 将纳秒时间戳转换为分钟时间戳
            minute_timestamp = (timestamp // (60 * 1e9)) * (60 * 1e9)
            
            signals = self.signal_buffer.get(minute_timestamp, [])
            if not signals:
                logger.info(f"没有找到时间戳 {minute_timestamp} 的信号")
                return
            
            # 添加调试日志
            logger.info(f"准备发送信号 - 时间戳: {minute_timestamp}, 信号数量: {len(signals)}")
            
            # 生成时间戳和签名
            timestamp_ms = int(time.time() * 1000)
            sign = self._generate_sign(timestamp_ms)
            
            # 构建消息内容
            data = self._format_signals(signals)
            data.update({
                "timestamp": timestamp_ms,
                "sign": sign,
            })
            
            # 发送请求
            try:
                response = requests.post(
                    self.webhook,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(data)
                )
                if response.status_code != 200:
                    logger.error(f"发送飞书消息失败: {response.text}")
                else:
                    logger.info(f"成功发送{len(signals)}个信号到飞书")
                    # 删除已发送的信号
                    del self.signal_buffer[minute_timestamp]
            except Exception as e:
                logger.error(f"发送飞书消息出错: {str(e)}")
        except Exception as e:
            logger.error(f"_send_signals执行出错: {str(e)}")
            logger.error(traceback.format_exc())
    
    def _format_signals(self, signals):
        """格式化信号消息"""
        # 合约代码映射
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

        # 定义周期显示名称
        period_display = {
            '10min': '**【10分钟】**',
            '15min': '**【15分钟】**',
            '30min': '**【30分钟】**',
            '60min': '**【60分钟】**',
            '120min': '**【120分钟】**',
            'daily': '**【日线】**',
            'weekly': '**【周线】**'
        }
        
        # 格式化消息
        dt = datetime.fromtimestamp(signals[0]['timestamp'] / 1e9)
        time_str = dt.strftime('%m月%d日 %H:%M')
        
        # 判断是否是收盘时间
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = dt.astimezone(beijing_tz)
        is_closing_time = (
            (beijing_time.hour == 14 and beijing_time.minute == 57)
        )
        
        # 根据时间设置不同的标题
        title = f"收盘建仓信号：{time_str}" if is_closing_time else f"建仓信号：{time_str}"
        
        # 构建飞书卡片消息
        card = {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title
                },
                "template": "green"
            },
            "elements": []
        }
        
        # 按周期分组信号
        period_groups = defaultdict(list)
        for signal in signals:
            period_groups[signal['period']].append(signal['info'])
        
        # 构建消息内容
        message_parts = []
        
        # 按周期顺序处理信号
        period_order = ['10min', '15min', '30min', '60min', '120min', 'daily', 'weekly']
        for period in period_order:
            if period in period_groups:
                # 添加周期标题
                message_parts.append(period_display[period])
                
                # 按信号类型优先级排序（标准 > 左侧 > 近似）
                signal_priority = {"标准": 1, "左侧": 2, "近似": 3}
                period_signals = sorted(period_groups[period], 
                    key=lambda x: (x['symbol'], signal_priority.get(x.get('signal_type', ''), 999)))
                
                # 格式化每个信号
                for signal in period_signals:
                    # 转换合约代码
                    symbol = signal['symbol']
                    for exchange_code, display_name in exchange_symbol_map.items():
                        if symbol.startswith(exchange_code):
                            contract_num = symbol.split('.')[-1]
                            # 根据交易方向设置颜色和加粗
                            color = "red" if signal['direction'] == "多头" else "green"
                            symbol_display = f"**<font color='{color}'>{display_name}{contract_num}</font>**"
                            break
                    else:
                        color = "red" if signal['direction'] == "多头" else "green"
                        symbol_display = f"**<font color='{color}'>{symbol}</font>**"
                    
                    # 方向符号和颜色
                    if signal['direction'] == "多头":
                        direction_display = "<font color='red'>多</font>"
                    else:
                        direction_display = "<font color='green'>空</font>"
                    
                    # 只在10分钟和15分钟级别显示信号类型
                    signal_type_display = ""
                    if period in ['10min', '15min'] and 'signal_type' in signal:
                        signal_type_display = f"**{signal['signal_type']}** "
                    
                    # 构建基本信号行
                    signal_line = (
                        f"{symbol_display} {direction_display} {signal_type_display} "
                        f"偏离 <font color='blue'>{signal['deviation']:.2f}%</font> "
                    )
                    
                    # 只在10分钟和15分钟级别显示120分钟偏离度
                    if period in ['10min', '15min']:
                        deviation_120min = signal['deviation_120min']
                        if abs(deviation_120min) > 2:
                            deviation_120min_display = f"<font color='red'>{deviation_120min:.2f}%</font>⚠️"
                        else:
                            deviation_120min_display = f"<font color='blue'>{deviation_120min:.2f}%</font>"
                        signal_line += f"120偏离 {deviation_120min_display} "
                    
                    # 添加日增仓信息
                    signal_line += f"日增仓 <font color='purple'>{signal['oi_ratio']:.1f}%</font>"
                    
                    message_parts.append(signal_line)
        
        # 将所有消息组合成一个字符串
        message_content = "\n".join(message_parts)
        
        # 添加消息到卡片
        card["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": message_content
            }
        })
        
        return {
            "msg_type": "interactive",
            "card": card
        }

class MACDStrategy:
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
        
        # 初始化飞书机器人
        self.feishu_bot = FeishuBot()
        
        # 初始化数据存储
        self.klines = {}  # K线数据
        self.quotes = {}  # 行情数据
        
        # 添加收盘信号控制标志
        self.last_closing_check = None  # 上次收盘检查的时间
        
        # 初始化各个品种的数据
        for symbol in SYMBOLS:
            # 获取K线数据
            self.klines[symbol] = {
                "1min": self.api.get_kline_serial(symbol, duration_seconds=60),
                "10min": self.api.get_kline_serial(symbol, duration_seconds=10*60),
                "15min": self.api.get_kline_serial(symbol, duration_seconds=15*60),
                "30min": self.api.get_kline_serial(symbol, duration_seconds=30*60),
                "60min": self.api.get_kline_serial(symbol, duration_seconds=60*60),
                "120min": self.api.get_kline_serial(symbol, duration_seconds=120*60),
                "daily": self.api.get_kline_serial(symbol, duration_seconds=24*60*60),
                "weekly": self.api.get_kline_serial(symbol, duration_seconds=7*24*60*60),
            }
            
            # 获取实时行情
            self.quotes[symbol] = self.api.get_quote(symbol)
        
        # 添加回测统计数据存储
        self.backtest_signals = []  # 存储所有回测信号
        
        logger.info("策略初始化完成")
    
    def __del__(self):
        if hasattr(self, 'api'):
            try:
                self.api.close()
            except Exception:
                pass
    
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
    
    def calculate_ma_slopes(self, klines):
        """
        计算MA20和MA60的斜率
        :param klines: K线数据
        :return: (ma20_slope, ma60_slope)
        """
        # 计算均线
        ma20_values = ma(klines.close, MA_SHORT)
        ma60_values = ma(klines.close, MA_LONG)
        
        # 计算斜率
        ma20_slope = self.calculate_slope(ma20_values, MA20_SLOPE_PERIOD)  # MA20回取5个周期
        ma60_slope = self.calculate_slope(ma60_values, MA60_SLOPE_PERIOD)  # MA60回取3个周期
        
        return ma20_slope, ma60_slope
    
    def calculate_price_deviation(self, price, ma_value):
        """
        计算价格偏离度
        :param price: 当前价格
        :param ma_value: 均线值
        :return: 偏离度
        """
        return abs(price - ma_value) / ma_value
    
    def convert_to_beijing_time(self, timestamp_nano):
        """
        将纳秒时间戳转换为北京时间
        :param timestamp_nano: 纳秒时间戳
        :return: 北京时间字符串
        """
        dt = datetime.fromtimestamp(timestamp_nano / 1e9)
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = dt.astimezone(beijing_tz)
        return beijing_time.strftime('%Y-%m-%d %H:%M:%S')
    
    def log_signal(self, symbol, direction, level, period, price, ma60_value, signal_details=None):
        """记录信号日志并推送到飞书"""
        # 获取当前周期K线数据并计算指标
        klines = self.klines[symbol][period]
        ma20 = ma(klines.close, MA_SHORT)
        ma60 = ma(klines.close, MA_LONG)
        macd = MACD(klines, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        ma20_slope, ma60_slope = self.calculate_ma_slopes(klines)
        
        # 生成简洁的指标对比日志
        time = self.convert_to_beijing_time(klines.datetime.iloc[-1])
        log_message = f"\n========== {symbol} 【{period}】 {direction} {level} {time} ==========\n"
        
        # 价格与均线关系
        log_message += f"价格: {price:.2f} {'>' if price > ma20.iloc[-1] else '<'} MA20: {ma20.iloc[-1]:.2f} {'>' if ma20.iloc[-1] > ma60.iloc[-1] else '<'} MA60: {ma60.iloc[-1]:.2f}\n"
        
        # 均线斜率
        log_message += f"斜率 - MA20: {ma20_slope:.6f} (上升), MA60: {ma60_slope:.6f} (上升)\n"
        
        # 计算并显示MA20偏离度
        ma20_deviation = self.calculate_price_deviation(price, ma20.iloc[-1]) * 100
        log_message += f"MA20偏离度: {ma20_deviation:.2f}%\n"
        
        # MACD详细信息
        log_message += f"MACD详情:\n"
        log_message += f"  BAR: 前值({macd['bar'].iloc[-3]:.4f}) {'<' if macd['bar'].iloc[-3] < 0 else '>'} 0 → 当前({macd['bar'].iloc[-2]:.4f}) {'>' if macd['bar'].iloc[-2] > 0 else '<'} 0\n"
        log_message += f"  DIFF: {macd['diff'].iloc[-2]:.4f} {'>' if macd['diff'].iloc[-2] > 0 else '<'} 0\n"
        log_message += f"  DEA: {macd['dea'].iloc[-2]:.4f} {'>' if macd['dea'].iloc[-2] > 0 else '<'} 0\n"
        
        logger.info(log_message)
    
    def check_standard_signal(self, symbol, period):
        """
        检查10分钟/15分钟级别的标准信号（1级信号）
        采用快速失败策略：任何一个级别条件不满足就立即返回
        :param symbol: 交易品种
        :param period: K线周期（10min或15min）
        :return: (bool, bool) - (多头信号, 空头信号)
        """
        # 1. 获取当前周期数据并计算指标
        klines = self.klines[symbol][period]
        ma20 = ma(klines.close, MA_SHORT)
        ma60 = ma(klines.close, MA_LONG)
        macd = MACD(klines, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        ma20_slope, ma60_slope = self.calculate_ma_slopes(klines)
        
        # 2. 先检查当前周期的基本条件
        # 2.1 检查多头基本条件
        current_long_macd = (
            macd["bar"].iloc[-3] < 0 and
            macd["bar"].iloc[-2] > 0 and
            macd["dea"].iloc[-2] > 0 and
            macd["diff"].iloc[-2] > 0
        )
        current_long_ma = klines.close.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]
        
        # 计算当前价格与均线的偏离度
        current_price = klines.close.iloc[-1]
        ma20_deviation = abs(current_price - ma20.iloc[-1]) / ma20.iloc[-1]
        
        # 如果当前周期的多头基本条件满足，继续检查多头的其他条件
        if current_long_macd and current_long_ma and ma60_slope > 0:
            # 检查偏离度条件
            if (ma20_deviation > MA20_DEVIATION_THRESHOLD):
                return False, False
            
            # 3. 获取其他周期数据并计算指标
            klines_120min = self.klines[symbol]["120min"]
            klines_daily = self.klines[symbol]["daily"]
            klines_60min = self.klines[symbol]["60min"]
            klines_30min = self.klines[symbol]["30min"]
            
            # 120分钟指标
            ma20_120 = ma(klines_120min.close, MA_SHORT)
            ma60_120 = ma(klines_120min.close, MA_LONG)
            macd_120 = MACD(klines_120min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            ma20_slope_120, ma60_slope_120 = self.calculate_ma_slopes(klines_120min)
            kdj_120 = KDJ(klines_120min, KDJ_N, KDJ_M1, KDJ_M2)
            
            # 日线指标
            ma20_daily = ma(klines_daily.close, MA_SHORT)
            ma60_daily = ma(klines_daily.close, MA_LONG)
            macd_daily = MACD(klines_daily, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            ma20_slope_daily, ma60_slope_daily = self.calculate_ma_slopes(klines_daily)
            
            # 60分钟指标
            ma20_60 = ma(klines_60min.close, MA_SHORT)
            ma60_60 = ma(klines_60min.close, MA_LONG)
            _, ma60_slope_60 = self.calculate_ma_slopes(klines_60min)
            
            # 30分钟指标
            ma20_30 = ma(klines_30min.close, MA_SHORT)
            ma60_30 = ma(klines_30min.close, MA_LONG)
            _, ma60_slope_30 = self.calculate_ma_slopes(klines_30min)
            
            # 检查其他周期的多头条件
            # 120分钟条件
            long_120min = (
                klines_120min.close.iloc[-1] > ma20_120.iloc[-1] > ma60_120.iloc[-1] and
                macd_120["bar"].iloc[-2] > 0 and
                macd_120["bar"].iloc[-1] > 0 and
                ma60_slope_120 > 0
            )
            if not long_120min:
                return False, False
            
            # 日线条件
            long_daily = (
                klines_daily.close.iloc[-1] > ma20_daily.iloc[-1] and
                klines_daily.close.iloc[-1] > ma60_daily.iloc[-1]
            )
            if not long_daily:
                return False, False
            
            # 60分钟条件
            long_60min = (
                klines_60min.close.iloc[-1] > ma20_60.iloc[-1] > ma60_60.iloc[-1] and
                ma60_slope_60 > 0
            )
            if not long_60min:
                return False, False
            
            # 30分钟条件
            long_30min = (
                klines_30min.close.iloc[-1] > ma20_30.iloc[-1] > ma60_30.iloc[-1] and
                ma60_slope_30 > 0
            )
            if not long_30min:
                return False, False
            
            # 检查斜率条件
            long_slope_condition1 = ma20_slope > 0  # 10分钟Ma20斜率>0
            long_slope_condition2 = (
                kdj_120["k"].iloc[-1] > kdj_120["d"].iloc[-1] and  # 120分钟K>D
                ma20_slope_120 > 0 and  # 120分钟ma20斜率>0
                ma20_daily.iloc[-1] > ma60_daily.iloc[-1] and  # 日级别ma20>ma60
                macd_daily["bar"].iloc[-1] > 0 and  # 日级别MACD>0
                (ma60_slope_daily > 0 or ma20_slope_daily > 0)  # 日级别ma60斜率>0 或 ma20斜率>0
            )
            
            if ma60_slope > 0 and (long_slope_condition1 
                                #    or long_slope_condition2
                                   ):
                return True, False  # 满足所有多头条件，直接返回
            
            return False, False  # 不满足斜率条件
        
        # 2.2 检查空头基本条件
        current_short_macd = (
            macd["bar"].iloc[-3] > 0 and
            macd["bar"].iloc[-2] < 0 and
            macd["dea"].iloc[-2] < 0 and
            macd["diff"].iloc[-2] < 0
        )
        current_short_ma = klines.close.iloc[-1] < ma20.iloc[-1] < ma60.iloc[-1]
        
        # 如果当前周期的空头基本条件满足，继续检查空头的其他条件
        if current_short_macd and current_short_ma and ma60_slope < 0:
            # 检查偏离度条件
            if (ma20_deviation > MA20_DEVIATION_THRESHOLD):
                return False, False
            
            # 3. 获取其他周期数据并计算指标
            klines_120min = self.klines[symbol]["120min"]
            klines_daily = self.klines[symbol]["daily"]
            klines_60min = self.klines[symbol]["60min"]
            klines_30min = self.klines[symbol]["30min"]
            
            # 120分钟指标
            ma20_120 = ma(klines_120min.close, MA_SHORT)
            ma60_120 = ma(klines_120min.close, MA_LONG)
            macd_120 = MACD(klines_120min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            ma20_slope_120, ma60_slope_120 = self.calculate_ma_slopes(klines_120min)
            kdj_120 = KDJ(klines_120min, KDJ_N, KDJ_M1, KDJ_M2)
            
            # 日线指标
            ma20_daily = ma(klines_daily.close, MA_SHORT)
            ma60_daily = ma(klines_daily.close, MA_LONG)
            macd_daily = MACD(klines_daily, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            ma20_slope_daily, ma60_slope_daily = self.calculate_ma_slopes(klines_daily)
            
            # 60分钟指标
            ma20_60 = ma(klines_60min.close, MA_SHORT)
            ma60_60 = ma(klines_60min.close, MA_LONG)
            _, ma60_slope_60 = self.calculate_ma_slopes(klines_60min)
            
            # 30分钟指标
            ma20_30 = ma(klines_30min.close, MA_SHORT)
            ma60_30 = ma(klines_30min.close, MA_LONG)
            _, ma60_slope_30 = self.calculate_ma_slopes(klines_30min)
            
            # 检查其他周期的空头条件
            # 120分钟条件
            short_120min = (
                klines_120min.close.iloc[-1] < ma20_120.iloc[-1] < ma60_120.iloc[-1] and
                macd_120["bar"].iloc[-2] < 0 and
                macd_120["bar"].iloc[-1] < 0 and
                ma60_slope_120 < 0
            )
            if not short_120min:
                return False, False
            
            # 日线条件
            short_daily = (
                klines_daily.close.iloc[-1] < ma20_daily.iloc[-1] and
                klines_daily.close.iloc[-1] < ma60_daily.iloc[-1]
            )
            if not short_daily:
                return False, False
            
            # 60分钟条件
            short_60min = (
                klines_60min.close.iloc[-1] < ma20_60.iloc[-1] < ma60_60.iloc[-1] and
                ma60_slope_60 < 0
            )
            if not short_60min:
                return False, False
            
            # 30分钟条件
            short_30min = (
                klines_30min.close.iloc[-1] < ma20_30.iloc[-1] < ma60_30.iloc[-1] and
                ma60_slope_30 < 0
            )
            if not short_30min:
                return False, False
            
            # 检查斜率条件
            short_slope_condition1 = ma20_slope < 0  # 10分钟Ma20斜率<0
            short_slope_condition2 = (
                kdj_120["k"].iloc[-1] < kdj_120["d"].iloc[-1] and  # 120分钟K<D
                ma20_slope_120 < 0 and  # 120分钟ma20斜率<0
                ma20_daily.iloc[-1] < ma60_daily.iloc[-1] and  # 日级别ma20<ma60
                macd_daily["bar"].iloc[-1] < 0 and  # 日级别MACD<0
                (ma60_slope_daily < 0 or ma20_slope_daily < 0)  # 日级别ma60斜率<0 或 ma20斜率<0
            )
            
            if ma60_slope < 0 and (short_slope_condition1 
                                   or short_slope_condition2
                                   ):
                return False, True  # 满足所有空头条件
        
        return False, False  # 不满足任何条件
    
    def check_left_side_short_signal(self, symbol, period):
        """
        检查左侧空头信号（2级信号）
        :param symbol: 交易品种
        :param period: 时间周期（10min或15min）
        :return: bool
        """
        # 1. 检查日线级别条件
        klines_daily = self.klines[symbol]["daily"]
        ma20_daily = ma(klines_daily.close, MA_SHORT)
        ma60_daily = ma(klines_daily.close, MA_LONG)
        macd_daily = MACD(klines_daily, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        _, ma60_slope_daily = self.calculate_ma_slopes(klines_daily)
        
        daily_condition = (
            klines_daily.close.iloc[-1] < ma20_daily.iloc[-1] and
            klines_daily.close.iloc[-1] < ma60_daily.iloc[-1] and
            macd_daily["bar"].iloc[-2] < 0 and
            ma60_slope_daily < 0
        )
        
        if not daily_condition:
            return False
        
        # 2. 检查120分钟条件
        klines_120min = self.klines[symbol]["120min"]
        ma20_120 = ma(klines_120min.close, MA_SHORT)
        ma60_120 = ma(klines_120min.close, MA_LONG)
        ma20_slope_120, ma60_slope_120 = self.calculate_ma_slopes(klines_120min)
        
        min120_condition = (
            klines_120min.close.iloc[-1] < ma60_120.iloc[-1] and
            klines_120min.close.iloc[-1] < ma20_120.iloc[-1] and
                ma60_slope_120 < 0 and
            ma20_slope_120 < 0
        )
        
        if not min120_condition:
            return False
        
        # 3. 检查1分钟和当前周期的MA穿越条件
        klines_1min = self.klines[symbol]["1min"]
        ma20_1min = ma(klines_1min.close, MA_SHORT)
        ma60_1min = ma(klines_1min.close, MA_LONG)
        
        # 1分钟级别的穿越条件
        min1_cross_condition = (
            ma20_1min.iloc[-3] >= ma60_1min.iloc[-3] and
            ma20_1min.iloc[-2] <= ma60_1min.iloc[-2]
        )
        
        # 当前周期（10分钟或15分钟）的条件
        klines = self.klines[symbol][period]
        ma20 = ma(klines.close, MA_SHORT)
        ma60 = ma(klines.close, MA_LONG)
        
        # 获取过去5根K线的最高价和收盘价
        last_5_high = klines.high.iloc[-5:].max()
        last_5_close = klines.close.iloc[-5:]
        
        # 计算MA差值比例
        ma_diff_ratio = (ma60.iloc[-1] - ma20.iloc[-1]) / ma60.iloc[-1]
        
        current_period_condition = (
            last_5_high >= ma60.iloc[-1] and
            all(close <= ma60.iloc[-1] for close in last_5_close) and
            ma_diff_ratio >= 0.003  # 0.3%
        )
        
        return min1_cross_condition and current_period_condition
    
    def check_approx_long_signal(self, symbol, period):
        """
        检查近似多头信号（3级信号）
        :param symbol: 交易品种
        :param period: 时间周期（10min或15min）
        :return: bool
        """
        # 1. 检查日增仓
        klines_daily = self.klines[symbol]["daily"]
        current_oi = float(klines_daily.open_oi.iloc[-1])
        prev_oi = float(klines_daily.open_oi.iloc[-2])
        daily_oi_change = current_oi - prev_oi
        # if daily_oi_change <= 0:
        #     return False
            
        # 2. 检查120分钟条件
        klines_120min = self.klines[symbol]["120min"]
        ma20_120 = ma(klines_120min.close, MA_SHORT)
        macd_120 = MACD(klines_120min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        
        min120_condition = (
            # klines_120min.close.iloc[-2] > ma20_120.iloc[-2] and
            macd_120["bar"].iloc[-2] > 0
        )
        
        if not min120_condition:
            return False
            
        # 3. 检查日线KDJ条件
        kdj_daily = KDJ(klines_daily, KDJ_N, KDJ_M1, KDJ_M2)
        if not kdj_daily["k"].iloc[-2] > kdj_daily["d"].iloc[-2]:
            return False
            
        # 4. 检查当前周期（10分钟或15分钟）的条件
        klines = self.klines[symbol][period]
        ma20 = ma(klines.close, MA_SHORT)
        ma60 = ma(klines.close, MA_LONG)
        macd = MACD(klines, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        ma20_slope, ma60_slope = self.calculate_ma_slopes(klines)
        
        # MACD条件
        macd_condition = (
            macd["bar"].iloc[-3] < 0 and
            macd["bar"].iloc[-2] > 0 and
            macd["dea"].iloc[-2] > 0 and
            macd["diff"].iloc[-2] > 0
        )
        
        # 均线条件
        ma_condition = (
            klines.close.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]
        )
        
        # MA60斜率和MA20>MA60持续性条件
        ma_slope_condition = (
            ma60_slope > 0 and
            ma20_slope > 0 and
            all(ma20.iloc[-3:] > ma60.iloc[-3:])
        )
        
        if not (macd_condition and ma_condition and ma_slope_condition):
            return False
        
        # 5. 检查偏离度
        current_price = klines.close.iloc[-1]
        ma20_value = ma20.iloc[-1]
        deviation = self.calculate_price_deviation(current_price, ma20_value)
        if deviation >= MA20_DEVIATION_THRESHOLD:
            return False
        
        return True
    
    def check_standard_3060_signal(self, symbol, period):
        """
        检查30分钟/60分钟级别的标准信号
        采用快速失败策略：任何一个级别条件不满足就立即返回
        :param symbol: 交易品种
        :param period: 时间周期（30min或60min）
        :return: (bool, bool) - (多头信号, 空头信号)
        """
        # 1. 获取当前周期K线数据并计算指标
        klines = self.klines[symbol][period]
        ma20 = ma(klines.close, MA_SHORT)
        ma60 = ma(klines.close, MA_LONG)
        macd = MACD(klines, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        ma20_slope, ma60_slope = self.calculate_ma_slopes(klines)
        
        # 2. 首先检查当前周期条件（最快判断）
        # 多头当前周期条件
        current_long = (
            macd["bar"].iloc[-3] < 0 and
            macd["bar"].iloc[-2] > 0 and
            macd["dea"].iloc[-2] > 0 and
            macd["diff"].iloc[-2] > 0 and
            klines.close.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1] and
            ma60_slope > 0 and
            ma20_slope > 0
        )
        
        # 空头当前周期条件
        current_short = (
            macd["bar"].iloc[-3] > 0 and
            macd["bar"].iloc[-2] < 0 and
            macd["dea"].iloc[-2] < 0 and
            macd["diff"].iloc[-2] < 0 and
            klines.close.iloc[-1] < ma20.iloc[-1] < ma60.iloc[-1] and
            ma60_slope < 0 and
            ma20_slope < 0
        )
        
        # 如果当前周期条件都不满足，直接返回False
        if not (current_long or current_short):
            return False, False
        
        # 3. 检查价格偏离度
        # current_price = klines.close.iloc[-1]
        # ma60_value = ma60.iloc[-1]
        # deviation = self.calculate_price_deviation(current_price, ma60_value)
        # if deviation >= MA20_DEVIATION_THRESHOLD:
        #     return False, False
        
        # 4. 检查日增仓
        klines_daily = self.klines[symbol]["daily"]
        # daily_oi_change = klines_daily.open_oi.iloc[-1] - klines_daily.open_oi.iloc[-2]
        # if daily_oi_change <= 0:
        #     return False, False
        
        # 5. 如果是多头信号，检查多头其他周期条件
        if current_long:
            # 5.1 检查120分钟条件
            klines_120min = self.klines[symbol]["120min"]
            ma20_120 = ma(klines_120min.close, MA_SHORT)
            ma60_120 = ma(klines_120min.close, MA_LONG)
            macd_120 = MACD(klines_120min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            _, ma60_slope_120 = self.calculate_ma_slopes(klines_120min)
            
            if not (klines_120min.close.iloc[-1] > ma20_120.iloc[-1] and
                   klines_120min.close.iloc[-1] > ma60_120.iloc[-1] and
                   macd_120["bar"].iloc[-2] > 0 and
                   ma60_slope_120 > 0):
                return False, False
            
            # 5.2 检查日线条件
            ma20_daily = ma(klines_daily.close, MA_SHORT)
            ma60_daily = ma(klines_daily.close, MA_LONG)
            kdj_daily = KDJ(klines_daily, KDJ_N, KDJ_M1, KDJ_M2)
            
            if not (klines_daily.close.iloc[-1] > ma20_daily.iloc[-1] and
                   klines_daily.close.iloc[-1] > ma60_daily.iloc[-1] and
                   kdj_daily["k"].iloc[-2] > kdj_daily["d"].iloc[-2]):
                return False, False
            
            return True, False
        
        # 6. 如果是空头信号，检查空头其他周期条件
        if current_short:
            # 6.1 检查120分钟条件
            klines_120min = self.klines[symbol]["120min"]
            ma20_120 = ma(klines_120min.close, MA_SHORT)
            ma60_120 = ma(klines_120min.close, MA_LONG)
            macd_120 = MACD(klines_120min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            _, ma60_slope_120 = self.calculate_ma_slopes(klines_120min)
            
            if not (klines_120min.close.iloc[-1] < ma20_120.iloc[-1] and
                   klines_120min.close.iloc[-1] < ma60_120.iloc[-1] and
                   macd_120["bar"].iloc[-2] < 0 and
                   ma60_slope_120 < 0):
                return False, False
            
            # 6.2 检查日线条件
            ma20_daily = ma(klines_daily.close, MA_SHORT)
            ma60_daily = ma(klines_daily.close, MA_LONG)
            kdj_daily = KDJ(klines_daily, KDJ_N, KDJ_M1, KDJ_M2)
            
            if not (klines_daily.close.iloc[-1] < ma20_daily.iloc[-1] and
                   klines_daily.close.iloc[-1] < ma60_daily.iloc[-1] and
                   kdj_daily["k"].iloc[-2] < kdj_daily["d"].iloc[-2]):
                return False, False
            
            return False, True
        
        return False, False
    
    def is_closing_check_time(self):
        """
        检查是否是收盘前检查时间（14:57或22:57）
        :return: bool
        """
        # 获取当前时间
        current_time = int(time.time() * 1e9)  # 转换为纳秒时间戳
        dt = datetime.fromtimestamp(current_time / 1e9)
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = dt.astimezone(beijing_tz)
        
        # 判断是否是14:57或22:57
        is_check_time = (
            (beijing_time.hour == 14 and beijing_time.minute == 57) or
            (beijing_time.hour == 22 and beijing_time.minute == 57)
        )
        
        return is_check_time

    def check_120min_signal(self, symbol):
        """
        检查120分钟级别的信号
        :param symbol: 交易品种
        :return: (bool, bool) - (多头信号, 空头信号)
        """
        # 判断是否在特定时间段
        is_special_time = self.is_closing_check_time()
        
        # 获取120分钟K线数据
        klines_120min = self.klines[symbol]["120min"]
        
        # 计算指标
        ma20_120 = ma(klines_120min.close, MA_SHORT)
        ma60_120 = ma(klines_120min.close, MA_LONG)
        macd_120 = MACD(klines_120min, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        ma20_slope_120, ma60_slope_120 = self.calculate_ma_slopes(klines_120min)
        
        # 根据时间段选择不同的MACD判断条件
        if is_special_time:
            # 特殊时间段的MACD条件
            long_macd_condition = (
                macd_120["bar"].iloc[-2] < 0 and
                macd_120["bar"].iloc[-1] > 0 and
                macd_120["dea"].iloc[-1] > 0 and
                macd_120["diff"].iloc[-1] > 0
            )
            
            short_macd_condition = (
                macd_120["bar"].iloc[-2] > 0 and
                macd_120["bar"].iloc[-1] < 0 and
                macd_120["dea"].iloc[-1] < 0 and
                macd_120["diff"].iloc[-1] < 0
            )
        else:
            # 普通时间段的MACD条件
            long_macd_condition = (
                macd_120["bar"].iloc[-3] < 0 and
                macd_120["bar"].iloc[-2] > 0 and
                macd_120["dea"].iloc[-2] > 0 and
                macd_120["diff"].iloc[-2] > 0
            )
            
            short_macd_condition = (
                macd_120["bar"].iloc[-3] > 0 and
                macd_120["bar"].iloc[-2] < 0 and
                macd_120["dea"].iloc[-2] < 0 and
                macd_120["diff"].iloc[-2] < 0
            )
        
        # 多头信号条件
        long_signal = (
            long_macd_condition and
            klines_120min.close.iloc[-1] > ma20_120.iloc[-1] > ma60_120.iloc[-1] and
            (ma60_slope_120 > 0 or ma20_slope_120 > 0)
        )
        
        # 空头信号条件
        short_signal = (
            short_macd_condition and
            klines_120min.close.iloc[-1] < ma20_120.iloc[-1] < ma60_120.iloc[-1] and
            (ma60_slope_120 < 0 or ma20_slope_120 < 0)
        )
        
        return long_signal, short_signal
    
    def check_daily_signal(self, symbol):
        """
        检查日线级别的信号
        :param symbol: 交易品种
        :return: (bool, bool) - (多头信号, 空头信号)
        """
        # 如果时间不在收盘时间段，直接返回False
        if not self.is_closing_check_time():
            return False, False
        
        # 获取日线数据
        klines_daily = self.klines[symbol]["daily"]
        
        # 计算指标
        ma20_daily = ma(klines_daily.close, MA_SHORT)
        ma60_daily = ma(klines_daily.close, MA_LONG)
        macd_daily = MACD(klines_daily, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        ma20_slope, ma60_slope = self.calculate_ma_slopes(klines_daily)
        
        # 多头信号条件
        long_signal = (
            # MACD条件
            macd_daily["bar"].iloc[-2] < 0 and
            macd_daily["bar"].iloc[-1] > 0 and
            macd_daily["dea"].iloc[-1] > 0 and
            macd_daily["diff"].iloc[-1] > 0 and
            
            # 均线条件
            klines_daily.close.iloc[-1] > ma20_daily.iloc[-1] > ma60_daily.iloc[-1] and
            
            # 均线斜率条件（任一满足即可）
            (ma20_slope > 0 or ma60_slope > 0)
        )
        
        # 空头信号条件
        short_signal = (
            # MACD条件
            macd_daily["bar"].iloc[-2] > 0 and
            macd_daily["bar"].iloc[-1] < 0 and
            macd_daily["dea"].iloc[-1] < 0 and
            macd_daily["diff"].iloc[-1] < 0 and
            
            # 均线条件
            klines_daily.close.iloc[-1] < ma20_daily.iloc[-1] < ma60_daily.iloc[-1] and
            
            # 均线斜率条件（任一满足即可）
            (ma20_slope < 0 or ma60_slope < 0)
        )
        
        return long_signal, short_signal
    
    def check_weekly_signal(self, symbol):
        """
        检查周级别的标准信号
        :param symbol: 交易品种
        :return: (bool, bool) - (多头信号, 空头信号)
        """
        # 如果时间不在收盘时间段，直接返回False
        if not self.is_closing_check_time():
            return False, False
            
        # 获取周线数据
        klines_weekly = self.klines[symbol]["weekly"]
        
        # 计算指标
        ma20 = ma(klines_weekly.close, MA_SHORT)
        macd = MACD(klines_weekly, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        kdj = KDJ(klines_weekly, KDJ_N, KDJ_M1, KDJ_M2)
        
        # 计算MA20斜率（回取2个周期）
        ma20_slope = self.calculate_slope(ma20, 2)
        
        # 多头信号条件
        macd_long_condition = (
            macd["bar"].iloc[-3] < 0 and
            macd["bar"].iloc[-2] > 0 and
            macd["dea"].iloc[-2] > 0 and
            macd["diff"].iloc[-2] > 0
        )
        
        kdj_long_condition = (
            macd["bar"].iloc[-2] > 0 and
            kdj["k"].iloc[-2] < kdj["d"].iloc[-2] and
            kdj["k"].iloc[-1] > kdj["d"].iloc[-1]
        )
        
        long_signal = (
            (macd_long_condition or kdj_long_condition) and
            klines_weekly.close.iloc[-1] > ma20.iloc[-1] and
            ma20_slope > 0
        )
        
        # 空头信号条件
        macd_short_condition = (
            macd["bar"].iloc[-3] > 0 and
            macd["bar"].iloc[-2] < 0 and
            macd["dea"].iloc[-2] < 0 and
            macd["diff"].iloc[-2] < 0
        )
        
        kdj_short_condition = (
            macd["bar"].iloc[-2] < 0 and
            kdj["k"].iloc[-2] > kdj["d"].iloc[-2] and
            kdj["k"].iloc[-1] < kdj["d"].iloc[-1]
        )
        
        short_signal = (
            (macd_short_condition or kdj_short_condition) and
            klines_weekly.close.iloc[-1] < ma20.iloc[-1] and
            ma20_slope < 0
        )
        
        return long_signal, short_signal
    
    def check_1min_signal(self, symbol):
        """
        检查1分钟级别的信号
        当 -2根k线收盘价<Ma20<ma60 且 -3根k线ma20>ma60 时 或 
        -2根k线收盘价>Ma20>ma60 且 -3根k线ma20<ma60 时都满足
        :param symbol: 交易品种
        :return: (bool, dict, str) - (是否有信号, 信号详情, 信号方向)
        """
        # 获取1分钟K线数据
        klines = self.klines[symbol]["1min"]
        
        # 计算MA20和MA60
        ma20 = ma(klines.close, MA_SHORT)
        ma60 = ma(klines.close, MA_LONG)
        
        # 获取具体数值用于日志记录
        close_price_2 = klines.close.iloc[-2]
        ma20_value_2 = ma20.iloc[-2]
        ma60_value_2 = ma60.iloc[-2]
        ma20_value_3 = ma20.iloc[-3]
        ma60_value_3 = ma60.iloc[-3]
        
        # 检查两种条件
        condition_down = (close_price_2 < ma20_value_2 < ma60_value_2) and (ma20_value_3 > ma60_value_3)
        condition_up = (close_price_2 > ma20_value_2 > ma60_value_2) and (ma20_value_3 < ma60_value_3)
        
        # 构建详细信息
        details = {
            "级别": "1分钟",
            "K线收盘价(-2)": f"{close_price_2:.2f}",
            "MA20(-2)": f"{ma20_value_2:.2f}",
            "MA60(-2)": f"{ma60_value_2:.2f}",
            "MA20(-3)": f"{ma20_value_3:.2f}",
            "MA60(-3)": f"{ma60_value_3:.2f}",
            "下穿条件": f"收盘价({close_price_2:.2f}) < MA20({ma20_value_2:.2f}) < MA60({ma60_value_2:.2f}) 且 MA20(-3)({ma20_value_3:.2f}) > MA60(-3)({ma60_value_3:.2f}): {condition_down}",
            "上穿条件": f"收盘价({close_price_2:.2f}) > MA20({ma20_value_2:.2f}) > MA60({ma60_value_2:.2f}) 且 MA20(-3)({ma20_value_3:.2f}) < MA60(-3)({ma60_value_3:.2f}): {condition_up}"
        }
        
        # 记录详细日志
        if condition_down or condition_up:
            log_msg = f"\n{symbol} 1分钟信号触发详情:"
            for key, value in details.items():
                log_msg += f"\n{key}: {value}"
            logger.info(log_msg)
        
        # 返回信号状态、详情和方向
        if condition_down:
            return True, details, "空头"
        elif condition_up:
            return True, details, "多头"
        else:
            return False, details, ""
    
    def _create_signal_info(self, symbol, period, direction, signal_type=None, current_price=None, ma60_value=None, timestamp=None):
        """
        创建信号信息
        :param symbol: 交易品种
        :param period: 时间周期
        :param direction: 交易方向
        :param signal_type: 信号类型（可选）
        :param current_price: 当前价格（如果未提供则自动获取）
        :param ma60_value: MA60值（如果未提供则自动计算）
        :param timestamp: 时间戳（如果未提供则使用当前K线时间戳）
        :return: dict - 信号信息字典
        """
        if current_price is None:
            current_price = float(self.quotes[symbol].last_price)
        
        # 计算MA20值
        ma20_value = float(ma(self.klines[symbol][period].close, MA_SHORT).iloc[-1])
        
        if timestamp is None:
            timestamp = self.klines[symbol][period].datetime.iloc[-1]
        
        # 计算日增仓比例
        daily_oi_current = float(self.klines[symbol]["daily"].open_oi.iloc[-1])
        daily_oi_prev = float(self.klines[symbol]["daily"].open_oi.iloc[-2])
        oi_ratio = (daily_oi_current - daily_oi_prev) / daily_oi_prev * 100 if daily_oi_prev != 0 else 0
        
        # 计算120分钟级别的价格与MA20偏离度
        klines_120min = self.klines[symbol]["120min"]
        ma20_120min = float(ma(klines_120min.close, MA_SHORT).iloc[-1])
        price_120min = float(klines_120min.close.iloc[-1])
        deviation_120min = self.calculate_price_deviation(price_120min, ma20_120min) * 100
        
        signal_info = {
            'timestamp': timestamp,
            'period': period,
            'info': {
                'symbol': symbol,
                'direction': direction,
                'deviation': self.calculate_price_deviation(current_price, ma20_value) * 100,
                'deviation_120min': deviation_120min,  # 添加120分钟偏离度
                'oi_ratio': oi_ratio
            }
        }
        
        if signal_type:
            signal_info['info']['signal_type'] = signal_type
            
        return signal_info

    def print_signals_to_log(self, signals, signal_type="常规"):
        """
        将信号按照统一格式打印到日志
        :param signals: 信号列表
        :param signal_type: 信号类型（常规/收盘）
        """
        if not signals:
            return
            
        # 合约代码映射
        exchange_symbol_map = {
            'SHFE.cu': '沪铜',
            'DCE.m': '豆粕',
            'CZCE.FG': '玻璃',
            'SHFE.ag': '沪银',
            'DCE.p': '棕榈',
            'SHFE.au': '沪金',
            'SHFE.sn': '沪锡',
            'CZCE.OI': '菜油',
            'SHFE.al': '铝',
            'CZCE.CF': '棉花',
            'DCE.eb': '苯乙烯',
            'CZCE.SH': '烧碱',
            'CZCE.AP': '苹果',
            'DCE.v': 'PVC',
            'SHFE.ru': '橡胶',
            'DCE.lh': '生猪',
            'GFEX.ps': '磷矿石'
        }
        
        # 按周期分组信号
        period_groups = defaultdict(list)
        for signal in signals:
            period_groups[signal['period']].append(signal['info'])
        
        # 按周期顺序处理信号
        period_order = ['10min', '15min', '30min', '60min', '120min', 'daily', 'weekly']
        for period in period_order:
            if period in period_groups:
                # 添加周期标题
                log_message = [f"【{period}】"]
                
                # 按信号类型优先级排序
                signal_priority = {"标准": 1, "左侧": 2, "近似": 3}
                period_signals = sorted(period_groups[period], 
                    key=lambda x: (x['symbol'], signal_priority.get(x.get('signal_type', ''), 999)))
                
                # 格式化每个信号
                for signal in period_signals:
                    # 转换合约代码
                    symbol = signal['symbol']
                    for exchange_code, display_name in exchange_symbol_map.items():
                        if symbol.startswith(exchange_code):
                            contract_num = symbol.split('.')[-1]
                            symbol_display = f"{display_name}{contract_num}"
                            break
                    else:
                        symbol_display = symbol
                    
                    # 方向显示
                    direction_display = "多" if signal['direction'] == "多头" else "空"
                    
                    # 信号类型显示
                    signal_type_display = signal.get('signal_type', '')
                    
                    # 构建基本信号行
                    signal_line = (
                        f"{symbol_display} {direction_display} {signal_type_display} "
                        f"偏离 {signal['deviation']:.2f}% "
                    )
                    
                    # 只在10分钟和15分钟级别显示120分钟偏离度
                    if period in ['10min', '15min']:
                        deviation_120min = signal['deviation_120min']
                        deviation_120min_display = f"{deviation_120min:.2f}%⚠️" if abs(deviation_120min) > 2 else f"{deviation_120min:.2f}%"
                        # 如果超过阈值，在日志中用不同的格式显示
                        if abs(deviation_120min) > 2:
                            deviation_120min_display = f"*{deviation_120min:.2f}%⚠️*"
                        signal_line += f"120偏离 {deviation_120min_display} "
                    # 添加日增仓信息
                    signal_line += f"日增仓 {signal['oi_ratio']:.1f}%"
                    
                    # 添加到日志消息和飞书消息
                    log_message.append(signal_line)
                
                # 打印日志
                logger.info("\n".join(log_message))
                
                # 添加飞书发送提示
                signal_count = len(period_signals)
                logger.info(f"成功发送{signal_count}个信号到飞书")
    
    def print_backtest_statistics(self):
        """打印回测统计信息"""
        if not self.backtest_signals:
            logger.info("回测期间没有产生任何信号")
            return
        
        logger.info("\n========== 回测信号统计 ==========")
        
        # 1. 总信号数量统计
        total_signals = len(self.backtest_signals)
        logger.info(f"总信号数量: {total_signals}")
        
        # 2. 综合统计（周期-类型-方向）
        period_type_direction_stats = defaultdict(lambda: defaultdict(lambda: {"多头": 0, "空头": 0}))
        for signal in self.backtest_signals:
            period = signal['period']
            signal_type = signal['info'].get('signal_type', '常规')
            direction = signal['info']['direction']
            period_type_direction_stats[period][signal_type][direction] += 1
        
        logger.info("\n综合统计（周期-类型-方向）:")
        # 按周期顺序处理
        for period in ['10min', '15min', '30min', '60min', '120min', 'daily', 'weekly']:
            if period in period_type_direction_stats:
                logger.info(f"\n【{period}】")
                period_total = sum(sum(counts.values()) for counts in period_type_direction_stats[period].values())
                period_percentage = (period_total / total_signals) * 100
                logger.info(f"合计: {period_total} 个 ({period_percentage:.2f}%)")
                
                # 按信号类型优先级排序（标准 > 左侧 > 近似 > 常规）
                type_order = {"标准": 1, "左侧": 2, "近似": 3, "常规": 4}
                sorted_types = sorted(period_type_direction_stats[period].items(), 
                                   key=lambda x: type_order.get(x[0], 999))
                
                for signal_type, directions in sorted_types:
                    type_total = sum(directions.values())
                    type_percentage = (type_total / total_signals) * 100
                    logger.info(f"  {signal_type}: {type_total} 个 ({type_percentage:.2f}%)")
                    
                    for direction, count in directions.items():
                        if count > 0:  # 只显示有信号的方向
                            direction_percentage = (count / total_signals) * 100
                            type_direction_percentage = (count / type_total) * 100 if type_total > 0 else 0
                            logger.info(f"    {direction}: {count} 个 "
                                      f"(占总数: {direction_percentage:.2f}%, "
                                      f"占类型: {type_direction_percentage:.2f}%)")
        
        # 3. 按品种统计
        symbol_direction_stats = defaultdict(lambda: {"多头": 0, "空头": 0})
        for signal in self.backtest_signals:
            symbol = signal['info']['symbol']
            direction = signal['info']['direction']
            symbol_direction_stats[symbol][direction] += 1
        
        logger.info("\n品种方向统计:")
        for symbol, directions in sorted(symbol_direction_stats.items(), 
                                      key=lambda x: sum(x[1].values()), reverse=True):
            symbol_total = sum(directions.values())
            symbol_percentage = (symbol_total / total_signals) * 100
            logger.info(f"\n{symbol}: {symbol_total} 个 ({symbol_percentage:.2f}%)")
            for direction, count in directions.items():
                if count > 0:  # 只显示有信号的方向
                    direction_percentage = (count / total_signals) * 100
                    symbol_direction_percentage = (count / symbol_total) * 100
                    logger.info(f"  {direction}: {count} 个 "
                              f"(占总数: {direction_percentage:.2f}%, "
                              f"占品种: {symbol_direction_percentage:.2f}%)")
        
        # 4. 按日期统计
        date_direction_stats = defaultdict(lambda: {"多头": 0, "空头": 0})
        for signal in self.backtest_signals:
            date = datetime.fromtimestamp(signal['timestamp'] / 1e9).strftime('%Y-%m-%d')
            direction = signal['info']['direction']
            date_direction_stats[date][direction] += 1
        
        logger.info("\n日期方向统计:")
        for date in sorted(date_direction_stats.keys()):
            directions = date_direction_stats[date]
            date_total = sum(directions.values())
            date_percentage = (date_total / total_signals) * 100
            logger.info(f"\n{date}: {date_total} 个 ({date_percentage:.2f}%)")
            for direction, count in directions.items():
                if count > 0:  # 只显示有信号的方向
                    direction_percentage = (count / total_signals) * 100
                    date_direction_percentage = (count / date_total) * 100
                    logger.info(f"  {direction}: {count} 个 "
                              f"(占总数: {direction_percentage:.2f}%, "
                              f"占当日: {date_direction_percentage:.2f}%)")
        
        # 5. 偏离度分布统计
        deviation_direction_ranges = {
            "0-0.5%": {"多头": 0, "空头": 0},
            "0.5-1%": {"多头": 0, "空头": 0},
            "1-1.5%": {"多头": 0, "空头": 0},
            "1.5-2%": {"多头": 0, "空头": 0},
            ">2%": {"多头": 0, "空头": 0}
        }
        
        for signal in self.backtest_signals:
            deviation = signal['info']['deviation']
            direction = signal['info']['direction']
            if deviation <= 0.5:
                deviation_direction_ranges["0-0.5%"][direction] += 1
            elif deviation <= 1:
                deviation_direction_ranges["0.5-1%"][direction] += 1
            elif deviation <= 1.5:
                deviation_direction_ranges["1-1.5%"][direction] += 1
            elif deviation <= 2:
                deviation_direction_ranges["1.5-2%"][direction] += 1
            else:
                deviation_direction_ranges[">2%"][direction] += 1
        
        logger.info("\n偏离度分布统计:")
        for range_name, directions in deviation_direction_ranges.items():
            range_total = sum(directions.values())
            if range_total > 0:  # 只显示有信号的区间
                range_percentage = (range_total / total_signals) * 100
                logger.info(f"\n{range_name}: {range_total} 个 ({range_percentage:.2f}%)")
                for direction, count in directions.items():
                    if count > 0:  # 只显示有信号的方向
                        direction_percentage = (count / total_signals) * 100
                        range_direction_percentage = (count / range_total) * 100
                        logger.info(f"  {direction}: {count} 个 "
                                  f"(占总数: {direction_percentage:.2f}%, "
                                  f"占区间: {range_direction_percentage:.2f}%)")
        
        logger.info("\n========== 统计结束 ==========\n")
    
    def run(self):
        """运行策略"""
        try:
            logger.info("策略开始运行")
            pending_signals = []  # 用于收集待发送的信号
            self.last_closing_check = None  # 初始化为None
            last_closing_signals_sent = False  # 添加标志来追踪是否已经发送过收盘信号
            
            while True:
                self.api.wait_update()
                
                # 获取当前时间
                current_time = int(time.time() * 1e9)
                dt = datetime.fromtimestamp(current_time / 1e9)
                beijing_tz = pytz.timezone('Asia/Shanghai')
                beijing_time = dt.astimezone(beijing_tz)
                
                # 检查是否是收盘前时间（14:57或22:57）
                is_closing_time = (
                    (beijing_time.hour == 14 and beijing_time.minute == 57) or
                    (beijing_time.hour == 22 and beijing_time.minute == 57)
                )
                
                # 生成当前收盘检查的唯一标识
                current_closing_check = f"{beijing_time.date()}_{beijing_time.hour}_{beijing_time.minute}"
                
                # 如果是收盘时间，且与上次检查时间不同，重置信号列表并进行收盘检查
                if is_closing_time and current_closing_check != self.last_closing_check:
                    pending_signals = []  # 清空之前的信号
                    self.last_closing_check = current_closing_check  # 更新检查时间
                    last_closing_signals_sent = False  # 重置发送标志
                    logger.info(f"开始收盘信号检查: {current_closing_check}")
                
                # 遍历所有交易品种
                for symbol in SYMBOLS:
                    # 检查10分钟和15分钟K线
                    for period in ["10min", "15min"]:
                        if self.api.is_changing(self.klines[symbol][period].iloc[-1], "datetime"):
                            current_price = float(self.quotes[symbol].last_price)
                            ma60_value = float(ma(self.klines[symbol][period].close, 60).iloc[-1])
                            # 检查标准信号
                            long_signal, short_signal = self.check_standard_signal(symbol, period)
                            if long_signal:
                                self.log_signal(symbol, "多头", "标准", period, current_price, ma60_value)
                                pending_signals.append(
                                    self._create_signal_info(symbol, period, "多头", "标准", current_price, ma60_value)
                                )
                            # if short_signal:
                            #     self.log_signal(symbol, "空头", "标准", period, current_price, ma60_value)
                            #     pending_signals.append(
                            #         self._create_signal_info(symbol, period, "空头", "标准", current_price, ma60_value)
                            #     )
                            
                            # # 检查左侧空头信号
                            # if self.check_left_side_short_signal(symbol, period):
                            #     self.log_signal(symbol, "空头", "左侧", period, current_price, ma60_value)
                            #     pending_signals.append(
                            #         self._create_signal_info(symbol, period, "空头", "左侧", current_price, ma60_value)
                            #     )
                            
                            # 只有在没有标准多头信号时才检查近似多头信号
                            if not long_signal and self.check_approx_long_signal(symbol, period):
                                self.log_signal(symbol, "多头", "近似", period, current_price, ma60_value)
                                pending_signals.append(
                                    self._create_signal_info(symbol, period, "多头", "近似", current_price, ma60_value)
                                )
                    
                    # # 检查30分钟和60分钟K线
                    # for period in ["30min", "60min"]:
                    #     if self.api.is_changing(self.klines[symbol][period].iloc[-1], "datetime"):
                    #         current_price = float(self.quotes[symbol].last_price)
                    #         ma60_value = float(ma(self.klines[symbol][period].close, 60).iloc[-1])
                    #         # 检查标准信号
                    #         long_signal, short_signal = self.check_standard_3060_signal(symbol, period)
                    #         if long_signal:
                    #             self.log_signal(symbol, "多头", "标准", period, current_price, ma60_value)
                    #             pending_signals.append(
                    #                 self._create_signal_info(symbol, period, "多头", "标准", current_price, ma60_value)
                    #             )
                    #         if short_signal:
                    #             self.log_signal(symbol, "空头", "标准", period, current_price, ma60_value)
                    #             pending_signals.append(
                    #                 self._create_signal_info(symbol, period, "空头", "标准", current_price, ma60_value)
                    #             )
                    
                    # # 检查120分钟K线更新或收盘前检查
                    # if self.api.is_changing(self.klines[symbol]["120min"].iloc[-1], "datetime") or (is_closing_time and not last_closing_signals_sent):
                    #     current_price = float(self.quotes[symbol].last_price)
                    #     ma60_value = float(ma(self.klines[symbol]["120min"].close, 60).iloc[-1])
                        
                    #     # 检查120分钟信号
                    #     long_signal, short_signal = self.check_120min_signal(symbol)
                    #     if long_signal:
                    #         self.log_signal(symbol, "多头", None, "120min", current_price, ma60_value)
                    #         pending_signals.append(
                    #             self._create_signal_info(symbol, "120min", "多头", None, current_price, ma60_value)
                    #         )
                    #     if short_signal:
                    #         self.log_signal(symbol, "空头", None, "120min", current_price, ma60_value)
                    #         pending_signals.append(
                    #             self._create_signal_info(symbol, "120min", "空头", None, current_price, ma60_value)
                    #         )
                    
                    # # 检查日线和周线信号（仅在收盘时检查，且仅检查一次）
                    # if is_closing_time and not last_closing_signals_sent:
                    #     # 检查日线信号
                    #     current_price = float(self.quotes[symbol].last_price)
                    #     ma60_value = float(ma(self.klines[symbol]["daily"].close, 60).iloc[-1])
                    #     long_signal, short_signal = self.check_daily_signal(symbol)
                    #     if long_signal:
                    #         self.log_signal(symbol, "多头", None, "daily", current_price, ma60_value)
                    #         pending_signals.append(
                    #             self._create_signal_info(symbol, "daily", "多头", None, current_price, ma60_value)
                    #         )
                    #     if short_signal:
                    #         self.log_signal(symbol, "空头", None, "daily", current_price, ma60_value)
                    #         pending_signals.append(
                    #             self._create_signal_info(symbol, "daily", "空头", None, current_price, ma60_value)
                    #         )
                        
                    #     # 检查周线信号
                    #     ma60_value = float(ma(self.klines[symbol]["weekly"].close, 60).iloc[-1])
                    #     long_signal, short_signal = self.check_weekly_signal(symbol)
                    #     if long_signal:
                    #         self.log_signal(symbol, "多头", None, "weekly", current_price, ma60_value)
                    #         pending_signals.append(
                    #             self._create_signal_info(symbol, "weekly", "多头", None, current_price, ma60_value)
                    #         )
                    #     if short_signal:
                    #         self.log_signal(symbol, "空头", None, "weekly", current_price, ma60_value)
                    #         pending_signals.append(
                    #             self._create_signal_info(symbol, "weekly", "空头", None, current_price, ma60_value)
                    #         )
                
                # 在检查完所有品种后，如果有待发送的信号，则统一发送
                if pending_signals:
                    # 如果是回测模式，保存信号用于统计
                    if BACKTEST_CONFIG["enabled"]:
                        self.backtest_signals.extend(pending_signals)
                    
                    # 打印常规信号到日志
                    self.print_signals_to_log(pending_signals, "常规")
                    
                    # 获取当前时间作为发送时间戳
                    send_timestamp = int(time.time() * 1e9) if not BACKTEST_CONFIG["enabled"] else pending_signals[0]['timestamp']
                    
                    # 将所有信号添加到飞书机器人并发送
                    for signal in pending_signals:
                        self.feishu_bot.add_signal(send_timestamp, signal['period'], signal['info'])
                    self.feishu_bot._send_signals(send_timestamp)
                    
                    # 清空待发送信号列表
                    pending_signals = []
                    
                    # 如果是收盘时间，标记已发送收盘信号
                    if is_closing_time:
                        last_closing_signals_sent = True
                        logger.info(f"收盘信号检查完成并发送: {current_closing_check}")
            
        except BacktestFinished:
            # 如果是回测模式，打印统计信息
            if BACKTEST_CONFIG["enabled"]:
                self.print_backtest_statistics()
            
            # 打印当前回测信息
            try:
                current_dt = self.api.get_kline_serial(SYMBOLS[0], 60).datetime.iloc[-1]
                current_time = datetime.fromtimestamp(current_dt / 1e9)
                logger.info(f"回测完成，结束时间: {current_time}")
            except:
                logger.info("回测完成")

if __name__ == "__main__":
    strategy = MACDStrategy()
    strategy.run()