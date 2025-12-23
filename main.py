import AmazingData as ad
import pandas as pd
import sqlite3
import os
import json
import logging
from datetime import datetime
import sys

# 1. 定义重定向类
class StreamToLogger:
    """
    将 sys.stdout 或 sys.stderr 的输出转发到 logging 模块。
    """
    def __init__(self, logger, log_level):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        # 逐行处理输出，避免日志格式错乱
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

# 2. 修改 setup_logging 函数
def setup_logging():
    logger = logging.getLogger("YeQuant")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 重要：控制台 Handler 必须使用系统原始的 __stdout__
        # 否则会陷入“stdout -> logger -> stdout”的死循环
        ch = logging.StreamHandler(sys.__stdout__) 
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        fh = logging.FileHandler("YeQuant_running.log", encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)
    
    return logger

# 3. 在程序入口处应用重定向
logger = setup_logging()

# 将所有的 print (stdout) 重定向到 INFO 级别日志
sys.stdout = StreamToLogger(logger, logging.INFO)
# 将所有的报错 (stderr) 重定向到 ERROR 级别日志
sys.stderr = StreamToLogger(logger, logging.ERROR)

# --- 1. 配置信息加载 ---
try:
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    USER = config.get("USER")
    PWD  = config.get("PWD")
    IP   = config.get("IP")
    PORT = config.get("PORT")
except Exception as e:
    logger.critical(f"无法加载 config.json: {e}. 请确保文件存在并包含 USER, PWD, IP, PORT。")
    exit(1)

# --- 配置 ---
DB_FILE = "A_Share_Base_Data.db"
FEATHER_DIR = "feather_cache"
START_DATE = 20130101
END_DATE = int(datetime.now().strftime("%Y%m%d"))
LOCAL_CACHE = os.path.join(os.getcwd(), "AmazingData_cache//")

if not os.path.exists(FEATHER_DIR):
    os.makedirs(FEATHER_DIR)


def get_all_latest_dates(conn):
    """
    获取数据库中每只股票对应的最新日期
    返回字典: { '000001.SZ': 20231027, ... }
    """
    try:
        cursor = conn.cursor()
        # 使用 GROUP BY 获取每只股票的最大日期
        query = "SELECT code, MAX(kline_time) FROM daily_klines_raw GROUP BY code"
        cursor.execute(query)
        rows = cursor.fetchall()
        # 将 '2023-10-27' 转换为整数 20231027 方便比较
        return {row[0]: int(row[1].replace('-', '')) for row in rows if row[1]}
    except Exception as e:
        logger.warning(f"获取各标的最新日期失败: {e}")
        return {}

def init_base_database():
    """初始化基础数据库"""
    logger.info("正在初始化 SQLite 数据库...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_klines_raw (
            code TEXT, kline_time TEXT, open REAL, high REAL, low REAL, 
            close REAL, volume INTEGER, amount REAL,
            PRIMARY KEY (code, kline_time)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS adjustment_factors (
            code TEXT, trade_date TEXT, factor REAL,
            PRIMARY KEY (code, trade_date)
        )
    """)
    conn.commit()
    return conn

def process_and_save_feather(code, df_new_k, df_f, df_s, feather_path):
    """构建理想的 Feather 列结构并保存"""
    logger.debug(f"开始处理标的 Feather 数据: {code}")

    """增量处理并合并 Feather 数据"""
    # 1. 如果已有缓存，先读取
    df_old = None
    if os.path.exists(feather_path):
        try:
            df_old = pd.read_feather(feather_path)
            last_date = df_old['date'].max()
            # 过滤掉新数据中已经存在于旧数据中的日期
            df_new_k = df_new_k[pd.to_datetime(df_new_k.index) > pd.to_datetime(last_date)]
        except Exception as e:
            logger.error(f"读取旧 Feather 失败 {code}: {e}")

    if df_new_k.empty:
        logger.info(f"[{code}] 没有新数据需要追加到 Feather")
        return df_old
    
    df_k = df_new_k.copy()
    df_k['date'] = pd.to_datetime(df_k['kline_time']).dt.strftime('%Y-%m-%d')
    logger.debug(f"[{code}] K线行数: {len(df_k)}, 日期范围: {df_k['date'].min()} ~ {df_k['date'].max()}")
    
    if df_f is not None and code in df_f.columns:
        df_f_single = df_f[[code]].rename(columns={code: 'adj_factor'})
        df_f_single.index = pd.to_datetime(df_f_single.index).strftime('%Y-%m-%d')
    else:
        logger.warning(f"[{code}] 未找到复权因子，使用默认值 1.0")
        df_f_single = pd.DataFrame(columns=['adj_factor'])

    if isinstance(df_s, dict):
        df_s_single = df_s.get(code)
    else:
        df_s_single = df_s[df_s['MARKET_CODE'] == code] if df_s is not None else None

    if df_s_single is not None and not df_s_single.empty:
        df_s_single = df_s_single.copy()
        df_s_single['date_fmt'] = pd.to_datetime(df_s_single['TRADE_DATE']).dt.strftime('%Y-%m-%d')
    else:
        logger.warning(f"[{code}] 未找到状态数据 (ST/停牌/涨跌停)")
        df_s_single = pd.DataFrame(columns=['date_fmt', 'HIGH_LIMITED', 'LOW_LIMITED', 'IS_ST_SEC', 'IS_SUSP_SEC'])

    df = pd.merge(df_k, df_f_single, left_on='date', right_index=True, how='left')
    df = pd.merge(df, df_s_single, left_on='date', right_on='date_fmt', how='left')
    
    df['adj_factor'] = df['adj_factor'].ffill().fillna(1.0)
    df['IS_ST_SEC'] = df['IS_ST_SEC'].fillna('0')
    df['IS_SUSP_SEC'] = df['IS_SUSP_SEC'].fillna('0')

    for col in ['open', 'high', 'low', 'close']:
        df[f'{col}_post'] = df[col] * df['adj_factor']
    
    df['close_raw'] = df['close']
    df['volume_post'] = df['volume'] / df['adj_factor']
    df['daily_return'] = df['close_post'].pct_change()
    df['is_trading'] = ((df['volume'] > 0) & (df['IS_SUSP_SEC'] != '1')).astype(int)
    
    if 'HIGH_LIMITED' in df.columns:
        df['is_limit_up'] = (df['close'] >= df['HIGH_LIMITED']).astype(int)
        df['is_limit_down'] = (df['close'] <= df['LOW_LIMITED']).astype(int)
    else:
        logger.error(f"[{code}] 缺失涨跌停字段")
        df['is_limit_up'] = 0
        df['is_limit_down'] = 0
    
    df['is_st'] = (df['IS_ST_SEC'] == '1').astype(int)

    final_cols = [
        'date', 'open_post', 'high_post', 'low_post', 'close_post', 
        'close_raw', 'adj_factor', 'daily_return', 
        'volume_post', 'amount', 'is_trading', 
        'is_limit_up', 'is_limit_down', 'is_st'
    ]
    
    df_final = df[[c for c in final_cols if c in df.columns]].reset_index(drop=True)

    # 3. 合并
    if df_old is not None:
        # 为了保证 daily_return 计算正确，建议在合并后再统一计算一次 return
        # 或者在处理 df_new_k 时，带上 df_old 的最后一行
        df_combined = pd.concat([df_old, df_final], ignore_index=True).drop_duplicates(subset=['date'])
        # 重新计算受前值影响的字段（如 daily_return）
        df_combined['daily_return'] = df_combined['close_post'].pct_change()
        df_combined.to_feather(feather_path)
        logger.info(f"成功保存 Feather 缓存: {feather_path} (行数: {len(df_combined)})")
        return df_combined
    else:
        df_final.to_feather(feather_path)
        logger.info(f"成功保存 Feather 缓存: {feather_path} (行数: {len(df_final)})")
        return df_final

def run_pipeline(is_test=True):
    logger.info("正在尝试登录星耀数智平台...")
    ad.login(username=USER, password=PWD, host=IP, port=PORT)
    conn = init_base_database()

    # 1. 预先获取全量进度字典
    latest_dates_dict = get_all_latest_dates(conn)
    
    try:
        base_data = ad.BaseData()
        info_data = ad.InfoData()
        
        all_codes = base_data.get_code_list(security_type='EXTRA_STOCK_A_SH_SZ')
        logger.info(f"获取全量沪深 A 股代码成功，共: {len(all_codes)} 只")

        calendar = base_data.get_calendar(market='SH') 
        market_data = ad.MarketData(calendar) 

        target_codes = all_codes[:60] if is_test else all_codes
        logger.info(f"运行模式: {'测试' if is_test else '全量'} | 计划处理数量: {len(target_codes)}")

        batch_size = 50
        for i in range(0, len(target_codes), batch_size):
            batch = target_codes[i : i + batch_size]
            logger.info(f"=== 开始处理批次: {i+1} 至 {min(i+batch_size, len(target_codes))} ===")

            # 2. 找到当前批次中“最老”的日期作为本次请求的 START_DATE
            # 如果某只股票没数据，则使用全局 START_DATE (20130101)
            batch_min_date = min([latest_dates_dict.get(code, START_DATE) for code in batch])

            logger.info(f"批次起点: {batch_min_date}，请求数据中...")
            kline_dict = market_data.query_kline(batch, batch_min_date, END_DATE, ad.constant.Period.day.value)
        
            df_factors = base_data.get_backward_factor(batch, local_path=LOCAL_CACHE, is_local=False)
            df_status = info_data.get_history_stock_status(batch, local_path=LOCAL_CACHE) 

            processed_count = 0
            for code in batch:
                logger.debug(f"正在校验标的: {code}")
                if kline_dict is None or code not in kline_dict:
                    logger.warning(f"跳过 {code}: 缺失 K 线数据")
                    continue
                try:
                    # 1. 过滤并写入 SQL
                    # 获取该股在库里的最后日期，转为字符串格式用于对比
                    last_date_val = latest_dates_dict.get(code, 0)
                    last_date_str = datetime.strptime(str(last_date_val), '%Y%m%d').strftime('%Y-%m-%d') if last_date_val > 0 else "1900-01-01"
                    
                    df_raw = kline_dict[code].reset_index()
                    df_raw['kline_time'] = pd.to_datetime(df_raw['kline_time']).dt.strftime('%Y-%m-%d')
                    
                    # 只保存比数据库里更新的数据
                    df_to_sql = df_raw[df_raw['kline_time'] > last_date_str].copy()
                    
                    if not df_to_sql.empty:
                        df_to_sql['code'] = code
                        df_to_sql[['code', 'kline_time', 'open', 'high', 'low', 'close', 'volume', 'amount']].to_sql(
                            'daily_klines_raw', conn, if_exists='append', index=False
                        )
                        
                        # 处理复权因子存入 SQL (可选)
                        if df_factors is not None and code in df_factors.columns:
                            df_f_sql = df_factors[[code]].copy().reset_index()
                            df_f_sql.columns = ['trade_date', 'factor']
                            df_f_sql['code'] = code
                            df_f_sql['trade_date'] = pd.to_datetime(df_f_sql['trade_date']).dt.strftime('%Y-%m-%d')
                            df_f_sql = df_f_sql[df_f_sql['trade_date'] > last_date_str].dropna()
                            df_f_sql[['code', 'trade_date', 'factor']].to_sql('adjustment_factors', conn, if_exists='append', index=False)

                    # 2. 增量更新 Feather 文件
                    feather_path = os.path.join(FEATHER_DIR, f"{code.replace('.', '_')}.feather")
                    process_and_save_feather(code, kline_dict[code], df_factors, df_status, feather_path)
                    
                    processed_count += 1
                except Exception as inner_e:
                    logger.error(f"处理标的 {code} 出错: {inner_e}")

            logger.info(f"批次处理结束，更新成功: {processed_count}/{len(batch)}")
    except Exception:
        logger.exception("流程被中断，捕获到严重错误:")
    finally:
        conn.close()
        try:
            ad.logout(USER)
        except:
            pass
        logger.info("进程已正常退出。")

if __name__ == "__main__":
    run_pipeline(is_test=False)
    os._exit(0)