import AmazingData as ad
import pandas as pd
import sqlite3
import os
import json
import logging
from datetime import datetime

# --- 配置日志输出 ---
def setup_logging():
    """配置日志系统的最佳实践：同时输出到控制台和文件"""
    logger = logging.getLogger("YeQuant")
    logger.setLevel(logging.DEBUG)  # 设置总级别为 DEBUG

    # 防止重复添加 Handler
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 1. 控制台 Handler (通常显示 INFO 及以上)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        # 2. 文件 Handler (记录 DEBUG 级别的详细日志)
        fh = logging.FileHandler("YeQuant_running.log", encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)
    
    return logger

logger = setup_logging()

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
START_DATE = 20250101
END_DATE = int(datetime.now().strftime("%Y%m%d"))
LOCAL_CACHE = os.path.join(os.getcwd(), "AmazingData_cache//")

if not os.path.exists(FEATHER_DIR):
    os.makedirs(FEATHER_DIR)

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

def process_and_save_feather(code, df_k, df_f, df_s, feather_path):
    """构建理想的 Feather 列结构并保存"""
    logger.debug(f"开始处理标的 Feather 数据: {code}")
    
    df_k = df_k.copy()
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
    df_final.to_feather(feather_path)
    logger.info(f"成功保存 Feather 缓存: {feather_path} (行数: {len(df_final)})")
    return df_final

def run_pipeline(is_test=True):
    logger.info("正在尝试登录星耀数智平台...")
    ad.login(username=USER, password=PWD, host=IP, port=PORT)
    conn = init_base_database()
    
    try:
        base_data = ad.BaseData()
        info_data = ad.InfoData()
        
        all_codes = base_data.get_code_list(security_type='EXTRA_STOCK_A_SH_SZ')
        logger.info(f"获取全量 A 股代码成功，共: {len(all_codes)} 只")

        calendar = base_data.get_calendar(market='SH') 
        market_data = ad.MarketData(calendar) 

        target_codes = all_codes[:10] if is_test else all_codes
        logger.info(f"运行模式: {'测试' if is_test else '全量'} | 计划处理数量: {len(target_codes)}")

        batch_size = 50
        for i in range(0, len(target_codes), batch_size):
            batch = target_codes[i : i + batch_size]
            logger.info(f"=== 开始处理批次: {i+1} 至 {min(i+batch_size, len(target_codes))} ===")
            
            logger.info("正在请求 K 线、复权因子及证券状态数据...")
            kline_dict = market_data.query_kline(batch, START_DATE, END_DATE, ad.constant.Period.day.value) 
            df_factors = base_data.get_backward_factor(batch, local_path=LOCAL_CACHE)
            df_status = info_data.get_history_stock_status(batch, local_path=LOCAL_CACHE) 

            processed_count = 0
            for code in batch:
                logger.debug(f"正在校验标的: {code}")
                
                if kline_dict is None or code not in kline_dict:
                    logger.warning(f"跳过 {code}: 缺失 K 线数据")
                    continue
                if df_factors is None or code not in df_factors.columns:
                    logger.warning(f"跳过 {code}: 复权因子缺失")
                    continue
                
                try:
                    # 归档至 SQL
                    # 1. 存入原始行情数据
                df_raw = kline_dict[code].reset_index()
                df_raw['kline_time'] = pd.to_datetime(df_raw['kline_time']).dt.strftime('%Y-%m-%d')
                df_raw['code'] = code
                df_raw[['code', 'kline_time', 'open', 'high', 'low', 'close', 'volume', 'amount']].to_sql(
                    'daily_klines_raw', conn, if_exists='append', index=False
                )
                
                # 2. 存入复权因子 (这里补上了！)
                if df_factors is not None and code in df_factors.columns:
                    # 从宽表中提取当前股票的因子
                    df_f_sql = df_factors[[code]].copy().reset_index()
                    df_f_sql.columns = ['trade_date', 'factor']
                    df_f_sql['code'] = code
                    df_f_sql['trade_date'] = pd.to_datetime(df_f_sql['trade_date']).dt.strftime('%Y-%m-%d')
                    
                    # 格式整理：code, trade_date, factor
                    df_f_sql = df_f_sql[['code', 'trade_date', 'factor']].dropna()
                    
                    try:
                        # 写入数据库，如果主键 (code, trade_date) 冲突则跳过
                        df_f_sql.to_sql('adjustment_factors', conn, if_exists='append', index=False)
                    except sqlite3.IntegrityError:
                        pass # 增量运行或重复下载时忽略冲突

                    # 生成 Feather
                    feather_path = os.path.join(FEATHER_DIR, f"{code.replace('.', '_')}.feather")
                    process_and_save_feather(code, kline_dict[code], df_factors, df_status, feather_path)
                    processed_count += 1
                    
                except Exception as inner_e:
                    logger.error(f"处理标的 {code} 时发生错误: {inner_e}")

            logger.info(f"批次处理结束，成功率: {processed_count}/{len(batch)}")

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
    run_pipeline(is_test=True)
    os._exit(0)