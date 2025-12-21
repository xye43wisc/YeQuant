import AmazingData as ad
import pandas as pd
import sqlite3
import os
import json
from datetime import datetime

# --- 1. 配置信息 (请确保 YeQuantVenv 环境已安装 tgw 和 AmazingData) ---
try:
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    USER = config.get("USER")
    PWD  = config.get("PWD")
    IP   = config.get("IP")
    PORT = config.get("PORT")
except Exception as e:
    print(f"Error loading config.json: {e}")
    print("Please ensure config.json exists and contains USER, PWD, IP, PORT.")
    exit(1)

# --- 配置 ---
DB_FILE = "A_Share_Base_Data.db"
FEATHER_DIR = "feather_cache"  # 存储复权计算结果的文件夹
START_DATE = 20250101
END_DATE = int(datetime.now().strftime("%Y%m%d"))
LOCAL_CACHE = os.path.join(os.getcwd(), "AmazingData_cache//")

# 确保 Feather 目录存在
if not os.path.exists(FEATHER_DIR):
    os.makedirs(FEATHER_DIR)

def init_base_database():
    """初始化基础数据库：仅保留原始数据和因子"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # 1. 原始不复权表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_klines_raw (
            code TEXT, kline_time TEXT, open REAL, high REAL, low REAL, 
            close REAL, volume INTEGER, amount REAL,
            PRIMARY KEY (code, kline_time)
        )
    """)
    # 2. 复权因子表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS adjustment_factors (
            code TEXT, trade_date TEXT, factor REAL,
            PRIMARY KEY (code, trade_date)
        )
    """)
    conn.commit()
    return conn

def process_and_save_feather(code, df_k, df_f, df_s, feather_path):
    """
    构建理想的 Feather 列结构
    df_k: 原始K线, df_f: 复权因子, df_s: 历史证券状态 (ST/涨跌停)
    """
    # 1. 统一日期格式并合并数据
    df_k['date'] = pd.to_datetime(df_k['kline_time']).dt.strftime('%Y-%m-%d')
    
    # 准备因子数据 (get_backward_factor 返回 index 为日期)
    df_f_single = df_f[[code]].rename(columns={code: 'adj_factor'})
    df_f_single.index = pd.to_datetime(df_f_single.index).strftime('%Y-%m-%d')
    
    # 准备状态数据 (get_history_stock_status 返回包含 TRADE_DATE 字段的 DF)
    df_s_single = df_s[df_s['MARKET_CODE'] == code].copy()
    df_s_single['TRADE_DATE'] = pd.to_datetime(df_s_single['TRADE_DATE']).dt.strftime('%Y-%m-%d')

    # 合并三方数据
    df = pd.merge(df_k, df_f_single, left_on='date', right_index=True, how='left')
    df = pd.merge(df, df_s_single, left_on='date', right_on='TRADE_DATE', how='left')
    
    # 填充因子与状态默认值
    df['adj_factor'] = df['adj_factor'].ffill().fillna(1.0)
    df['IS_ST_SEC'] = df['IS_ST_SEC'].fillna('0')
    df['IS_SUSP_SEC'] = df['IS_SUSP_SEC'].fillna('0')

    # 2. 计算复权价格与成交量
    for col in ['open', 'high', 'low', 'close']:
        df[f'{col}_post'] = df[col] * df['adj_factor']
    
    df['close_raw'] = df['close']
    df['volume_post'] = df['volume'] / df['adj_factor'] # 保持 Price_post * Volume_post = Amount
    df['daily_return'] = df['close_post'].pct_change()

    # 3. 状态标识 [参照手册字段 4.2.10]
    # is_trading: 成交量大于0 且 状态不为停牌 
    df['is_trading'] = ((df['volume'] > 0) & (df['IS_SUSP_SEC'] != '1')).astype(int)
    
    # is_limit_up/down: 实际收盘价达到手册返回的涨跌停价 
    df['is_limit_up'] = (df['close'] >= df['HIGH_LIMITED']).astype(int)
    df['is_limit_down'] = (df['close'] <= df['LOW_LIMITED']).astype(int)
    
    # is_st: 手册返回的 IS_ST_SEC 字段 
    df['is_st'] = (df['IS_ST_SEC'] == '1').astype(int)

    # 4. 最终理想列结构
    final_cols = [
        'date', 
        'open_post', 'high_post', 'low_post', 'close_post', 
        'close_raw', 'adj_factor', 'daily_return', 
        'volume_post', 'amount', 'is_trading', 
        'is_limit_up', 'is_limit_down', 'is_st'
    ]
    
    df_final = df[final_cols].reset_index(drop=True)
    df_final.to_feather(feather_path)

def run_pipeline(is_test=True):
    # 登录
    print("正在尝试登录星耀数智平台...")
    ad.login(username=USER, password=PWD, host=IP, port=PORT)
    
    conn = init_base_database()
    # 优化写入速度
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    
    try:
        base_data = ad.BaseData()
        info_data = ad.InfoData() # 实例化 InfoData 类 [cite: 1611]
        all_codes = base_data.get_code_list(security_type='EXTRA_STOCK_A_SH_SZ')
        calendar = base_data.get_calendar(market='SH')
        market_data = ad.MarketData(calendar)

        target_codes = all_codes[:10] if is_test else all_codes
        batch_size = 50

        for i in range(0, len(target_codes), batch_size):
            batch = target_codes[i : i + batch_size]
            print(f"正在同步批次: {i+1}-{min(i+batch_size, len(target_codes))}")
            
            # 1. 批量获取三类核心数据
            kline_dict = market_data.query_kline(batch, START_DATE, END_DATE, ad.constant.Period.day.value) # [cite: 1808]
            df_factors = base_data.get_backward_factor(batch, LOCAL_CACHE, is_local=False) # [cite: 1503]
            # 获取历史证券信息 (含 ST 和 涨跌停价) [cite: 1597]
            df_status = info_data.get_history_stock_status(batch, begin_date=START_DATE, end_date=END_DATE, local_path=LOCAL_CACHE)

            # 2. 逐一处理
            for code in batch:
                if code not in kline_dict or df_factors is None or code not in df_factors.columns:
                    continue
                print(f"正在处理: {code}")
                # A. 存入 SQL (原始行情底账)
                df_raw = kline_dict[code].reset_index()
                df_raw['kline_time'] = pd.to_datetime(df_raw['kline_time']).dt.strftime('%Y-%m-%d')
                df_raw['code'] = code
                df_raw[['code', 'kline_time', 'open', 'high', 'low', 'close', 'volume', 'amount']].to_sql(
                    'daily_klines_raw', conn, if_exists='append', index=False
                )

                # B. 生成 Feather 缓存 (理想结构)
                feather_path = os.path.join(FEATHER_DIR, f"{code.replace('.', '_')}.feather")
                process_and_save_feather(code, kline_dict[code], df_factors, df_status, feather_path)
            
            print("批次处理完成。")

    finally:
        conn.close()
        ad.logout(USER)

if __name__ == "__main__":
    run_pipeline(is_test=True)
    os._exit(0)