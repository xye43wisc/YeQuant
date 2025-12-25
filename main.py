import AmazingData as ad
import pandas as pd
import sqlite3
import os
import json
import logging
from datetime import datetime
import sys
import argparse

# --- 1. 日志与系统工具 ---
class StreamToLogger:
    def __init__(self, logger, log_level):
        self.logger = logger
        self.log_level = log_level

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

def setup_logging():
    logger = logging.getLogger("YeQuant")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch = logging.StreamHandler(sys.__stdout__) 
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        fh = logging.FileHandler("YeQuant_running.log", encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(ch)
        logger.addHandler(fh)
    return logger

logger = setup_logging()
sys.stdout = StreamToLogger(logger, logging.INFO)
sys.stderr = StreamToLogger(logger, logging.ERROR)

# --- 2. 数据库逻辑 ---
def init_base_database(db_file):
    """初始化基础数据库"""
    logger.info(f"正在初始化数据库: {db_file}")
    conn = sqlite3.connect(db_file)
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

def get_all_latest_dates(conn):
    try:
        cursor = conn.cursor()
        query = "SELECT code, MAX(kline_time) FROM daily_klines_raw GROUP BY code"
        cursor.execute(query)
        rows = cursor.fetchall()
        return {row[0]: int(row[1].replace('-', '')) for row in rows if row[1]}
    except Exception as e:
        logger.warning(f"获取各标的最新日期失败: {e}")
        return {}

# --- 3. 数据处理逻辑 ---
def process_and_save_feather(code, df_new_k, df_f, df_s, feather_path):
    """增量处理并保存 Feather 数据"""
    logger.debug(f"开始处理标的 Feather 数据: {code}")
    df_old = None
    if os.path.exists(feather_path):
        try:
            df_old = pd.read_feather(feather_path)
            last_date = df_old['date'].max()
            df_new_k = df_new_k[pd.to_datetime(df_new_k.index) >= pd.to_datetime(last_date)]
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
        df['is_limit_up'] = df['is_limit_down'] = 0
    
    df['is_st'] = (df['IS_ST_SEC'] == '1').astype(int)

    final_cols = [
        'date', 'open_post', 'high_post', 'low_post', 'close_post', 
        'close_raw', 'adj_factor', 'daily_return', 
        'volume_post', 'amount', 'is_trading', 
        'is_limit_up', 'is_limit_down', 'is_st'
    ]
    
    df_final = df[[c for c in final_cols if c in df.columns]].reset_index(drop=True)

    if df_old is not None:
        df_combined = pd.concat([df_old, df_final], ignore_index=True).drop_duplicates(subset=['date'], keep='last')
        df_combined['daily_return'] = df_combined['close_post'].pct_change()
        df_combined.to_feather(feather_path)
        logger.debug(f"成功保存 Feather 缓存: {feather_path} (行数: {len(df_combined)})")
        return df_combined
    else:
        df_final.to_feather(feather_path)
        logger.debug(f"成功保存 Feather 缓存: {feather_path} (行数: {len(df_final)})")
        return df_final

# --- 4. 主流程逻辑 ---
def run_pipeline(config):
    auth = config['AUTH']
    strat = config['STRATEGY']
    paths = config['PATH']
    data_cfg = config['DATA']

    logger.info(f"模式: {strat['MODE']} | 目标数据库: {paths['DB_FILE']}")
    
    ad.login(username=auth['USER'], password=auth['PWD'], host=auth['IP'], port=auth['PORT'])
    
    if not os.path.exists(paths['FEATHER_DIR']):
        os.makedirs(paths['FEATHER_DIR'])
    
    conn = init_base_database(paths['DB_FILE'])
    latest_dates_dict = get_all_latest_dates(conn)
    
    try:
        base_data = ad.BaseData()
        info_data = ad.InfoData()
        
        # 模式切换逻辑
        if strat['MODE'] == 'single':
            target_codes = [strat['SINGLE_CODE']]
        else:
            all_codes = base_data.get_code_list(security_type=data_cfg['SECURITY_TYPE'])
            target_codes = all_codes[:strat['TEST_COUNT']] if strat['MODE'] == 'test' else all_codes

        logger.info(f"准备处理标的数量: {len(target_codes)}")

        calendar = base_data.get_calendar(market='SH') 
        market_data = ad.MarketData(calendar) 
        end_date = int(datetime.now().strftime("%Y%m%d"))

        batch_size = strat.get('BATCH_SIZE', 50)
        for i in range(0, len(target_codes), batch_size):
            batch = target_codes[i : i + batch_size]
            logger.info(f"=== 开始处理批次: {i+1} 至 {min(i+batch_size, len(target_codes))} ===")
            batch_min_date = min([latest_dates_dict.get(code, data_cfg['START_DATE']) for code in batch]) # type: ignore
            logger.info(f"批次起点: {batch_min_date}，请求数据中...")
            kline_dict = market_data.query_kline(batch, batch_min_date, end_date, ad.constant.Period.day.value)
            df_factors = base_data.get_backward_factor(batch, local_path=paths['LOCAL_CACHE'], is_local=False)
            df_status = info_data.get_history_stock_status(batch, local_path=paths['LOCAL_CACHE']) 
            processed_count = 0
            for code in batch:
                logger.debug(f"正在校验标的: {code}")
                if kline_dict is None or code not in kline_dict:
                    logger.warning(f"跳过 {code}: 缺失 K 线数据")
                    continue
                try:
                    last_date_val = latest_dates_dict.get(code, 0)
                    last_date_str = datetime.strptime(str(last_date_val), '%Y%m%d').strftime('%Y-%m-%d') if last_date_val > 0 else "1900-01-01"
                    
                    df_raw = kline_dict[code].reset_index()
                    df_raw['kline_time'] = pd.to_datetime(df_raw['kline_time']).dt.strftime('%Y-%m-%d')
                    df_to_sql = df_raw[df_raw['kline_time'] >= last_date_str].copy()
                    
                    if not df_to_sql.empty:
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM daily_klines_raw WHERE code = ? AND kline_time = ?", (code, last_date_str))
                        cursor.execute("DELETE FROM adjustment_factors WHERE code = ? AND trade_date = ?", (code, last_date_str))
                        df_to_sql['code'] = code
                        df_to_sql[['code', 'kline_time', 'open', 'high', 'low', 'close', 'volume', 'amount']].to_sql(
                            'daily_klines_raw', conn, if_exists='append', index=False
                        )
                        
                        if df_factors is not None and code in df_factors.columns:
                            df_f_sql = df_factors[[code]].copy().reset_index()
                            df_f_sql.columns = ['trade_date', 'factor']
                            df_f_sql['code'] = code
                            df_f_sql['trade_date'] = pd.to_datetime(df_f_sql['trade_date']).dt.strftime('%Y-%m-%d')
                            df_f_sql = df_f_sql[df_f_sql['trade_date'] > last_date_str].dropna()
                            df_f_sql[['code', 'trade_date', 'factor']].to_sql('adjustment_factors', conn, if_exists='append', index=False)

                    feather_path = os.path.join(paths['FEATHER_DIR'], f"{code.replace('.', '_')}.feather")
                    process_and_save_feather(code, kline_dict[code], df_factors, df_status, feather_path)
                    
                    processed_count += 1
                except Exception as inner_e:
                    logger.error(f"处理标的 {code} 出错: {inner_e}")

            logger.info(f"批次处理结束，更新成功: {processed_count}/{len(batch)}")
    except Exception:
        logger.exception("流程中断:")
    finally:
        conn.close()
        try: ad.logout(auth['USER'])
        except: pass
        logger.info("进程退出。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YeQuant Quantitative System")
    parser.add_argument('--config', type=str, default='config.json', help='配置文件路径')
    args = parser.parse_args()

    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        run_pipeline(config_data)
    except FileNotFoundError:
        logger.critical(f"错误: 找不到配置文件 {args.config}")
    except Exception as e:
        logger.critical(f"启动失败: {e}")
    os._exit(0)