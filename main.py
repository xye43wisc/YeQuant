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
    df_k: 原始K线 (DataFrame)
    df_f: 复权因子 (全量宽表 DataFrame)
    df_s: 历史证券状态 (字典 Dict[str, DataFrame])
    """
    print(f"\n    >>> [FEATHER-DEBUG] 开始处理标的: {code}")
    
    # 1. 统一 K 线日期格式
    df_k = df_k.copy()
    df_k['date'] = pd.to_datetime(df_k['kline_time']).dt.strftime('%Y-%m-%d')
    print(f"        - K线数据行数: {len(df_k)} | 日期范围: {df_k['date'].min()} ~ {df_k['date'].max()}")
    
    # 2. 准备因子数据 (从宽表中提取对应列)
    if df_f is not None and code in df_f.columns:
        df_f_single = df_f[[code]].rename(columns={code: 'adj_factor'})
        df_f_single.index = pd.to_datetime(df_f_single.index).strftime('%Y-%m-%d')
        print(f"        - 成功提取复权因子")
    else:
        print(f"        - [WARN] 未找到 {code} 的因子，使用默认值 1.0")
        df_f_single = pd.DataFrame(columns=['adj_factor'])

    # 3. 准备状态数据 (关键修复：从字典中提取)
    # 根据你打印的结构，df_s 是 dict，所以直接用 .get(code)
    if isinstance(df_s, dict):
        df_s_single = df_s.get(code)
    else:
        # 兼容性处理：如果以后 SDK 改回返回单张大表
        df_s_single = df_s[df_s['MARKET_CODE'] == code] if df_s is not None else None

    if df_s_single is not None and not df_s_single.empty:
        df_s_single = df_s_single.copy()
        # 统一日期格式：将 20251219 转换为 2025-12-19
        df_s_single['date_fmt'] = pd.to_datetime(df_s_single['TRADE_DATE']).dt.strftime('%Y-%m-%d')
        print(f"        - 成功提取状态数据: {len(df_s_single)} 行")
    else:
        print(f"        - [WARN] 未找到 {code} 的状态数据 (ST/停牌/涨跌停)")
        df_s_single = pd.DataFrame(columns=['date_fmt', 'HIGH_LIMITED', 'LOW_LIMITED', 'IS_ST_SEC', 'IS_SUSP_SEC'])

    # 4. 合并数据
    # 合并因子
    df = pd.merge(df_k, df_f_single, left_on='date', right_index=True, how='left')
    # 合并状态 (使用格式化后的日期列)
    df = pd.merge(df, df_s_single, left_on='date', right_on='date_fmt', how='left')
    
    # 5. 填充默认值与计算
    df['adj_factor'] = df['adj_factor'].ffill().fillna(1.0)
    df['IS_ST_SEC'] = df['IS_ST_SEC'].fillna('0')
    df['IS_SUSP_SEC'] = df['IS_SUSP_SEC'].fillna('0')

    # 计算复权价格
    for col in ['open', 'high', 'low', 'close']:
        df[f'{col}_post'] = df[col] * df['adj_factor']
    
    df['close_raw'] = df['close']
    df['volume_post'] = df['volume'] / df['adj_factor']
    df['daily_return'] = df['close_post'].pct_change()

    # 6. 状态标识逻辑
    # is_trading: 有成交量且未停牌
    df['is_trading'] = ((df['volume'] > 0) & (df['IS_SUSP_SEC'] != '1')).astype(int)
    
    # 涨跌停判断 (增加字段存在性检查，防止 merge 失败导致 crash)
    if 'HIGH_LIMITED' in df.columns:
        df['is_limit_up'] = (df['close'] >= df['HIGH_LIMITED']).astype(int)
        df['is_limit_down'] = (df['close'] <= df['LOW_LIMITED']).astype(int)
    else:
        print("        - [ERROR] 缺失涨跌停字段，请检查日期对齐")
        df['is_limit_up'] = 0
        df['is_limit_down'] = 0
    
    # IS_ST_SEC 根据手册是字符串 '1' 或 '0'
    df['is_st'] = (df['IS_ST_SEC'] == '1').astype(int)

    # 7. 整理最终列并保存
    final_cols = [
        'date', 
        'open_post', 'high_post', 'low_post', 'close_post', 
        'close_raw', 'adj_factor', 'daily_return', 
        'volume_post', 'amount', 'is_trading', 
        'is_limit_up', 'is_limit_down', 'is_st'
    ]
    
    # 检查是否有列缺失（防止计算过程出错）
    existing_cols = [c for c in final_cols if c in df.columns]
    df_final = df[existing_cols].reset_index(drop=True)
    
    df_final.to_feather(feather_path)
    print(f"    <<< [FEATHER-SUCCESS] {code} 已保存至 {feather_path} | 最终行数: {len(df_final)}")
    
    return df_final

def run_pipeline(is_test=True):
    # 1. 登录平台
    print("正在尝试登录星耀数智平台...")
    ad.login(username=USER, password=PWD, host=IP, port=PORT)
    conn = init_base_database()
    
    try:
        base_data = ad.BaseData()
        info_data = ad.InfoData()
        
        # 1. 检查标的列表获取情况
        all_codes = base_data.get_code_list(security_type='EXTRA_STOCK_A_SH_SZ')
        print(f">>> [DEBUG] 成功从接口获取全量 A 股代码共: {len(all_codes)} 只")

        calendar = base_data.get_calendar(market='SH') 
        market_data = ad.MarketData(calendar) 

        target_codes = all_codes[:10] if is_test else all_codes
        print(f">>> [DEBUG] 当前模式: {'测试' if is_test else '全量'}, 计划处理标的数量: {len(target_codes)}")
        print(f">>> [DEBUG] 计划处理的代码列表: {target_codes}")

        batch_size = 50
        for i in range(0, len(target_codes), batch_size):
            batch = target_codes[i : i + batch_size]
            print(f"\n=== 开始处理批次: {i+1} 至 {min(i+batch_size, len(target_codes))} ===")
            
            # 2. 检查批量数据请求结果
            print(f">>> [FETCH] 正在请求 K 线数据 ({START_DATE} - {END_DATE})...")
            kline_dict = market_data.query_kline(batch, START_DATE, END_DATE, ad.constant.Period.day.value) 
            print(f">>> [DEBUG] K 线数据返回代码量: {len(kline_dict.keys()) if kline_dict else 0}")

            print(f">>> [FETCH] 正在请求复权因子...")
            df_factors = base_data.get_backward_factor(batch, local_path=LOCAL_CACHE)
            print(f">>> [DEBUG] 复权因子表列数 (代码量): {len(df_factors.columns) if df_factors is not None else 0}")

            print(f">>> [FETCH] 正在请求历史证券状态 (ST/涨跌停)...")
            df_status = info_data.get_history_stock_status(batch, local_path=LOCAL_CACHE) 
            print(f">>> [DEBUG] 历史状态表记录数: {len(df_status) if df_status is not None else 0}")
            # 3. 逐一处理内部循环
            processed_count = 0
            for code in batch:
                print(f"  > 正在校验标的: {code}")
                
                # 细化检查过滤原因
                if kline_dict is None or code not in kline_dict:
                    print(f"    [SKIP] {code} 缺失 K 线数据")
                    continue
                if df_factors is None:
                    print(f"    [SKIP] 复权因子表为空")
                    continue
                if code not in df_factors.columns:
                    print(f"    [SKIP] {code} 在复权因子表中无对应列")
                    continue
                
                try:
                    # A. 存入 SQL (归档)
                    df_raw = kline_dict[code].reset_index()
                    df_raw['kline_time'] = pd.to_datetime(df_raw['kline_time']).dt.strftime('%Y-%m-%d')
                    df_raw['code'] = code
                    df_raw[['code', 'kline_time', 'open', 'high', 'low', 'close', 'volume', 'amount']].to_sql(
                        'daily_klines_raw', conn, if_exists='append', index=False
                    )

                    # B. 生成 Feather 缓存
                    feather_path = os.path.join(FEATHER_DIR, f"{code.replace('.', '_')}.feather")
                    process_and_save_feather(code, kline_dict[code], df_factors, df_status, feather_path)
                    
                    processed_count += 1
                    print(f"    [SUCCESS] {code} 处理完成并保存")
                    
                except Exception as inner_e:
                    print(f"    [ERROR] {code} 处理过程中报错: {inner_e}")

            print(f"=== 批次处理结束, 成功: {processed_count}/{len(batch)} ===")

    except Exception as e:
        print(f"\n>>> [FATAL ERROR] 流程被中断: {e}")
        import traceback
        traceback.print_exc() # 打印完整堆栈信息
    finally:
        conn.close()
        try:
            ad.logout(USER)
        except:
            pass
        print("\n>>> [FINISH] 进程已正常退出。")

if __name__ == "__main__":
    run_pipeline(is_test=True)
    os._exit(0)