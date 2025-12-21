import AmazingData as ad
import pandas as pd
import sqlite3
import datetime
import os

import json

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
DB_FILE = "Meituan_Test.db"       # 测试数据库文件

def test_meituan_pipeline():
    # 2. 登录平台 [cite: 150, 158]
    print("正在尝试登录星耀数智平台...")
    ad.login(username=USER, password=PWD, host=IP, port=PORT)
    print("登录星耀数智平台成功")
    # 3. 初始化基础数据对象以获取交易日历 [cite: 293, 302]
    base_data = ad.BaseData()
    # 获取港股市场交易日历 [cite: 296, 1260]
    calendar = base_data.get_calendar(market='SZN') 
    # code_list = base_data.get_code_list(security_type='EXTRA_HKT')
    #print(code_list)
    
    # 4. 获取美团日线数据 [cite: 534, 535]
    # 美团代码在 SDK 中的标准格式为 '03690.SZ' 
    meituan_code = ['03690.SZ']
    market_data = ad.MarketData(calendar)
    
    today = datetime.datetime.now().strftime("%Y%m%d")
    print(f"正在查询美团 (03690.SZ) 从 20250101 到 {today} 的日线数据...")
    
    # 查询历史 K 线 [cite: 537]
    kline_dict = market_data.query_snapshot(
        code_list=meituan_code,
        begin_date=20251201,
        #end_date=int(today),
        end_date=20251218,
        #period=ad.constant.Period.day.value  # 日线周期 [cite: 1286]
    )
    
    # if not kline_dict or '03690.SZ' not in kline_dict:
    #     print("未获取到美团数据，请检查网络或账号权限。")
    #     return
    print(kline_dict)
    return
    df = kline_dict['03690.SZ']
    print(f"成功获取 {len(df)} 条记录。")

    # 5. 存入 SQLite 数据库
    conn = sqlite3.connect(DB_FILE)
    try:
        # 数据清洗：确保包含代码列且索引重置 
        df = df.reset_index()
        df['code'] = '03690.SZ'
        
        # 按照开发手册 4.2.6 的标准字段存储 
        standard_cols = ['code', 'trade_time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        df = df[standard_cols]
        
        # 写入数据库 (如果表存在则追加)
        df.to_sql('daily_kline_test', conn, if_exists='replace', index=False)
        print(f"数据已成功保存至 {DB_FILE} 中的 'daily_kline_test' 表。")
        
        # 验证查询
        test_query = pd.read_sql("SELECT * FROM daily_kline_test LIMIT 5", conn)
        print("\n数据库前5条记录验证:")
        print(test_query)
        
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        test_meituan_pipeline()
    except Exception as e:
        print(f"测试运行失败: {e}")
    finally:
        # 强制登出并释放 SDK 占用的所有后台线程资源
        try:
            print("正在关闭 SDK 连接...")
            ad.logout() 
        except:
            pass
        print("进程已结束。")
    os._exit(0)


