__Author__ = "Junxiao Zhao"

import datetime
import schedule
import time
from functions import is_trade_date, sh_today, sz_today, bj_today

def update_stock():
    today = datetime.date.today().strftime('%Y-%m-%d')

    with open("Z:\log_CN.txt", 'a') as f:
        if is_trade_date():
            print("今日是交易日，正在更新数据...")
            sh_today(True)
            sz_today(True)
            bj_today(True)
            f.write(today + " 当日数据更新完毕\n")
        else:
            print("今日不是交易日")
            f.write(today + " 今日不是交易日\n")

if __name__ == "__main__":
    schedule.every().day.at("19:00").do(update_stock)
    while True:
        schedule.run_pending()
        time.sleep(1)
    

    
