__Author__ = "Junxiao Zhao"

import requests
import pandas as pd
import re  # 正则表达式库
import json  # python自带的json库
import time
import datetime
import random
import os
import sys
from io import StringIO

def get_headers():
    headers = [{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62'},
        {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50"},
        {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1"},
        {"User-Agent": "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11"}]
    return headers[random.randint(0,3)]

def info_each():
    return {
        'code': None,
        'start': 0,
        'end': 20491001,
        'fields': 'TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP',
    }, 'http://quotes.money.163.com/service/chddata.html'

def info_all():
    return {
        "cb": "jQuery112400007185746155222716_1640776475085",
        "pn": 1,
        "pz": 200,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": None,
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        "_": None
    }, "http://11.push2.eastmoney.com/api/qt/clist/get"

#获取上证股票列表或更新今日数据
def sh_today(update):
    # 时间随机数
    t = time.time()
    sh_params_all, sh_url_all = info_all()
    random_timestamp = (int(round(t * 1000)))
    sh_params_all["fs"] = "m:1 t:2,m:1 t:23"
    sh_params_all["_"] = random_timestamp
    return request_data(sh_url_all, sh_params_all, "sh", update)

#获取上证股票历史数据
def get_sh_history():
    sh_params_each, sh_url_each = info_each()
    sh_list = sh_today(False)
    pos = 0

    while pos < len(sh_list):
        sh_params_each["code"] = f'0{sh_list[pos]}'
        status = get_history(sh_list[pos], sh_url_each, sh_params_each, "sh")
        if status == "Fail":
            time.sleep(60)
        else:
            pos += 1

#获取深证股票列表或更新今日数据
def sz_today(update):
    # 时间随机数
    t = time.time()
    sz_params_all, sz_url_all = info_all()
    random_timestamp = (int(round(t * 1000)))
    sz_params_all["fs"] = "m:0 t:6,m:0 t:80"
    sz_params_all["_"] = random_timestamp
    return request_data(sz_url_all, sz_params_all, "sz", update)

#获取深证股票历史数据
def get_sz_history():
    sz_params_each, sz_url_each = info_each()
    sz_list = sz_today(False)
    pos = 0

    while pos < len(sz_list):
        sz_params_each["code"] = f'1{sz_list[pos]}'
        status = get_history(sz_list[pos], sz_url_each, sz_params_each, "sz")

        if status == "Fail":
            time.sleep(60)
        else:
            pos += 1

#获取北证股票列表或更新今日数据
def bj_today(update):
    # 时间随机数
    t = time.time()
    random_timestamp = (int(round(t * 1000)))
    bj_params_all, bj_url_all = info_all()
    bj_params_all["fs"] = "m:0 t:81 s:2048"
    bj_params_all["_"] = random_timestamp
    return request_data(bj_url_all, bj_params_all, "bj", update)

#获取北证股票历史数据
def get_bj_history():
    bj_params_each, bj_url_each = info_each()
    bj_list = bj_today(False)
    pos = 0

    while pos < len(bj_list):
        bj_params_each["code"] = f'1{bj_list[pos]}'
        status = get_history(bj_list[pos], bj_url_each, bj_params_each, "bj")

        if status == "Fail":
            time.sleep(60)
        else:
            pos += 1

def request_data(url, params, market, update):
    stock_df = pd.DataFrame()
    while True:
        try:
            print(f'正在访问{market}第{params["pn"]}页...')
            # 获取url内容
            r = requests.get(url=url, params=params, timeout=30, headers=get_headers())
            content = r.content
            content = content.decode()  # 对内容进行解码
            content = re.findall(r'jQuery\d+_\d+\((.+)\)', content)[0]

            # ==内容解析
            js_content = json.loads(content)  # 转化为dict格式

        except Exception as e:
            print(f'出现{e}报错，暂停访问，保存已获取数据')
            break
        
        if js_content['data'] is not None:
            # 数据整理
            data = js_content['data']['diff']
            # 将数据整理成表格
            df = pd.DataFrame(data)
            # 重命名
            rename_dic = {'f14': '名称', 'f12': '股票代码', 'f15': '最高价', 'f16': '最低价', 
                          'f17': '开盘价', 'f2': '收盘价', 'f3': '涨跌幅', 'f4': '涨跌额', 'f5': '成交量',
                          'f6': '成交额', 'f8': '换手率', 'f18': '前收盘', 'f20': '总市值', 'f21': '流通市值'}
                          #'f7': '振幅%', 'f9': '市盈率', 'f10': '量比', 'f23': '市净率'}
            df.rename(columns=rename_dic, inplace=True)
            # 筛选所需数据
            df = df[rename_dic.values()]
            df["成交量"] = pd.to_numeric(df["成交量"]) * 100
            stock_df = stock_df.append(df, ignore_index=True)

        else:
            print('===数据已获取完毕===')
            break

        params['pn'] += 1
        time.sleep(random.randint(1, 5))

    if not update:
        return stock_df['股票代码']

    else:
        # =补全股票代码
        stock_df['股票代码'] = market + stock_df['股票代码']
        # =添加日期
        stock_df['日期'] = datetime.date.today().strftime('%Y-%m-%d')
        # =调整列顺序
        stock_df = stock_df[["日期", "股票代码", '名称', '收盘价', '最高价', '最低价', '开盘价', 
                            '前收盘', '涨跌额', '涨跌幅', '换手率', '成交量', '成交额', '总市值', '流通市值']]   
        # ===存储数据
        for i in stock_df.index:
            t = stock_df.iloc[i:i+1, :]
            stock_code = t.iloc[0]['股票代码']

            print('正在存储数据：'+stock_code)
            # 构建存储文件路径
            path = f'Z:/stock_database_CN/{stock_code}.csv'
            # 文件存在，不是新股
            if os.path.exists(path):
                t.to_csv(path, header=None, index=False, mode='a', encoding='utf-8-sig')
            # 文件不存在，说明是新股
            else:
                t.to_csv(path, index=False, encoding='utf-8-sig')
         

def get_history(stock_code, url, params, market):
    stock_code = market + stock_code
    if not os.path.isfile(f'Z:/stock_database_CN/{stock_code}.csv'):
        try:
            print(f'正在访问{stock_code}的历史日K线数据...')
            # 获取url内容
            r = requests.get(url=url, params=params, timeout=30, headers=get_headers())
            content = r.content
            content = content.decode('gbk')  # 对内容进行解码

            # ==内容解析
            content = StringIO(content)  # 用于像文件一样对字符串缓冲区或者叫做内存文件进行读写。
            each_stock_history = pd.read_csv(content, parse_dates=['日期'], na_values='None')

            # 数据整理
            each_stock_history['股票代码'] = stock_code
            each_stock_history.sort_values(by=['日期'], ascending=True, inplace=True)
            each_stock_history.reset_index(drop=True, inplace=True)

            each_stock_history.to_csv(f'Z:/stock_database_CN/{stock_code}.csv', index=False, encoding= "utf-8-sig")

            time.sleep(random.randint(1, 5))

        except Exception as e:
            print(f'出现{e}报错，暂停访问，保存已获取数据')
            return "Fail"

    else:
        print(f'{stock_code}的历史数据已存在')

def is_trade_date():
    trade_date_url = "https://hq.sinajs.cn/wskt?list=sh000001"

    try:
        print(f'正在判断今天是否为交易日...')
        # 获取url内容
        r = requests.get(url=trade_date_url, timeout=30)
        content = r.content
        content = content.decode('gbk')  # 对内容进行解码

        last_trade_date = re.findall(f'hq_str_sh000001="(.+)"', content)[0].split(",")[-4]
        return (last_trade_date == datetime.date.today().strftime('%Y-%m-%d'))

    except Exception as e:
        print(f'出现{e}报错，暂停访问')

bj_today(True)