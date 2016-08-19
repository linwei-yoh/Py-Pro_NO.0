#!/usr/bin/env python3
# -*- coding: utf-8 -*-

' get history list'


import DB_Helper
import datetime
import tushare as ts
import socket
import logging
from multiprocessing import Pool



def DisPlayHisTable():
    conn = DB_Helper.ConnectStockDB()
    cursor = conn.cursor()
    cursor.execute('select * from HistoryList')
    values = cursor.fetchall()
    cursor.close()
    return values


def getlistrange(lens,step):
    num = int(lens / step)
    inputlist = [step * x for x in range(num + 1)]
    
    if inputlist[-1] != lens:
        inputlist.append(lens)
        
    inputlen = len(inputlist)
    outputlist = []
    for i in range(inputlen - 1):
        outputlist.append([inputlist[i],inputlist[i+1]])
    return outputlist
    
    
logger = logging.getLogger('mylogger')


def logconfig():
    # 创建一个logger
    logger = logging.getLogger('mylogger')
    logger.setLevel(logging.DEBUG)
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler('test.log')
    fh.setLevel(logging.DEBUG)
    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # 给logger添加handler
    logger.addHandler(fh)


def History_task(code,startTime,endTime):
    try:
        df = ts.get_h_data(code, start=startTime, end=endTime)
        if df is None:
            return
        df['code'] = code
        return df
    except Exception as e:
        logger.error("code:%s 查询失败" % code)
        logger.exception(e)
        return None

if __name__ == '__main__':
    logconfig()
    conn = DB_Helper.ConnectStockDB()
    cursor = conn.cursor()
    cursor.execute('select code from StockList')
    values = cursor.fetchall()
    cursor.close()
    codeList = [i[0] for i in values]
    seglist = getlistrange(len(codeList),100)
    
    i = datetime.datetime.now()
    endTime = ("%d-%02d-%02d" % (i.year, i.month, i.day))
    startTime = ("%d-%02d-%02d" % (i.year-3, i.month, i.day))
    
    print("开始数据采集\n")
    maxsize = len(seglist)
    count = 30
    for seg in seglist[30:maxsize]:
        simpleList = codeList[seg[0]:seg[1]]
        p = Pool(8)
        dfList = []
        for code in simpleList:
            dfList.append(p.apply_async(History_task, args=(code, startTime, endTime)))
        p.close()
        p.join()

        for dfRes in dfList:
            df = dfRes.get()
            if df is None:
                continue
            df.to_sql(DB_Helper.HistoryList_Table_Name, conn, if_exists='append')
        count += 1
        print("已经完成%d/%d" % (count,maxsize))
        
    print("历史数据载入完成\n")
    conn.close()
    
