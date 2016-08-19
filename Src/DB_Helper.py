#!/usr/bin/env python3
# -*- coding: utf-8 -*-

' Create DB'

__author__ = 'AL'

import sqlite3,os
import pandas as pd
from sqlalchemy import create_engine

DataBaseName = 'stock.db'

ConceptList_Table_Name = 'ConceptList'
StockList_Table_Name = 'StockList'
HistoryList_Table_Name = 'HistoryList'


   

def __Creat_DB():
    conn = sqlite3.connect(DataBaseName)
    cursor = conn.cursor()   


    #id股票独有编号 股票名称 所属概念名称
    cursor.execute('create table ' + StockList_Table_Name +' \
                   (code    integer primary key,\
                    name    text    not null,\
                    c_name    text    not null)')
    
    #序号ID 股票编号 记录时间 开盘价 收盘价 最低价 成交量 价格变动 涨跌幅
    cursor.execute('create table ' + HistoryList_Table_Name +' \
                   (Id          integer     primary key,\
                   code         integer     not null,\
                   date         text        not null,\
                   open         real        not null,\
                   high         real        not null,\
                   close        real        not null,\
                   low          real        not null,\
                   volume       real        not null,\
                   amount       real        not null,\
                   UNIQUE (code,date) ON CONFLICT REPLACE)')

    cursor.close()
    conn.commit()
    conn.close()

def DB_File_Exist():
    db_file = os.path.join(os.path.dirname(__file__),DataBaseName)

    if os.path.isfile(db_file):
        return True
    else:
        return False

def InitStockDB():
    if DB_File_Exist():
        print("数据库已存在")
    else:
        __Creat_DB();
        print("数据库已创建")



def ConnectStockDB():
    conn = sqlite3.connect(DataBaseName)
    return conn

def SaveSqltoExcel(tableName,excelName,sh_name):
    if not DB_File_Exist():
        print('数据库不存在')
        return
    engine = create_engine('sqlite:///' + DataBaseName)
    with engine.connect() as conn, conn.begin():
        data = pd.read_sql_table(tableName, conn).loc[:,['code','name','c_name']]

    data.to_excel(excelName+'.  xlsx',sheet_name = sh_name)
    print("\n成份股列表已经保存至" + excelName+'.xlsx' + '文件中\n')

