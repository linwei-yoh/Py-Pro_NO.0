#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'Read ConceptList From Excel to DB'

__author__ = 'AL'


import pandas as pd
import DB_Helper,os
from sqlalchemy import create_engine

ExcelName = 'stock_type.xlsx'

def GetConceptListToDB():
    excel_Path = os.path.join(os.path.dirname(__file__),ExcelName)
    if not os.path.isfile(excel_Path):
        print('概念列表名称文件不存在')
        return
    if not DB_Helper.DB_File_Exist():
        print('数据库不存在')
        return

    conn = DB_Helper.ConnectStockDB()    
    df = pd.read_excel("stock_type.xlsx",sheetname="stock_type").c_name
    df.to_sql(DB_Helper.ConceptList_Table_Name ,\
              conn,\
              if_exists = 'replace')
    conn.close()
    print("概念列表文件已经载入到数据库中")


def GetConceptList():
    if DB_Helper.DB_File_Exist():
        conn = DB_Helper.ConnectStockDB()
        cursor = conn.cursor()
        cursor.execute('select c_name from ' + DB_Helper.ConceptList_Table_Name)
        values = cursor.fetchall()
        valList = [i[0] for i in values]
        cursor.close()
        conn.close()
        return valList
    else:
        print('数据库不存在')
        return []
