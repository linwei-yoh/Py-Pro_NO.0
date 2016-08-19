#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'get detail Stock list from cenceptList'

__author__ = 'AL'

import tushare as ts
import pandas as pd
import DB_Helper
import GetConceptList as gcl
from sqlalchemy import create_engine


def GetDetailListToDB():
    ConceptList = gcl.GetConceptList()
    if len(ConceptList) == 0:
        print("概念列表为空")
    else:
        df = ts.get_concept_classified()
        conn = DB_Helper.ConnectStockDB()
        df[df['c_name'].isin(ConceptList)]\
                       .to_sql(DB_Helper.StockList_Table_Name ,\
                               conn,\
                               if_exists = 'replace')
        conn.close()
        print("\n成份股列表已经导入")
