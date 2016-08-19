#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import DB_Helper  as DB
import GetConceptList as GCL
import pandas as pd
from pandas import ExcelWriter
from sqlalchemy import create_engine



def savehistorydatatoexcels(c_namelist):
    engine = create_engine('sqlite:///' + DB.DataBaseName)
    # 获取完整的概念 成份股 code-名称
    data = pd.read_sql_table(DB.StockList_Table_Name, engine)
    Base_dir = os.path.join(os.path.dirname(__file__),'historyFile')
    
    #逐个概念写入一个Excel
    for c_name in c_namelist:
        codelist =data[data.c_name == c_name].code.tolist()
        excelname = ("%s.xlsx" % c_name)
        excelpath = os.path.join(Base_dir, excelname)
        writer = ExcelWriter(excelpath)
        #逐个code写入一个sheet
        for code in codelist:
            code_name = (data[data.code == code].name.values.tolist())[0]
            trans = str.maketrans({'*':'X'})
            code_name = code_name.translate(trans)
            #获取一个code的历史数据
            Sqlquery = ("SELECT * FROM HistoryList WHERE code = %s" % code)
            det_df = pd.read_sql_query(Sqlquery,engine)
            det_df = det_df.drop(['code','Id'],axis=1)
            try:
                det_df.to_excel(writer, sheet_name=code_name)
            except ValueError as e:
                print("\n %s 有问题" % code_name)
            
        writer.save()

            

if __name__ == '__main__':
    if not DB.DB_File_Exist():
        print("数据库不存在")
    else:
        # 概念名称列表
        cnamelist = GCL.GetConceptList()
        savehistorydatatoexcels(cnamelist)
