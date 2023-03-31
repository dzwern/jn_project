# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/29 10:03
# @Author  : diaozhiwei
# @FileName: 测试01.py
# @description:
"""

import pandas as pd
from datetime import  datetime

# time1='2023-01-31 16:29:17'
#
# time2='2023-02-01 09:38:11'
#
#
# df = ((pd.to_datetime(time2, format='%Y-%m-%d') - pd.to_datetime(time1, format='%Y-%m-%d'))/pd.Timedelta(1, 'D'))
#
#
# df2=pd.to_datetime(str('2023-02-01 09:38:11'),format='%Y-%m-%d')
#
#
# df3=datetime.strptime(time2, '%Y-%m-%d')
#
# print(df3)

# '2023-02-01 09:38:11'
# '2022-06-11 11:03'


import datetime
strTime = '2022-06-11 11:03'
strTime = datetime.datetime.strptime(strTime[0:10],"%Y-%m-%d")
print(strTime)

df_arrangement_operation_records = df_arrangement_operation_records.fillna(0)
df_arrangement_operation_records['startTime'] = df_arrangement_operation_records['startTime'].apply(lambda x: '1900-01-01' if x == 0 else x)
df_arrangement_operation_records['endTime'] = df_arrangement_operation_records['endTime'].apply( lambda x: '1900-01-01' if x == 0 else x)
df_arrangement_operation_records['date'] = df_arrangement_operation_records['date'].apply(lambda x: '1900-01-01' if x == 0 else x)
df_arrangement_operation_records['startTime'] = pd.to_datetime(df_arrangement_operation_records['startTime'])
df_arrangement_operation_records['endTime'] = pd.to_datetime(df_arrangement_operation_records['endTime'])
df_arrangement_operation_records['date'] = pd.to_datetime(df_arrangement_operation_records['date'])
df_arrangement_operation_records['startTime'] = df_arrangement_operation_records['startTime'].apply(lambda x: x.strftime('%Y-%m-%d'))
df_arrangement_operation_records['endTime'] = df_arrangement_operation_records['endTime'].apply(lambda x: x.strftime('%Y-%m-%d'))
df_arrangement_operation_records['date'] = df_arrangement_operation_records['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

