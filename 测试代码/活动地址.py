# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:31
# @Author  : diaozhiwei
# @FileName: hhx_order_pred_campaign.py
# @description: 活动预估，使用预估的数据进行实时监控，实时监控表，到员工
# @update：更新时间在，活动中监控
"""

import pandas as pd
from datetime import  datetime,timedelta
import sys
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import numpy as np

userName = 'dzw'
password = 'dsf#4oHGd'
dbHost = 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com'
dbPort = 3306
URL = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/'
schema = 'crm_tm_jnmt'
schema2 = 'hhx_dx'
engine = create_engine(URL + schema + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)
engine2 = create_engine(URL + schema2 + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)


# 加载数据到df
def get_DataFrame_PD(sql='SELECT * FROM DUAL'):
    conn = engine.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 加载数据到df
def get_DataFrame_PD2(sql='SELECT * FROM DUAL'):
    conn = engine2.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 批量执行更新sql语句
def executeSqlManyByConn(sql, data):
    conn = engine2.connect()
    if len(data) > 0:
        with conn as connection:
            return connection.execute(sql, data)


# 时间转化字符串
def date2str(parameter, format='%Y-%m-%d'):
    if isinstance(parameter, str):
        return parameter
    return parameter.strftime(format)


# 执行sql
def executeSqlByConn(sql='SELECT * FROM DUAL', conn=None):
    conn = engine2.connect()
    with conn as connection:
        return connection.execute(sql)


def get_city_order():
    sql = '''
    select 
        a.dept_name,
        a.receiver_province,
        a.receiver_city,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) members_amount
    from 
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.activity_name='{}'
    GROUP BY a.dept_name,a.receiver_province,a.receiver_city
    '''.format(activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 员工信息
def get_hhx_user():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组','光芒部一组',
           '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组','光华部1组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端',
           '光芒部蜜梓源后端','光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端',
           '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉后端','光华部蜜梓源面膜老粉前端','光华部蜜梓源面膜老粉后端','光华部蜜梓源面膜进粉后端',
           '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组','光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部',
           '光芒部', '光芒部', '光芒部', '光芒部',
           '光华部', '光华部', '光华部', '光华部', '光华部','光华部','光华部',
           '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name2': df2,
          'dept_name1': df3}
    data = pd.DataFrame(df)
    return data


def save_sql(df):
    sql = '''
    INSERT INTO `t_address_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`receiver_province`,
     `receiver_city`,`members`,`members_rate`,`members_amount`,`members_amount_rate`,
     `members_value`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `receiver_province`=values(`receiver_province`),`receiver_city`=values(`receiver_city`),`members`=values(`members`),
         `members_rate`=values(`members_rate`),`members_amount`=values(`members_amount`),`members_amount_rate`=values(`members_amount_rate`),
         `members_value`=values(`members_value`),`activity_name`=values(`activity_name`)
         '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础城市数据源
    df_city_order = get_city_order()
    # 部门表
    df_dept_name = get_hhx_user()
    df_city_order = df_city_order.merge(df_dept_name, on=['dept_name'], how='left')
    df_city_order = df_city_order
    df_city_order['members_sum'] = df_city_order['members'].sum()
    df_city_order['members_amount_sum'] = df_city_order['members_amount'].sum()
    # 占比
    df_city_order['members_rate'] = df_city_order['members'] / df_city_order['members_sum']
    df_city_order['members_amount_rate'] = df_city_order['members_amount'] / df_city_order['members_amount_sum']
    # 价值
    df_city_order['members_value'] = df_city_order['members_amount'] / df_city_order['members']
    df_city_order = df_city_order.fillna(0)
    df_city_order['activity_name'] = '2023年5.1活动'
    df_city_order['id'] = df_city_order['dept_name'] + df_city_order['receiver_province'] + df_city_order[
        'receiver_city']
    df_city_order = df_city_order[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'receiver_province', 'receiver_city', 'members',
         'members_rate', 'members_amount', 'members_amount_rate', 'members_value', 'activity_name']]
    df_city_order=df_city_order
    save_sql(df_city_order)


if __name__ == '__main__':
    activity_name = '2023年38女神节活动'
    main()
