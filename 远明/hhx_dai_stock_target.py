# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/15 16:37
# @Author  : diaozhiwei
# @FileName: hhx_dai_stock_target.py
# @description: 
# @update:
"""

import pandas as pd
from datetime import datetime, timedelta
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
schema2 = 'ymlj_dx'
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


def get_dai_stock_target():
    sql = '''
    SELECT b.*, go.goal
    FROM (
        SELECT a.centel, a.department, a.member_type, a.quar, sum(jieyu_amount) amount
        FROM (
            SELECT ord.member_id, QUARTER(ord.create_time) quar, jieyu_amount, m.dept_name, dep.centel, dep.department
                ,case 
                WHEN member_wzd is not null AND dep.centel = '三中心' THEN '网转电'
                WHEN m.member_source = '电商平台获取' THEN '电商客户'
                WHEN year(first_order_time)<2023 THEN '成交客户'
                WHEN m.member_source = '投流线索获取' THEN '电销未成交线索'
                ELSE '网销未成交老粉'
                end as member_type
            FROM ymlj_dx.dai_orders_new ord
            LEFT JOIN ymlj_dx.`dai_member_5.15` m on ord.member_id = m.member_id
            LEFT JOIN ymlj_dx.dai_dept dep on m.dept_name = dep.dept_name
            WHERE 1=1
                AND ord.create_time>='2023-01-01' 
                AND year(LEAST(ord.first_communicate_time , ord.incoming_line_time))<2023
                AND dep.channel = '销售部'
            ) a 
            GROUP BY a.centel, a.department, a.member_type, a.quar
    ) b 
        left JOIN ymlj_dx.dai_goal_depart go on CONCAT(go.centel,go.department,go.member_type, go.quar) = CONCAT(b.centel,b.department,b.member_type, b.quar)
        
        UNION
        
    SELECT go.centel,go.department,go.member_type,go.quar, 0,go.goal
    FROM (
        SELECT a.centel, a.department, a.member_type, a.quar, sum(jieyu_amount) amount
        FROM (
            SELECT ord.member_id, QUARTER(ord.create_time) quar, jieyu_amount, m.dept_name, dep.centel, dep.department
                ,case 
                WHEN member_wzd is not null AND dep.centel = '三中心' THEN '网转电'
                WHEN m.member_source = '电商平台获取' THEN '电商客户'
                WHEN year(first_order_time)<2023 THEN '成交客户'
                WHEN m.member_source = '投流线索获取' THEN '电销未成交线索'
                ELSE '网销未成交老粉'
                end as member_type
            FROM ymlj_dx.dai_orders_new ord
            LEFT JOIN ymlj_dx.`dai_member_5.15` m on ord.member_id = m.member_id
            LEFT JOIN ymlj_dx.dai_dept dep on m.dept_name = dep.dept_name
            WHERE 1=1
                AND ord.create_time>='2023-01-01' 
                AND year(LEAST(ord.first_communicate_time , ord.incoming_line_time))<2023
                AND dep.channel = '销售部'
            ) a 
            GROUP BY a.centel, a.department, a.member_type, a.quar
    ) b 
        right JOIN ymlj_dx.dai_goal_depart go on CONCAT(go.centel,go.department,go.member_type, go.quar) = CONCAT(b.centel,b.department,b.member_type, b.quar)
        WHERE b.amount is null
    '''
    df=get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `dai_stock_target_total` 
     (`id`,`centel`,`department`,`member_type`,`quar`,
     `amount`,`goal`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `centel`= VALUES(`centel`),`department`= VALUES(`department`),`member_type`=VALUES(`member_type`),
         `quar`=values(`quar`),`amount`=values(`amount`),`goal`=values(`goal`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table dai_stock_target_total;
    '''
    executeSqlByConn(sql)


def main():
    df_stock_target=get_dai_stock_target()
    df_stock_target=df_stock_target.fillna(0)
    df_stock_target['id']=df_stock_target['centel']+df_stock_target['department']+df_stock_target['member_type']+df_stock_target['quar'].astype(str)
    df_stock_target=df_stock_target[['id','centel','department','member_type','quar','amount','goal']]
    df_stock_target.to_excel('./测试.xlsx',index=False)
    del_sql()
    save_sql(df_stock_target)


if __name__ == '__main__':
    main()


