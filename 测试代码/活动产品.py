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


def get_product_order():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.product_name,
        sum(a.quantity) quantitys
    FROM
        t_order_item_middle a 
    WHERE a.activity_name='{}'
    GROUP BY a.dept_name,a.product_name
    '''.format(activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_product_campaign;
    '''
    executeSqlByConn(sql)


def save_sql(df):
    sql = '''
    INSERT INTO `t_product_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`product_name`,`quantitys`,`activity_name`) 
     VALUES (%s,%s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `product_name`=values(`product_name`),`quantitys`=values(`quantitys`),`activity_name`=values(`activity_name`)
         '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 产品信息
    df_product_order=get_product_order()
    df_product_order['id']=df_product_order['dept_name'].astype(str)+df_product_order['product_name'].astype(str)
    df_product_order['activity_name'] = '2023年5.1活动'
    df_product_order=df_product_order[['id','dept_name1','dept_name2','dept_name','product_name','quantitys','activity_name']]
    del_sql()
    save_sql(df_product_order)


if __name__ == '__main__':
    activity_name = '2023年38女神节活动'
    main()




