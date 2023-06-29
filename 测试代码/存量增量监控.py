# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:00
# @Author  : diaozhiwei
# @FileName: demo_campaign.py
# @description: 活动期间整体数据指标监控
# @update:
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



# 设备客户类型
def get_campaign_time():
    sql = '''
    SELECT
        a.id,
        a.order_sn,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.wechat_id,
        a.wechat_name,
        a.wechat_number,
        a.member_id,
        a.first_time,
        left(a.first_time,7) year_months,
        year(a.first_time) years,
        a.create_time,
        a.time_diff,
        a.order_amount 
    FROM
        t_orders_middle a
    WHERE a.activity_name='2023年五一活动'
    '''
    df = get_DataFrame_PD2(sql)
    return df


def get_time_level(x):
    if datetime.strptime('2023-01-01','%Y-%m-%d') <= x:
        return '增量'
    else:
        return '存量'


def save_sql(df):
    sql = '''
    INSERT INTO `t_member_time_campaign` 
     (`id`,`order_sn`,`dept_name1`,`dept_name2`,`dept_name`,
     `sys_user_id`,`user_name`,`nick_name`,`wechat_id`,`wechat_name`,
     `wechat_number`,`member_id`,`first_time`,`stock_increment`,`year_months`,
     `years`,`create_time`,`time_diff`,`order_amount`,`activity_name`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_sn`= VALUES(`order_sn`),`dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),
         `dept_name`=values(`dept_name`),`sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),
         `nick_name`=values(`nick_name`),`wechat_id`=values(`wechat_id`),`wechat_name`=values(`wechat_name`),
         `wechat_number`=values(`wechat_number`),`member_id`=values(`member_id`),`first_time`=values(`first_time`),
         `stock_increment`=values(`stock_increment`),`year_months`=values(`year_months`),`years`=values(`years`),
         `create_time`=values(`create_time`),
         `time_diff`=values(`time_diff`),`order_amount`=values(`order_amount`),`activity_name`=values(`activity_name`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础数据
    df_member_time=get_campaign_time()
    df_member_time['activity_name'] = '2023年5.1活动'
    # 存量，增量
    df_member_time['stock_increment'] = df_member_time.apply(lambda x: get_time_level(x['first_time']), axis=1)
    df_member_time=df_member_time[['id','order_sn','dept_name1','dept_name2','dept_name','sys_user_id','user_name',
                                   'nick_name','wechat_id','wechat_name','wechat_number','member_id','first_time',
                                   'stock_increment','year_months','years','create_time','time_diff','order_amount',
                                   'activity_name']]
    save_sql(df_member_time)


if __name__ == '__main__':
    main()



