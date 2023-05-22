# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/15 17:20
# @Author  : diaozhiwei
# @FileName: hhx_dai_member_resources.py
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


def get_member_resource():
    sql='''
        SELECT 
        channel, centel, department,
        member_rank, is_add, two_way_friend, 
        member_identity, cous_status, 存增量, 
    -- 	member_source, member_source_level2, 
        在后端成交, 
        member_wzd, 获客年份, 获客月份, 
    -- 	cycle_name, order_period, act_name, 
        COUNT(1) 客户数
    FROM (
        SELECT 
            m.dept_name, channel , centel, department 
            , account_status
            , member_rank, is_add , two_way_friend, member_identity, cous_status
            , if(amount_houduan>0, 1,0) 在后端成交
            , if(year(LEAST(first_communicate_time , incoming_line_time))<2023,'存量','增量') 存增量
    -- 		, member_source, member_source_level2
            , wzd.member_wzd
            , year(LEAST(first_communicate_time , incoming_line_time)) 获客年份
            , month(LEAST(first_communicate_time , incoming_line_time)) 获客月份
            , left(LEAST(first_communicate_time , incoming_line_time),10) 获客日期
    -- 		, cy.order_period , cy.cycle_name, cy.act_name
            , m.member_id
        FROM ymlj_dx.dai_member_new m
        INNER JOIN ymlj_dx.dai_dept dep on m.dept_name = dep.dept_name
    -- 	LEFT JOIN t_act_schedule as cy on LEAST(first_communicate_time , incoming_line_time) BETWEEN cy.start_time AND cy.end_time
        LEFT JOIN ymlj_dx.t_member_special as wzd on wzd.member_id = m.member_id
        WHERE 1=1
    -- 		AND m.member_id < 10000
            AND channel = '销售部'
            AND member_rank > 'V0'
    ) a
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    '''
    df=get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `dai_member_resources` 
     (`channel`,`centel`,`department`,`member_rank`,
     `is_add`,`two_way_friend`,`member_identity`,`cous_status`,`stock_increment`,
     `houduan_orders`,`member_wzd`,`years`,`monthly`,`members`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `channel`= VALUES(`channel`),`centel`=VALUES(`centel`),`department`=values(`department`),`member_rank`=values(`member_rank`),
         `is_add`=values(`is_add`),`two_way_friend`=values(`two_way_friend`),`member_identity`=values(`member_identity`),
         `cous_status`=values(`cous_status`),`stock_increment`=values(`stock_increment`),`houduan_orders`=values(`houduan_orders`),
         `member_wzd`=values(`member_wzd`),`years`=values(`years`),`monthly`=values(`monthly`),
         `members`=values(`members`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table dai_member_resources;
    '''
    executeSqlByConn(sql)


def main():
    df_member_resource = get_member_resource()
    df_member_resource = df_member_resource.fillna(0)
    del_sql()
    save_sql(df_member_resource)


if __name__ == '__main__':
    main()



