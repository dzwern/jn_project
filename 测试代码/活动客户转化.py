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


# 员工基础信息
def get_user_base():
    sql = '''
    SELECT
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.wechat_id) wechat_nums
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    and a.wechat_name not in ('玫瑰诗') 
    GROUP BY a.sys_user_id
    '''
    df = get_DataFrame_PD2(sql)
    return df


# 员工粉丝，客户数据源
def get_user_fans():
    sql = '''
    SELECT
        a.sys_user_id,
        a.member_category,
        sum(a.members) fans
    FROM
        t_pred_campaign a
    GROUP BY a.sys_user_id,a.member_category
    '''
    df = get_DataFrame_PD2(sql)
    return df


# 客户成交金额,成交数，粉丝转换，新进粉成交
def get_member_strike():
    sql = '''
    SELECT 
        a.sys_user_id,
        'add_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.first_time>='{}'
    and a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    and a.activity_name='{}'
    GROUP BY a.sys_user_id
    '''.format(st2, et,activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 老粉成交
def get_member_strike2():
    sql = '''
    SELECT 
        a.sys_user_id,
        'old_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE  a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    and a.activity_name='{}'
    GROUP BY a.sys_user_id
    '''.format(st,activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 新粉成交
def get_member_strike3():
    sql = '''
    SELECT 
        a.sys_user_id,
        'new_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.first_time>='{}'
    and a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    and a.activity_name='{}'
    GROUP BY a.sys_user_id
    '''.format(st2, st,activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 客户转换
def get_member_struck():
    sql = '''
    SELECT 
        a.sys_user_id,
        b.member_level member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a
    LEFT JOIN  t_member_middle b on a.member_id=b.member_id
    where a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('复购日常成交','复购活动成交')
    and a.activity_name='{}'
    GROUP BY a.sys_user_id,b.member_level
    ORDER BY a.sys_user_id
    '''.format(activity_name)
    df = get_DataFrame_PD2(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_fans_member_campaign` 
     (`id`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`,
     `dept_name2`,`dept_name`,`wechat_nums`,`member_category`,`fans`,
     `members_develop`,`member_rate`,`members_amount`,`member_price`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `wechat_nums`=values(`wechat_nums`),`member_category`=values(`member_category`),`fans`=values(`fans`),
         `members_develop`=values(`members_develop`),`member_rate`=values(`member_rate`),`members_amount`=values(`members_amount`),
         `member_price`=values(`member_price`),`activity_name`=values(`activity_name`)
         '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础用户
    df_user_base=get_user_base()
    # 用户的客户数
    df_user_fans=get_user_fans()
    df_user_base = df_user_base.merge(df_user_fans, on=['sys_user_id'], how='left')
    # 员工真实消费情况
    df_member_strike = get_member_strike()
    df_member_strike2 = get_member_strike2()
    df_member_strike3 = get_member_strike3()
    df_member_struck = get_member_struck()
    df_member_strike = pd.concat([df_member_strike, df_member_strike2, df_member_strike3, df_member_struck])
    df_user_base = df_user_base.merge(df_member_strike, on=['sys_user_id', 'member_category'], how='left')
    df_user_base = df_user_base.fillna(0)
    # 转化率
    df_user_base['member_rate']=df_user_base['members_develop']/df_user_base['fans']
    # 客单价
    df_user_base['member_price']=df_user_base['members_amount']/df_user_base['members_develop']
    # 活动名称
    df_user_base['activity_name'] = '2023年5.1活动'
    df_user_base = df_user_base.replace([np.inf, -np.inf], np.nan)
    df_user_base = df_user_base.fillna(0)
    df_user_base['id'] = df_user_base['sys_user_id'].astype(str) + df_user_base['member_category'].astype(str)
    df_user_base = df_user_base[['id', 'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
                                 'wechat_nums', 'member_category', 'fans', 'members_develop', 'member_rate',
                                 'members_amount', 'member_price', 'activity_name']]
    df_user_base=df_user_base
    print(df_user_base)
    save_sql(df_user_base)


if __name__ == '__main__':
    st = '2022-02-15'
    st2 = '2023-04-18'
    et = '2023-04-29'
    activity_name = '2023年38女神节活动'
    main()
