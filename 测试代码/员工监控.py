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


# 成交客户数，未成交客户数
def get_user_fans():
    sql = '''
    SELECT
        a.sys_user_id,
        sum(a.members) fans
    FROM
        t_pred_campaign a
    where a.member_category in ('add_fans','new_fans','old_fans')
    GROUP BY a.sys_user_id
    '''
    df = get_DataFrame_PD2(sql)
    return df


# 成交客户数
def get_user_member():
    sql = '''
    SELECT
    a.sys_user_id,
    sum(a.members) members
    FROM
        t_pred_campaign a
    where a.member_category in ('V1','V2','V3','V4','V5')
    GROUP BY a.sys_user_id
    '''
    df = get_DataFrame_PD2(sql)
    return df


# 未成交开发
def get_user_fans_develop():
    sql = '''
    SELECT 
        a.sys_user_id,
        count(DISTINCT a.member_id) fans_develop
    FROM 
        t_orders_middle a 
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.activity_name='{}'
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    GROUP BY a.sys_user_id
    '''.format(activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 复购客户
def get_user_member_develop():
    sql = '''
    SELECT 
        a.sys_user_id,
        count(DISTINCT a.member_id) members_develop
    FROM 
        t_orders_middle a 
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.activity_name='{}'
    and a.clinch_type in ('复购日常成交','复购活动成交')
    GROUP BY a.sys_user_id
    '''.format(activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 金额
def get_user_amount():
    sql = '''
    SELECT 
        a.sys_user_id,
        sum(a.order_amount) members_amount
    FROM 
        t_orders_middle a
    LEFT JOIN  t_member_middle b on a.member_id=b.member_id
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.activity_name='{}'
    GROUP BY a.sys_user_id
    '''.format(activity_name)
    df = get_DataFrame_PD2(sql)
    return df


def get_amount_divide(x):
    if 20000 > x >= 0:
        return '0-2W区间'
    elif 50000 > x >= 20000:
        return '2-5W区间'
    elif 100000 > x >= 50000:
        return '5-10W区间'
    elif 150000 > x >= 100000:
        return '10-15W区间'
    elif 200000 > x >= 150000:
        return '15-20W区间'
    elif 300000 > x >= 200000:
        return '20-30W区间'
    elif x >= 300000:
        return '30W以上'


def save_sql(df):
    sql = '''
    INSERT INTO `t_user_campaign` 
     (`id`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`,
     `dept_name2`,`dept_name`,`wechat_nums`,`fans`,`members`,
     `fans_develop`,`members_develop`,`fans_develop_rate`,`member_develop_rate`,`members_amount`,
     `amount_range`,`member_price`,`member_develop_price`,`member_price_rank`,`amount_develop_rank`,
     `activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s
     )
     ON DUPLICATE KEY UPDATE
         `sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `wechat_nums`=values(`wechat_nums`),`fans`=values(`fans`),`members`=values(`members`),
         `fans_develop`=values(`fans_develop`),`members_develop`=values(`members_develop`),`fans_develop_rate`=values(`fans_develop_rate`),
         `member_develop_rate`=values(`member_develop_rate`),`members_amount`=values(`members_amount`),`amount_range`=values(`amount_range`),
         `member_price`=values(`member_price`),`member_develop_price`=values(`member_develop_price`),`member_price_rank`=values(`member_price_rank`),
         `amount_develop_rank`=values(`amount_develop_rank`),`activity_name`=values(`activity_name`)
         '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 员工基础信息
    df_user_base = get_user_base()
    # 未成交客户数
    df_user_fans = get_user_fans()
    # 成交客户数
    df_user_member = get_user_member()
    # 未成交客户开发
    df_user_fans_develop = get_user_fans_develop()
    # 成交客户开发
    df_user_member_develop = get_user_member_develop()
    # 消费金额
    df_user_amount = get_user_amount()
    df_user_base = df_user_base.merge(df_user_fans, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.merge(df_user_member, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.merge(df_user_fans_develop, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.merge(df_user_member_develop, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.merge(df_user_amount, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.fillna(0)
    # 开发率
    df_user_base['fans_develop_rate']=df_user_base['fans_develop']/df_user_base['fans']
    # 复购率
    df_user_base['member_develop_rate']=df_user_base['members_develop']/df_user_base['members']
    # 开发金额区间
    df_user_base['amount_range']=df_user_base.apply(lambda x:get_amount_divide(x['members_amount']),axis=1)
    # 单产
    df_user_base['member_price']=df_user_base['members_amount']/df_user_base['members']
    # 客单价
    df_user_base['member_develop_price']=df_user_base['members_amount']/(df_user_base['fans_develop']+df_user_base['members_develop'])
    df_user_base['activity_name'] = '2023年5.1活动'
    df_user_base = df_user_base.replace([np.inf, -np.inf], np.nan)
    df_user_base = df_user_base.fillna(0)
    # 单产排名
    df_user_base['member_price_rank'] = df_user_base.groupby(['dept_name1'])['member_price'].rank(method='dense')
    # 业绩排名
    df_user_base['amount_develop_rank'] = df_user_base.groupby(['dept_name1'])['members_amount'].rank(method='dense')
    df_user_base['id']=df_user_base['sys_user_id']
    df_user_base = df_user_base[['id', 'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
                                 'wechat_nums', 'fans', 'members', 'fans_develop',
                                 'members_develop', 'fans_develop_rate', 'member_develop_rate', 'members_amount',
                                 'amount_range', 'member_price', 'member_develop_price', 'member_price_rank',
                                 'amount_develop_rank', 'activity_name']]
    df_user_base=df_user_base
    save_sql(df_user_base)


if __name__ == '__main__':
    activity_name = '2023年38女神节活动'
    main()
