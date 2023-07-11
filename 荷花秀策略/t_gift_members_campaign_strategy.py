# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/7 17:36
# @Author  : diaozhiwei
# @FileName: t_gift_members_campaign_strategy.py
# @description: 
# @update:
"""
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from dateutil.relativedelta import relativedelta


def get_gift_total():
    sql='''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.nick_name,
        MONTH(a.create_time) monthly,
        count(1) presents
    FROM
        t_orders_middle a
    where  a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.create_time>='2023-01-01'
    and a.order_amount=0
    and a.no_performance_type not in ('积分总换','订单补发','客户转移','换货订单')
    GROUP BY a.dept_name,a.nick_name,MONTH(a.create_time)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_order():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.nick_name,
        a.member_id,
        a.create_time
    FROM
        t_orders_middle a
    where  a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.create_time>='2023-01-01'
    and a.order_amount=0
    and a.no_performance_type not in ('积分总换','订单补发','客户转移','换货订单')
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_order_campaign():
    sql = '''
    SELECT
        a.member_id,
        a.create_time create_time2,
        a.order_amount,
        a.activity_name
    FROM
        t_orders_middle a
    where  a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.activity_name in ('2023年38女神节活动','2023年五一活动','2023年618活动')
    and a.order_amount>40
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_gift_members_campaign_strategy` 
     (
     `id`,`dept_name1`,`dept_name2`,`dept_name`,`nick_name`,
     `monthly`,`presents`,`activity_name`,`members`,`members_ratio`,
     `order_amount`,`amount_price`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `nick_name`=values(`nick_name`),`monthly`=values(`monthly`),`presents`=values(`presents`),
         `activity_name`=values(`activity_name`),`members`=values(`members`),`members_ratio`=values(`members_ratio`),
         `order_amount`=values(`order_amount`),`amount_price`=values(`amount_price`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 员工送礼次数
    df_gift_total = get_gift_total()
    # 送礼时间
    df_order = get_order()
    # 活动激活
    df_order_campaign = get_order_campaign()
    df_order = df_order.merge(df_order_campaign, on=['member_id'], how='left')
    df_order = df_order
    df_order['time_diff'] = ((pd.to_datetime(df_order['create_time']) - pd.to_datetime(df_order['create_time2']))/pd.Timedelta(1, 'D')).fillna(0).astype(int)
    # 数据筛选，激活客户
    df_order = df_order[df_order['time_diff'] < 0]
    df_order['monthly'] = df_order['create_time'].dt.month
    # 数据汇总
    df_order1 = df_order.groupby(['nick_name', 'monthly', 'activity_name'])['member_id'].count().reset_index()
    df_order2 = df_order.groupby(['nick_name', 'monthly', 'activity_name'])['order_amount'].sum().reset_index()
    # 数据关联
    df_gift_total = df_gift_total.merge(df_order1, on=['nick_name', 'monthly'], how='left')
    df_gift_total = df_gift_total.merge(df_order2, on=['nick_name', 'monthly', 'activity_name'], how='left')
    df_gift_total = df_gift_total.rename(columns={'member_id': 'members'})
    df_gift_total = df_gift_total.fillna(0)
    df_gift_total['members_ratio'] = df_gift_total['members'] / df_gift_total['presents']
    df_gift_total['amount_price'] = df_gift_total['order_amount'] / df_gift_total['members']
    df_gift_total['id']=df_gift_total['dept_name']+df_gift_total['nick_name']+df_gift_total['monthly'].astype(str)+df_gift_total['activity_name'].astype(str)
    df_gift_total = df_gift_total.fillna(0)
    df_gift_total = df_gift_total[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'nick_name', 'monthly', 'presents', 'activity_name', 'members',
         'members_ratio', 'order_amount', 'amount_price']]
    save_sql(df_gift_total)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()
