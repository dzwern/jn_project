# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:15
# @Author  : diaozhiwei
# @FileName: demo_order_total_day.py
# @description: 整体数据监控，主要指标有客户数，单数，销售额/业绩，进粉数，JS业绩，单产，HX业绩，CC业绩（时间判断），其他业绩，单月复购人数，总复购人数，单月复购率，总复购率
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


# 整体数据销售
def get_order_total():
    sql = '''
     SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.wechat_id,
        a.wechat_name,
        a.wechat_number,
        left(a.create_time,10) create_time,
        year(a.create_time) years,
        QUARTER(a.create_time) quarterly,
        MONTH(a.create_time) monthly,
        WEEKOFYEAR(a.create_time) weekly,
        a.is_activity,
        a.activity_name,
        count(DISTINCT a.member_id) order_member_total,
        sum(a.order_amount) order_amount_total
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    GROUP BY a.dept_name,a.wechat_number,left(a.create_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 日常数据销售
def get_order_day():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.create_time,10) create_time,
        count(DISTINCT a.member_id) order_member_day,
        sum(a.order_amount) order_amount_day
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.is_activity='否'
    GROUP BY a.dept_name,a.wechat_number,left(a.create_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 活动数据销售
def get_order_activity():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.create_time,10) create_time,
        count(DISTINCT a.member_id) order_member_campaign,
        sum(a.order_amount) order_amount_campaign
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.is_activity='是'
    GROUP BY a.dept_name,a.wechat_number,left(a.create_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_dept(x):
    if x == '光辉部':
        return '1部门'
    elif x == '光华部':
        return '2部门'
    elif x == '光源部':
        return '3部门'
    elif x == '光芒部':
        return '4部门'
    else:
        return '1部门'


def get_dept2(x):
    if x == '光华部1组':
        return '小组1'
    elif x == '光华部二组':
        return '小组2'
    elif x == '光华部六组':
        return '小组3'
    elif x == '光华部五组':
        return '小组4'
    elif x == '光华部一组1':
        return '小组5'
    elif x == '光华部三组':
        return '小组6'
    elif x == '光华部七组':
        return '小组7'
    elif x == '光华部一组':
        return '小组8'
    elif x == '光辉部八组':
        return '小组1'
    elif x == '光辉部七组':
        return '小组2'
    elif x == '光辉部三组':
        return '小组3'
    elif x == '光辉部一组':
        return '小组4'
    elif x == '光辉部二组':
        return '小组5'
    elif x == '光辉部五组':
        return '小组6'
    elif x == '光辉部六组':
        return '小组7'
    elif x == '光辉组九组':
        return '小组8'
    elif x == '光芒部二组':
        return '小组1'
    elif x == '光芒部六组':
        return '小组2'
    elif x == '光芒部三组':
        return '小组3'
    elif x == '光芒部一组':
        return '小组4'
    elif x == '光源部蜂蜜八组':
        return '小组1'
    elif x == '光源部蜂蜜九组':
        return '小组2'
    elif x == '光源部蜂蜜四组':
        return '小组3'
    elif x == '光源部蜂蜜五组':
        return '小组4'
    elif x == '光源部海参七组':
        return '小组5'
    else:
        return '小组1'


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `t_order_total_day` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`sys_user_id`,
     `user_name`,`nick_name`,`wechat_id`,`wechat_name`,`wechat_number`,
     `create_time`,`years`,`quarterly`,`monthly`,`weekly`,
     `order_member_total`,`order_amount_total`,`member_price_total`,`order_member_day`,`order_amount_day`,
     `member_price_day`,`order_member_campaign`,`order_amount_campaign`,`member_price_campaign`,`is_activity`,
     `activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `sys_user_id`=values(`sys_user_id`), `user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `wechat_id`=values(`wechat_id`),`wechat_name`=values(`wechat_name`),`wechat_number`=values(`wechat_number`),
         `create_time`=values(`create_time`),`years`=values(`years`),`quarterly`=values(`quarterly`),
         `monthly`=values(`monthly`),`weekly`=values(`weekly`),`order_member_total`=values(`order_member_total`),
         `order_amount_total`=values(`order_amount_total`),`member_price_total`=values(`member_price_total`),`order_member_day`=values(`order_member_day`),
         `order_amount_day`=values(`order_amount_day`),`member_price_day`=values(`member_price_day`),`order_member_campaign`=values(`order_member_campaign`),
         `order_amount_campaign`=values(`order_amount_campaign`),`member_price_campaign`=values(`member_price_campaign`),`is_activity`=values(`is_activity`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 整体销售数据
    df_order_total = get_order_total()
    # 日常销售数据
    df_order_day = get_order_day()
    # 整体销售数据
    df_order_activity = get_order_activity()
    df_order_total = df_order_total.merge(df_order_day, on=['dept_name', 'wechat_number', 'create_time'], how='left')
    df_order_total = df_order_total.merge(df_order_activity, on=['dept_name', 'wechat_number', 'create_time'],
                                          how='left')
    # 总单价
    df_order_total['member_price_total'] = df_order_total['order_amount_total'] / df_order_total[
        'order_member_total'] * 1.32
    # 活动单价
    df_order_total['member_price_campaign'] = df_order_total['order_amount_campaign'] / df_order_total[
        'order_member_campaign'] * 14.31
    # 日常单价
    df_order_total['member_price_day'] = df_order_total['order_amount_day'] / df_order_total[
        'order_member_day'] * 0.981231
    df_order_total = df_order_total.loc[~df_order_total['dept_name1'].isin(['0']), :]
    df_order_total = df_order_total.fillna(0)
    df_order_total['id'] = df_order_total['wechat_number'] + df_order_total['create_time']
    df_order_total = df_order_total[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'sys_user_id', 'user_name', 'nick_name',
         'wechat_id', 'wechat_name', 'wechat_number', 'create_time', 'years', 'quarterly', 'monthly',
         'weekly', 'order_member_total', 'order_amount_total', 'member_price_total', 'order_member_day',
         'order_amount_day', 'member_price_day', 'order_member_campaign', 'order_amount_campaign',
         'member_price_campaign', 'is_activity', 'activity_name']]
    print(df_order_total)
    df_order_total['dept_name1'] = df_order_total.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_order_total['dept_name'] = df_order_total.apply(lambda x: get_dept2(x['dept_name']), axis=1)
    df_order_total['order_amount_total'] = df_order_total['order_amount_total'] * 123.4
    df_order_total['order_member_total'] = df_order_total['order_member_total'] * 0.412
    save_sql(df_order_total)


# 8.141.237.253
if __name__ == '__main__':
    # hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', '8.141.237.253')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', '8.141.237.253')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 开始时间，结束时间
    main()
