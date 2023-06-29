# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/8 17:27
# @Author  : diaozhiwei
# @FileName: demo_nickname_rank_total.py
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


# 员工基础信息
def get_base():
    sql = '''
    SELECT
        a.nick_name,
        a.dept_name1,
        a.dept_name
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    GROUP BY a.nick_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_order(st, et):
    sql = '''
    SELECT
        a.dept_name,
        a.nick_name,
        count(1) orders,
        sum(a.order_amount) orders_amount 
    FROM
        t_orders_middle a 
    WHERE a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.order_amount>40
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name2,a.nick_name
    '''.format(st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
     INSERT INTO `t_nickname_rank_total` 
     (
     `dept_name1`,`dept_name`,`nick_name`,`weekly_order`,
     `weekly_amount`,`weekly_rank`,`monthly_order`,`monthly_amount`,`monthly_rank`
     )
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name`= VALUES(`dept_name`),`nick_name`= VALUES(`nick_name`),
         `weekly_order`= VALUES(`weekly_order`),`weekly_amount`= VALUES(`weekly_amount`),`weekly_rank`= VALUES(`weekly_rank`),
         `monthly_order`= VALUES(`monthly_order`),`monthly_amount`= VALUES(`monthly_amount`),`monthly_rank`= VALUES(`monthly_rank`)
     '''
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


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


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_nickname_rank_total;
    '''
    hhx_sql3.executeSqlByConn(sql)


def main():
    # 员工信息
    df_nick_name = get_base()
    # 销售数据
    # 本周
    df_order_weekly = get_order(st, et)
    # 本月
    df_order_monthly = get_order(st2, et)
    df_nick_name = df_nick_name.merge(df_order_weekly, on=['dept_name', 'nick_name'], how='left')
    df_nick_name = df_nick_name.merge(df_order_monthly, on=['dept_name', 'nick_name'], how='left')
    df_nick_name = df_nick_name.rename(
        columns={'orders_x': 'weekly_order', 'orders_amount_x': 'weekly_amount', 'orders_y': 'monthly_order',
                 'orders_amount_y': 'monthly_amount'})
    df_nick_name = df_nick_name.fillna(0)
    df_nick_name['weekly_rank'] = df_nick_name.groupby(['dept_name'])['weekly_amount'].rank(method='dense',
                                                                                             ascending=False)
    df_nick_name['monthly_rank'] = df_nick_name.groupby(['dept_name'])['monthly_amount'].rank(method='dense',
                                                                                               ascending=False)
    df_nick_name['dept_name1'] = df_nick_name.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_nick_name['dept_name'] = df_nick_name.apply(lambda x: get_dept2(x['dept_name']), axis=1)
    df_nick_name['nick_name'] = df_nick_name['nick_name'].str.split('').apply(lambda x: x[1]) + '三'
    df_nick_name = df_nick_name[
        ['dept_name1', 'dept_name', 'nick_name', 'weekly_order', 'weekly_amount', 'weekly_rank', 'monthly_order',
         'monthly_amount', 'monthly_rank']]


    print(df_nick_name)
    del_sql()
    save_sql(df_nick_name)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    now = datetime.now().date()
    # 本周
    st = now - timedelta(days=now.weekday())
    # 本月
    st2 = datetime(now.year, now.month, 1)
    et = now
    print(st, st2, et)
    main()
