# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:20
# @Author  : diaozhiwei
# @FileName: demo_target_day.py
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


# 增量
def get_stock_target():
    sql = '''
    SELECT
        a.id,
        a.dept_name1,
        a.dept_name2 dept_name,
        years,
        monthly,
        a.stock_increment,
        a.target_amount
    FROM
        t_target_day a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 增量销售数据
def get_member_stock_order(monthly, st, et, st0, et0):
    sql = '''
    SELECT 
        a.dept_name1,
        a.dept_name,
        '2023' years,
        '{}' monthly,
        '增量' stock_increment,
        sum(a.order_amount) complate_amount
    FROM 
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.order_amount>40
    and a.first_time>='{}'
    and a.first_time<'{}'
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name1,dept_name,years,monthly,stock_increment
    '''.format(monthly, st, et, st0, et0)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 存量销售
def get_member_increment_order(monthly, st, st0, et0):
    sql = '''
    SELECT 
        a.dept_name1,
        a.dept_name,
        '2023' years,
        '{}' monthly,
        '存量' stock_increment,
        sum(a.order_amount) complate_amount
    FROM 
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.order_amount>40
    and a.first_time<'{}'
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name1,dept_name,years,monthly,stock_increment
    '''.format(monthly, st, st0, et0)
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


def save_sql(df):
    sql = '''
     INSERT INTO `t_target_day` 
     (`id`,`dept_name1`,`dept_name`,`years`,`monthly`,`stock_increment`,
     `target_amount`,`complate_amount`,`amount_rate`
     )
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name`= VALUES(`dept_name`),`years`= VALUES(`years`),`monthly`= VALUES(`monthly`),
         `stock_increment`=VALUES(`stock_increment`),`target_amount`=VALUES(`target_amount`),`complate_amount`=values(`complate_amount`),
         `amount_rate`=values(`amount_rate`)
     '''
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_target_day;
    '''
    hhx_sql3.executeSqlByConn(sql)


def main():
    df_stock_increment_target = get_stock_target()
    # 增量完成业绩
    df_member_stock_order1 = get_member_stock_order(monthly1, st1, st2, st1, st2)
    df_member_stock_order2 = get_member_stock_order(monthly2, st1, st3, st2, st3)
    df_member_stock_order3 = get_member_stock_order(monthly3, st1, st4, st3, st4)
    df_member_stock_order4 = get_member_stock_order(monthly4, st1, st5, st4, st5)
    df_member_stock_order5 = get_member_stock_order(monthly5, st1, st6, st5, st6)
    df_member_stock_order6 = get_member_stock_order(monthly6, st1, st7, st6, st7)
    df_member_stock_order7 = get_member_stock_order(monthly7, st1, st8, st7, st8)
    df_member_stock_order8 = get_member_stock_order(monthly8, st1, st9, st8, st9)
    df_member_stock_order9 = get_member_stock_order(monthly9, st1, st10, st9, st10)
    df_member_stock_order10 = get_member_stock_order(monthly10, st1, st11, st10, st11)
    df_member_stock_order11 = get_member_stock_order(monthly11, st1, st12, st11, st12)
    df_member_stock_order12 = get_member_stock_order(monthly12, st1, st13, st12, st13)
    df_member_stock_order = pd.concat(
        [df_member_stock_order1, df_member_stock_order2, df_member_stock_order3, df_member_stock_order4,
         df_member_stock_order5, df_member_stock_order6, df_member_stock_order7, df_member_stock_order8,
         df_member_stock_order9, df_member_stock_order10, df_member_stock_order11, df_member_stock_order12])
    # 存量完成业绩
    df_member_increment_order1 = get_member_increment_order(monthly1, st1, st1, st2)
    df_member_increment_order2 = get_member_increment_order(monthly2, st1, st2, st3)
    df_member_increment_order3 = get_member_increment_order(monthly3, st1, st3, st4)
    df_member_increment_order4 = get_member_increment_order(monthly4, st1, st4, st5)
    df_member_increment_order5 = get_member_increment_order(monthly5, st1, st5, st6)
    df_member_increment_order6 = get_member_increment_order(monthly6, st1, st6, st7)
    df_member_increment_order7 = get_member_increment_order(monthly7, st1, st7, st8)
    df_member_increment_order8 = get_member_increment_order(monthly8, st1, st8, st9)
    df_member_increment_order9 = get_member_increment_order(monthly9, st1, st9, st10)
    df_member_increment_order10 = get_member_increment_order(monthly10, st1, st10, st11)
    df_member_increment_order11 = get_member_increment_order(monthly11, st1, st11, st12)
    df_member_increment_order12 = get_member_increment_order(monthly12, st1, st12, st13)
    df_member_increment_order = pd.concat([
        df_member_increment_order1, df_member_increment_order2, df_member_increment_order3, df_member_increment_order4,
        df_member_increment_order5, df_member_increment_order6, df_member_increment_order7, df_member_increment_order8,
        df_member_increment_order9, df_member_increment_order10, df_member_increment_order11,
        df_member_increment_order12
    ])
    df_member_order = pd.concat([df_member_stock_order, df_member_increment_order])
    df_stock_increment_target = df_stock_increment_target.merge(df_member_order, on=['dept_name1', 'dept_name','years', 'monthly',
                                                                                     'stock_increment'], how='left')
    df_stock_increment_target = df_stock_increment_target.fillna(0)
    df_stock_increment_target['amount_rate'] = df_stock_increment_target['complate_amount'] / df_stock_increment_target[
        'target_amount']
    df_stock_increment_target = df_stock_increment_target[
        ['id', 'dept_name1','dept_name', 'years', 'monthly', 'stock_increment', 'target_amount', 'complate_amount', 'amount_rate']]
    df_stock_increment_target = df_stock_increment_target.replace([np.inf, -np.inf], np.nan)
    df_stock_increment_target = df_stock_increment_target.fillna(0)
    print(df_stock_increment_target)
    del_sql()
    df_stock_increment_target['dept_name1'] = df_stock_increment_target.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_stock_increment_target['dept_name'] = df_stock_increment_target.apply(lambda x: get_dept2(x['dept_name']), axis=1)
    save_sql(df_stock_increment_target)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    monthly1 = '1月'
    monthly2 = '2月'
    monthly3 = '3月'
    monthly4 = '4月'
    monthly5 = '5月'
    monthly6 = '6月'
    monthly7 = '7月'
    monthly8 = '8月'
    monthly9 = '9月'
    monthly10 = '10月'
    monthly11 = '11月'
    monthly12 = '12月'
    st1 = '2023-01-01'
    st2 = '2023-02-01'
    st3 = '2023-03-01'
    st4 = '2023-04-01'
    st5 = '2023-05-01'
    st6 = '2023-06-01'
    st7 = '2023-07-01'
    st8 = '2023-08-01'
    st9 = '2023-09-01'
    st10 = '2023-10-01'
    st11 = '2023-11-01'
    st12 = '2023-12-01'
    st13 = '2024-01-01'
    main()
