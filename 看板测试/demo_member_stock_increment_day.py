# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/11 17:37
# @Author  : diaozhiwei
# @FileName: demo_member_stock_increment_day.py
# @description: 
# @update: 客户增量存量目标，，转化
"""

from datetime import datetime
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd


# 存量客户
def get_member_stock(monthly,st,log_name):
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        '2023' years,
        '{}' monthly,
        '存量' stock_increment,
        a.member_level,
        count(DISTINCT a.member_id) members
    FROM
        t_member_middle_log a
    where a.first_time<'{}'
    and a.log_name='{}'
    and a.dept_name1 !='0'
    GROUP BY a.dept_name,a.member_level
    '''.format(monthly,st,log_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 增量客户
def get_member_increment(monthly, st, et):
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        '2023' years,
        '{}' monthly,
        '增量' stock_increment,
        a.member_level,
        count(DISTINCT a.member_id) members
    FROM
        t_member_middle a
    where a.first_time>='{}'
    and a.first_time<'{}'
    and a.dept_name1 !='0'
    GROUP BY a.dept_name,a.member_level
    '''.format(monthly, st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 增量销售数据
def get_member_stock_order(monthly, log_name, st, et, st0, et0):
    sql = '''
    SELECT 
        a.dept_name,
        '2023' years,
        '{}' monthly,
        '增量' stock_increment,
        b.member_level,
        count(DISTINCT a.member_id) member_develop,
        sum(a.order_amount) member_order
    FROM 
        t_orders_middle a
    LEFT JOIN  t_member_middle_log b on a.member_id=b.member_id and b.log_name='{}'
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.order_amount>40
    and a.first_time>='{}'
    and a.first_time<'{}'
    and a.create_time>='{}'
    and a.create_time<'{}'
    and a.dept_name1 !='0'
    GROUP BY a.dept_name,b.member_level
    '''.format(monthly, log_name, st, et, st0, et0)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 存量销售
def get_member_increment_order(monthly, log_name, st, st0, et0):
    sql = '''
    SELECT 
        a.dept_name,
        '2023' years,
        '{}' monthly,
        '存量' stock_increment,
        b.member_level,
        count(DISTINCT a.member_id) member_develop,
        sum(a.order_amount) member_order
    FROM 
        t_orders_middle a
    LEFT JOIN  t_member_middle_log b on a.member_id=b.member_id and b.log_name='{}'
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.order_amount>40
    and a.first_time<'{}'
    and a.create_time>='{}'
    and a.create_time<'{}'
    and a.dept_name1 !='0'
    GROUP BY a.dept_name,b.member_level
    '''.format(monthly, log_name, st, st0, et0)
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
     INSERT INTO `t_member_stock_increment_day` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`years`,
     `monthly`,`stock_increment`,`member_level`,`members`,`member_develop`,
     `member_order`,`member_rate`,`member_price`,`member_develop_price`
     )
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`= VALUES(`dept_name`),
         `years`=VALUES(`years`),`monthly`=VALUES(`monthly`),`stock_increment`=values(`stock_increment`),
         `member_level`=values(`member_level`),`members`=values(`members`),
         `member_develop`=values(`member_develop`), `member_order`=values(`member_order`),
         `member_rate`=values(`member_rate`),`member_price`=values(`member_price`),`member_develop_price`=values(`member_develop_price`)
     '''
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_member_stock_increment_day;
    '''
    hhx_sql3.executeSqlByConn(sql)


def main():
    # 存量客户
    df_member_stock1 = get_member_stock(monthly1, st1, log_name1)
    df_member_stock2 = get_member_stock(monthly2, st1, log_name2)
    df_member_stock3 = get_member_stock(monthly3, st1, log_name3)
    df_member_stock4 = get_member_stock(monthly4, st1, log_name4)
    df_member_stock5 = get_member_stock(monthly5, st1, log_name5)
    df_member_stock6 = get_member_stock(monthly6, st1, log_name6)
    # 增量客户
    df_member_increment1 = get_member_increment(monthly1, st1, st2)
    df_member_increment2 = get_member_increment(monthly2, st1, st3)
    df_member_increment3 = get_member_increment(monthly3, st1, st4)
    df_member_increment4 = get_member_increment(monthly4, st1, st5)
    df_member_increment5 = get_member_increment(monthly5, st1, st6)
    # df_member_increment6 = get_member_increment(monthly1, st1, st7)
    # 汇总
    df_member_stock = pd.concat(
        [df_member_stock1, df_member_stock2, df_member_stock3, df_member_stock4, df_member_stock5])
    df_member_increment = pd.concat(
        [df_member_increment1, df_member_increment2, df_member_increment3, df_member_increment4, df_member_increment5])
    df_member = pd.concat([df_member_stock, df_member_increment])
    df_member = df_member.fillna(0)
    # 增量销售
    df_member_stock_order1 = get_member_stock_order(monthly1, log_name2, st1, st2, st1, st2)
    df_member_stock_order2 = get_member_stock_order(monthly2, log_name3, st1, st3, st2, st3)
    df_member_stock_order3 = get_member_stock_order(monthly3, log_name4, st1, st4, st3, st4)
    df_member_stock_order4 = get_member_stock_order(monthly4, log_name5, st1, st5, st4, st5)
    df_member_stock_order5 = get_member_stock_order(monthly5, log_name6, st1, st6, st5, st6)
    # df_member_stock_order6 = get_member_stock_order(monthly6, log_name6, st1, st7, st6, st7)
    # df_member_stock_order7 = get_member_stock_order(monthly7, log_name7, st1, st8, st7, st8)
    # 存量销售
    df_member_increment_order1 = get_member_increment_order(monthly1, log_name2, st1, st1, st2)
    df_member_increment_order2 = get_member_increment_order(monthly2, log_name3, st1, st2, st3)
    df_member_increment_order3 = get_member_increment_order(monthly3, log_name4, st1, st3, st4)
    df_member_increment_order4 = get_member_increment_order(monthly4, log_name5, st1, st4, st5)
    df_member_increment_order5 = get_member_increment_order(monthly5, log_name6, st1, st5, st6)
    # df_member_increment_order6 = get_member_increment_order(monthly6, log_name1, st1, st6, st7)
    # df_member_increment_order7 = get_member_increment_order(monthly7, log_name1, st1, st7, st8)
    df_member_stock_order = pd.concat(
        [df_member_stock_order1, df_member_stock_order2, df_member_stock_order3, df_member_stock_order4,
         df_member_stock_order5])
    df_member_increment_order = pd.concat(
        [df_member_increment_order1, df_member_increment_order2, df_member_increment_order3, df_member_increment_order4,
         df_member_increment_order5])
    df_member_order = pd.concat([df_member_stock_order, df_member_increment_order])
    df_member_order['years'] = df_member_order['years'].astype(str)
    df_member_order = df_member_order.fillna(0)
    df_member = df_member.merge(df_member_order,on=['dept_name', 'years', 'monthly', 'stock_increment', 'member_level'], how='left')
    # 转化率
    df_member['member_rate'] = df_member['member_develop'] / df_member['members']*0.412
    # 客单价
    df_member['member_price'] = df_member['member_order'] / df_member['member_develop']*12
    # 单产
    df_member['member_develop_price'] = df_member['member_order'] / df_member['members']*9.4123
    df_member = df_member.fillna(0)
    df_member['id'] = df_member['dept_name'] + df_member['years'] + df_member['monthly'] + df_member[
        'stock_increment'] + df_member['member_level']
    df_member = df_member[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'years', 'monthly', 'stock_increment', 'member_level',
         'members', 'member_develop', 'member_order', 'member_rate', 'member_price', 'member_develop_price']]

    df_member['dept_name1'] = df_member.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_member['dept_name'] = df_member.apply(lambda x: get_dept2(x['dept_name']), axis=1)

    # del_sql()
    save_sql(df_member)


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
    log_name1 = '2023年1月初客户等级'
    log_name2 = '2023年2月初客户等级'
    log_name3 = '2023年3月初客户等级'
    log_name4 = '2023年4月初客户等级'
    log_name5 = '2023年5月初客户等级'
    log_name6 = '2023年6月初客户等级'
    log_name7 = '2023年7月初客户等级'
    log_name8 = '2023年8月初客户等级'
    log_name9 = '2023年9月初客户等级'
    log_name10 = '2023年10月初客户等级'
    log_name11 = '2023年11月初客户等级'
    log_name12 = '2023年12月初客户等级'
    main()








