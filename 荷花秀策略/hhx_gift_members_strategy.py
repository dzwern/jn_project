# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/7 14:31
# @Author  : diaozhiwei
# @FileName: hhx_gift_members_strategy.py
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


def get_member():
    sql = '''
    SELECT
        a.member_id,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.nick_name,
        min(a.first_time) first_time,
        max(a.create_time) create_time,
        count(1) gift_orders
    FROM
        t_gift_orders_strategy a
    GROUP BY a.member_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_order():
    sql = '''
    SELECT
        a.member_id,
        a.order_nums orders,
        a.order_amounts  order_amount
    FROM
        t_member_middle a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_gift_members_strategy` 
     (
     `id`,`member_id`,`dept_name1`,`dept_name2`,`dept_name`,
     `nick_name`,`first_time`,`create_time`,`gift_orders`,`orders`,
     `order_amount`,`member_price`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `member_id`= VALUES(`member_id`),`dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),
         `dept_name`=values(`dept_name`),`nick_name`=values(`nick_name`),`first_time`=values(`first_time`),
         `create_time`=values(`create_time`),`gift_orders`=values(`gift_orders`),`orders`=values(`orders`),
         `order_amount`=values(`order_amount`),`member_price`=values(`member_price`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 送礼客户
    df_member = get_member()
    # 历史销售数据
    df_order = get_order()
    df_member = df_member.merge(df_order, on=['member_id'], how='left')
    df_member['member_price'] = df_order['order_amount'] / df_order['orders']
    df_member['id'] = df_order['member_id']
    df_member = df_member[
        ['id', 'member_id', 'dept_name1', 'dept_name2', 'dept_name', 'nick_name', 'first_time', 'create_time',
         'gift_orders','orders', 'order_amount', 'member_price']]
    df_member = df_member.fillna(0)
    print(df_member)
    save_sql(df_member)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()
