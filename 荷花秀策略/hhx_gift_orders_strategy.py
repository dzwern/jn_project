# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/7 14:31
# @Author  : diaozhiwei
# @FileName: hhx_gift_orders_strategy.py
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


def get_order():
    sql = '''
    SELECT
        a.id,
        a.order_sn,
        a.order_type,
        a.no_performance_type,
        a.clinch_type,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.nick_name,
        a.wechat_number,
        a.member_id,
        a.first_time,
        a.create_time,
        a.product_name,
        a.order_amount,
        a.pay_type_name,
        a.order_state 
    FROM
        t_orders_middle a
    LEFT JOIN  t_order_item_middle b on a.order_sn=b.order_sn
    where  a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.create_time>='2023-01-01'
    and a.order_amount=0
    and a.no_performance_type not in ('积分总换','订单补发','客户转移','换货订单')
    GROUP BY a.id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_gift_orders_strategy` 
     (
     `id`,`order_sn`,`order_type`,`no_performance_type`,`clinch_type`,
     `dept_name1`,`dept_name2`,`dept_name`,`nick_name`,`wechat_number`,
     `member_id`,`first_time`,`create_time`,`product_name`,`order_amount`,
     `pay_type_name`,`order_state`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_sn`= VALUES(`order_sn`),`order_type`=VALUES(`order_type`),`no_performance_type`=values(`no_performance_type`),
         `clinch_type`=values(`clinch_type`),`dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),
         `dept_name`=values(`dept_name`),`nick_name`=values(`nick_name`),`wechat_number`=values(`wechat_number`),
         `member_id`=values(`member_id`),`first_time`=values(`first_time`),`create_time`=values(`create_time`),
         `product_name`=values(`product_name`),`order_amount`=values(`order_amount`),`pay_type_name`=values(`pay_type_name`),
         `order_state`=values(`order_state`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def del_sql():
    sql = '''
    truncate table t_gift_orders_strategy;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    df_order = get_order()
    df_order = df_order
    print(df_order)
    save_sql(df_order)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()
