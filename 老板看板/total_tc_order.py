# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/7 8:33
# @Author  : diaozhiwei
# @FileName: total_tc_order.py
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


def get_consume():
    sql = '''
    SELECT
        '甜橙直播间' dept_name,
        left(a.dat,7) monthly,
        sum(a.cost)  promotion_budget,
        sum(a.backend_amount) order_amounts_h
    FROM
        tccm_dx.t_app_online_launch a
    GROUP BY left(a.dat,7)
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def get_order():
    sql = '''
    SELECT
        '甜橙直播间' dept_name,
        left(a.create_time,7) monthly,
        count(DISTINCT a.member_id) fans,
        sum(a.order_amount) order_amounts_q
    FROM
        t_online_retailer_plan_log a
    LEFT JOIN sys_user b on a.member_id=b.user_id
    LEFT JOIN t_online_retailer_plan c on a.online_retailer_plan_id=c.id
    LEFT JOIN sys_dept d on b.dept_id=d.dept_id
    where a.tenant_id=12
    and customer_exists=0
    and a.create_time>='2023-01-01'
    and order_amount>0
    GROUP BY left(a.create_time,7) 
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
     INSERT INTO `jn_tc_fans_total` 
     (
     `dept_name`,`monthly`,`promotion_budget`,`fans`,`fans_price`,`order_amounts_q`,`order_amounts_h`,
     `order_amounts`,`amounts_ratio`
     )
     VALUES (
     %s,%s,%s,%s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name`= VALUES(`dept_name`),`monthly`= VALUES(`monthly`),`promotion_budget`= VALUES(`promotion_budget`),
         `fans`= VALUES(`fans`),`fans_price`= VALUES(`fans_price`),`order_amounts_q`= VALUES(`order_amounts_q`),
         `order_amounts_h`= VALUES(`order_amounts_h`),
         `order_amounts`= VALUES(`order_amounts`),`amounts_ratio`= VALUES(`amounts_ratio`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def del_sql():
    sql = '''
    truncate table jn_tc_fans_total;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 直播间消耗
    df_consume = get_consume()
    # 直播间产出
    df_order = get_order()
    df_consume = df_consume.merge(df_order, on=['dept_name', 'monthly'], how='left')
    df_consume = df_consume
    df_consume = df_consume.fillna(0)
    df_consume['order_amounts'] = df_consume['order_amounts_q'] + df_consume['order_amounts_h']
    df_consume['fans_price'] = df_consume['promotion_budget'] / df_consume['fans']
    df_consume['amounts_ratio'] = df_consume['order_amounts'] / df_consume['promotion_budget']
    df_consume = df_consume[
        ['dept_name', 'monthly', 'promotion_budget', 'fans', 'fans_price', 'order_amounts_q', 'order_amounts_h',
         'order_amounts', 'amounts_ratio']]
    df_consume = df_consume.replace([np.inf, -np.inf], np.nan)
    df_consume = df_consume.fillna(0)
    del_sql()
    save_sql(df_consume)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'wangkai', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 时间
    main()
