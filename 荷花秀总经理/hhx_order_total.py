# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/8 17:26
# @Author  : diaozhiwei
# @FileName: hhx_order_total.py
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
        a.dept_name,
        a.dept_name1,
        a.dept_name2,
        a.tenant_id tenant_id2
    FROM
        t_dept_tmp a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_fans():
    sql = '''
    SELECT
        f.dept_name,
        a.tenant_id,
        sum(a.credit) fans 
    FROM t_wechat_fans_log a
    LEFT JOIN t_wechat d on d.id=a.wechat_id
    LEFT JOIN sys_user e on e.user_id=d.sys_user_id
    LEFT JOIN sys_dept f on e.dept_id=f.dept_id
    where a.tenant_id in ( '25', '26', '27', '28' ) 
    and a.new_sprint_time>='{}'
    and a.new_sprint_time<'{}'
    and a.credit>0
    GROUP BY f.dept_name,a.tenant_id
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def get_order(st, et):
    sql = '''
    select 
        a.dept_name,
        count(1) orders,
        sum(a.order_amount) orders_amount 
    FROM
        t_orders_middle a 
    WHERE a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.order_amount>40
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name
    '''.format(st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_target():
    sql = '''
    SELECT
        a.dept_name2,
        sum(a.target_amount) target_amount
    FROM
        t_target_day a
    where a.monthly='{}'
    GROUP BY a.dept_name2
    '''.format(monthly)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
     INSERT INTO `t_order_total` 
     (
     `dept_name1`,`dept_name2`,`fans`,`day_order`,
     `day_amount`,`weekly_order`,`weekly_amount`,`monthly_order`,`monthly_amount`,
     `target_amount`,`target_rate`
     )
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`fans`= VALUES(`fans`),
         `day_order`= VALUES(`day_order`),`day_amount`= VALUES(`day_amount`),`weekly_order`= VALUES(`weekly_order`),
         `weekly_amount`= VALUES(`weekly_amount`),`monthly_order`= VALUES(`monthly_order`),`monthly_amount`= VALUES(`monthly_amount`),
         `target_amount`= VALUES(`target_amount`),`target_rate`= VALUES(`target_rate`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_order_total;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 部门信息
    df_name_base = get_base()
    # 进粉
    df_fans = get_fans()
    # 筛选进粉
    df_name_base = df_name_base.merge(df_fans, on=['dept_name'], how='left')
    df_name_base = df_name_base.fillna(0)
    df_name_base['fuzhu'] = df_name_base['tenant_id2'] - df_name_base['tenant_id']
    df_name_base = df_name_base.loc[df_name_base['fuzhu'] == 0, :]
    # 销售
    df_order_day = get_order(st, et)
    df_order_weekly = get_order(st2, et)
    df_order_monthly = get_order(st3, et)
    df_name_base = df_name_base.merge(df_order_day, on=['dept_name'], how='left')
    df_name_base = df_name_base.merge(df_order_weekly, on=['dept_name'], how='left')
    df_name_base = df_name_base.merge(df_order_monthly, on=['dept_name'], how='left')
    df_name_base = df_name_base.rename(
        columns={'orders_x': 'day_order', 'orders_y': 'weekly_order', 'orders': 'monthly_order',
                 'orders_amount_x': 'day_amount', 'orders_amount_y': 'weekly_amount',
                 'orders_amount': 'monthly_amount'})
    df_name_base = df_name_base[
        ['dept_name1', 'dept_name2', 'fans', 'day_order', 'day_amount', 'weekly_order', 'weekly_amount',
         'monthly_order', 'monthly_amount']]
    df_name_base = df_name_base.fillna(0)
    df_name_base = df_name_base.groupby(['dept_name1', 'dept_name2'])[
        'fans', 'day_order', 'day_amount', 'weekly_order', 'weekly_amount', 'monthly_order', 'monthly_amount'].sum().reset_index()
    # 目标
    df_target = get_target()
    df_name_base = df_name_base.merge(df_target, on=['dept_name2'], how='left')
    df_name_base['target_rate'] = df_name_base['target_amount'] / df_name_base['monthly_amount']
    df_name_base = df_name_base.fillna(0)
    df_name_base = df_name_base[
        ['dept_name1', 'dept_name2', 'fans', 'day_order', 'day_amount', 'weekly_order', 'weekly_amount',
         'monthly_order', 'monthly_amount', 'target_amount', 'target_rate']]
    del_sql()
    save_sql(df_name_base)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    st = '2023-05-01'
    st2 = '2023-05-10'
    st3 = '2023-05-15'
    et = '2023-06-01'
    monthly = '5月'
    main()



