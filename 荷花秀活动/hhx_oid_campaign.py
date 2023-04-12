# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 14:57
# @Author  : diaozhiwei
# @FileName: hhx_oid_campaign.py
# @description: 活动期间订单监控
# @update:
"""

from modules.mysql import jnmtMySQL
import pandas as pd
import numpy as np


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
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 员工订单数据
def get_user_oid():
    sql = '''
    SELECT 
        a.sys_user_id,
        a.order_sn,
        sum(a.order_amount) order_amount
    FROM 
    t_orders_middle a
    LEFT JOIN  t_member_middle b on a.member_id=b.member_id
    WHERE a.create_time >= '2023-04-01' 
    AND a.create_time < '2023-04-10'
    GROUP BY a.sys_user_id,a.order_sn
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 订单区间
def get_order_divide(x):
    if 1000 > x >= 0:
        return '1k以下'
    elif 2000 > x >= 1000:
        return '1-2k'
    elif 3000 > x >= 2000:
        return '2-3k'
    elif 4000 > x >= 3000:
        return '3-4k'
    elif 5000 > x >= 4000:
        return '4-5k'
    elif x >= 5000:
        return '5k以上'


def save_sql(df):
    sql = '''
    INSERT INTO `t_oid_campaign` 
     (`id`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`,
     `dept_name2`,`dept_name`,`wechat_nums`,`order_sn`,`order_amount`,
     `order_interval`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `wechat_nums`=values(`wechat_nums`),`order_sn`=values(`order_sn`),`order_amount`=values(`order_amount`),
         `order_interval`=values(`order_interval`),`activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 员工基础信息
    df_user_base = get_user_base()
    # 订单信息
    df_user_oid = get_user_oid()
    df_user_oid_base = df_user_base.merge(df_user_oid, on=['sys_user_id'], how='left')
    df_user_oid_base = df_user_oid_base.fillna(0)
    # 订单区间
    df_user_oid_base['order_interval'] = df_user_oid_base.apply(lambda x: get_order_divide(x['order_amount']), axis=1)
    df_user_oid_base['activity_name'] = '2023年5.1活动'
    df_user_oid_base['id']=df_user_oid_base['sys_user_id'].astype(str)+df_user_oid_base['order_sn'].astype(str)
    df_user_oid_base = df_user_oid_base[
        ['id', 'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
         'wechat_nums', 'order_sn', 'order_amount','order_interval','activity_name']]
    df_user_oid_base=df_user_oid_base
    save_sql(df_user_oid_base)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    st = '2022-12-20'
    st2 = '2023-02-15'
    et = '2023-03-01'
    main()
