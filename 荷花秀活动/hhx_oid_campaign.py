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
    and a.dept_name1 not in ('0')
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
    # 状态
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.activity_name='{}'
    and a.order_amount>40
    GROUP BY a.sys_user_id,a.order_sn
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 订单区间
# 光辉前端

def get_order_divide(x):
    if 700 > x >= 0:
        return '0-700'
    elif 1000 > x >= 700:
        return '700-1k'
    elif 2000 > x >= 1000:
        return '1-2k'
    elif x >= 2000:
        return '2k以上'
'''
7000以上
4000-7000
3000-4000
2000-3000
900-2000
900以下
'''


# 光辉后端
def get_order_divide2(x):
    if 900 > x >= 0:
        return '0-900'
    elif 2000 > x >= 900:
        return '900-2k'
    elif 3000 > x >= 2000:
        return '2-3k'
    elif 4000 > x >= 3000:
        return '3-4k'
    elif 7000 > x >= 4000:
        return '4-7k'
    elif x >= 7000:
        return '7k以上'


# 光华前端
def get_order_divide3(x):
    if 700 > x >= 0:
        return '0-700'
    elif 700 > x >= 1000:
        return '700-1k'
    elif 1000 > x >= 2000:
        return '1-2k'
    elif 4000 > x >= 3000:
        return '3-4k'
    elif 5000 > x >= 4000:
        return '4-5k'
    elif x >= 5000:
        return '5k以上'


# 光华后端
def get_order_divide4(x):
    if 700 > x >= 0:
        return '0-700'
    elif 700 > x >= 1000:
        return '700-1k'
    elif 1000 > x >= 2000:
        return '1-2k'
    elif 4000 > x >= 3000:
        return '3-4k'
    elif 5000 > x >= 4000:
        return '4-5k'
    elif x >= 5000:
        return '5k以上'


# 光芒
def get_order_divide5(x):
    if 700 > x >= 0:
        return '0-700'
    elif 700 > x >= 1000:
        return '700-1k'
    elif 1000 > x >= 2000:
        return '1-2k'
    elif 4000 > x >= 3000:
        return '3-4k'
    elif 5000 > x >= 4000:
        return '4-5k'
    elif x >= 5000:
        return '5k以上'


# 蜂蜜
def get_order_divide6(x):
    if 700 > x >= 0:
        return '0-700'
    elif 700 > x >= 1000:
        return '700-1k'
    elif 1000 > x >= 2000:
        return '1-2k'
    elif 4000 > x >= 3000:
        return '3-4k'
    elif 5000 > x >= 4000:
        return '4-5k'
    elif x >= 5000:
        return '5k以上'


# 海参
def get_order_divide7(x):
    if 700 > x >= 0:
        return '0-700'
    elif 700 > x >= 1000:
        return '700-1k'
    elif 1000 > x >= 2000:
        return '1-2k'
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


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_oid_campaign;
    '''
    hhx_sql2.executeSqlByConn(sql)


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
    del_sql()
    save_sql(df_user_oid_base)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    activity_name = '2023年五一活动'
    main()





