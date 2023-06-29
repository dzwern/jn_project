# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 14:57
# @Author  : diaozhiwei
# @FileName: demo_oid_campaign.py
# @description: 活动期间订单监控
# @update:
"""

from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
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
        a.user_name,
        a.nick_name,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.wechat_id) wechat_nums,
        a.order_sn,
        sum(a.order_amount) order_amount
    FROM 
        t_orders_middle a
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
    else:
        return '2k以上'


# 光辉后端
def get_order_divide2(x):
    if 900 > x >= 0:
        return '0-900'
    else:
        return '2k以上'


# 光华前端
def get_order_divide3(x):
    if 600 > x >= 0:
        return '0-600'
    else:
        return '2k以上'


# 光华后端
def get_order_divide4(x):
    if 600 > x >= 0:
        return '0-600'
    else:
        return '2k以上'


# 光芒
def get_order_divide5(x):
    if 600 > x >= 0:
        return '0-600'
    else:
        return '2k以上'


# 蜂蜜
def get_order_divide6(x):
    if 300 > x >= 0:
        return '0-300'
    else:
        return '2k以上'


# 海参
def get_order_divide7(x):
    if 1700 > x >= 0:
        return '0-1700'
    else:
        return '2k以上'


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
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_oid_campaign;
    '''
    hhx_sql3.executeSqlByConn(sql)


def main():
    # 员工基础信息
    # df_user_base = get_user_base()
    # 订单信息
    df_user_oid = get_user_oid()
    # df_user_oid_base = df_user_base.merge(df_user_oid, on=['sys_user_id'], how='left')
    df_user_oid_base = df_user_oid.fillna(0)
    # 订单区间
    # df_user_oid_base['order_interval'] = df_user_oid_base.apply(lambda x: get_order_divide(x['order_amount']), axis=1)
    df1 = df_user_oid_base[df_user_oid_base['dept_name2'] == '光辉部蜜肤语前端']
    df1['order_interval'] = df1['order_amount'].apply(lambda x: get_order_divide(x))
    df2 = df_user_oid_base[df_user_oid_base['dept_name2'] == '光辉部蜜肤语后端']
    df2['order_interval'] = df2['order_amount'].apply(lambda x: get_order_divide2(x))
    df3 = df_user_oid_base[df_user_oid_base['dept_name2'] == '光华部蜜梓源面膜进粉前端']
    df3['order_interval'] = df3['order_amount'].apply(lambda x: get_order_divide3(x))
    df4 = df_user_oid_base[df_user_oid_base['dept_name2'] == '光华部蜜梓源面膜进粉后端']
    df4['order_interval'] = df4['order_amount'].apply(lambda x: get_order_divide4(x))
    df5 = df_user_oid_base[df_user_oid_base['dept_name2'] == '光芒部蜜梓源后端']
    df5['order_interval'] = df5['order_amount'].apply(lambda x: get_order_divide5(x))
    df6 = df_user_oid_base[df_user_oid_base['dept_name2'] == '光源部蜂蜜组']
    df6['order_interval'] = df6['order_amount'].apply(lambda x: get_order_divide6(x))
    df7 = df_user_oid_base[df_user_oid_base['dept_name2'] == '光源部海参组']
    df7['order_interval'] = df7['order_amount'].apply(lambda x: get_order_divide7(x))
    df_user_oid_base = pd.concat([df1, df2, df3, df4, df5, df6, df7])
    df_user_oid_base['activity_name'] = activity_name
    df_user_oid_base['id'] = df_user_oid_base['sys_user_id'].astype(str) + df_user_oid_base['order_sn'].astype(str)
    df_user_oid_base = df_user_oid_base[
        ['id', 'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
         'wechat_nums', 'order_sn', 'order_amount', 'order_interval', 'activity_name']]
    df_user_oid_base = df_user_oid_base
    df_user_oid_base['dept_name1'] = df_user_oid_base.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_user_oid_base['dept_name'] = df_user_oid_base.apply(lambda x: get_dept2(x['dept_name']), axis=1)

    # del_sql()
    print(df_user_oid_base)
    save_sql(df_user_oid_base)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 2023年五一活动，2023年38女神节活动，2023年618活动
    activity_name = '2023年618活动返场'
    main()




