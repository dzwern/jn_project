# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/12 10:07
# @Author  : diaozhiwei
# @FileName: hhx_member_active_day.py
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


# 客户基础信息
def get_member_base():
    sql = '''
    SELECT
        a.member_id,
        a.member_source,
        a.wechat_id,
        a.wechat_name,
        a.wechat_number,
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.member_level,
        a.first_time,
        a.create_time,
        a.last_time,
        a.last_time_diff,
        a.order_nums,
        a.order_amounts,
        a.order_nums_2023,
        a.order_amounts_2023 
    FROM
        t_member_middle a
    where a.dept_name1 !='0'
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户是否参与活动
def get_member_activity():
    sql = '''
    SELECT
        a.member_id,
        '是' is_activity
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.is_activity='是'
    and a.first_time>='2023-01-01'
    GROUP BY a.member_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 是否送礼
def get_member_present():
    sql = '''
    SELECT
        a.member_id,
        '是' is_present
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.order_amount<40
    GROUP BY a.member_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 送礼时间
def get_member_present2():
    sql = '''
    SELECT
        a.member_id,
        count(1) present_nums,
        max(a.create_time) present_time
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.order_amount<40
    GROUP BY a.member_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户活跃状态
'''
引入期：购买一次，180天内购买1次
成长期：购买2次，180天内购买2次
成熟期：购买>=3次，180天内购买多次
休眠期：180-360天休眠，
流失期：360天流失，

'''


def get_member_active(x, y):
    if 180 >= x >= 0 and y == 1:
        return '引入期'
    elif 180 >= x >= 0 and y == 2:
        return '成长期'
    elif 180 >= x >= 0 and y >= 3:
        return '成熟期'
    elif 360 >= x > 180:
        return '休眠期'
    elif x > 360:
        return '流失期'


def save_sql(df):
    sql = '''
    INSERT INTO `t_member_active_day` 
     (`id`,`member_id`,`member_source`,`wechat_id`,`wechat_name`,
     `wechat_number`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`,
     `dept_name2`,`dept_name`,`member_level`,`first_time`,`create_time`,
     `last_time`, `last_time_diff`,`order_nums`,`order_amounts`,`order_nums_2023`,
     `order_amounts_2023`,`is_activity`,`is_present`,`present_time`,`present_nums`,
     `member_active`,`decline_times`,`loss_times`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `member_id`= VALUES(`member_id`),`member_source`=values(`member_source`), `wechat_id`=values(`wechat_id`),
         `wechat_name`=values(`wechat_name`),`wechat_number`=values(`wechat_number`),`sys_user_id`=values(`sys_user_id`),
         `user_name`=values(`user_name`),`nick_name`=values(`nick_name`),`dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),
         `dept_name`=values(`dept_name`),`member_level`=values(`member_level`),`first_time`=values(`first_time`),
         `create_time`=values(`create_time`),`last_time`=values(`last_time`),`last_time_diff`=values(`last_time_diff`),
         `order_nums`=values(`order_nums`),`order_amounts`=values(`order_amounts`),`order_nums_2023`=values(`order_nums_2023`),
         `order_amounts_2023`=values(`order_amounts_2023`),`is_activity`=values(`is_activity`),`is_present`=values(`is_present`),
         `present_time`=values(`present_time`),`present_nums`=values(`present_nums`),`member_active`=values(`member_active`),
         `decline_times`=values(`decline_times`),`loss_times`=values(`loss_times`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_member_active_day;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 客户基础数据
    df_member_base = get_member_base()
    # 是否参与活动
    df_member_activity = get_member_activity()
    # 是否送礼
    df_member_present = get_member_present()
    # 送礼时间
    df_member_present2 = get_member_present2()
    df_member_base = df_member_base.merge(df_member_activity, on=['member_id'], how='left')
    df_member_base = df_member_base.merge(df_member_present, on=['member_id'], how='left')
    df_member_base = df_member_base.merge(df_member_present2, on=['member_id'], how='left')
    df_member_base = df_member_base
    df_member_base['member_active'] = df_member_base.apply(lambda x: get_member_active(x['last_time_diff'], x['order_nums']), axis=1)
    df_member_base['decline_times'] = df_member_base['last_time_diff'] - 180
    df_member_base.loc[(df_member_base["decline_times"] < 0), "decline_times"] = 0
    df_member_base['loss_times'] = df_member_base['last_time_diff'] - 360
    df_member_base.loc[(df_member_base["loss_times"] < 0), "loss_times"] = 0
    df_member_base = df_member_base.fillna(0)
    df_member_base['id'] = df_member_base['member_id']
    df_member_base['present_time'] = df_member_base['present_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member_base = df_member_base[
        ['id', 'member_id', 'member_source', 'wechat_id', 'wechat_name', 'wechat_number', 'sys_user_id', 'user_name',
         'nick_name', 'dept_name1', 'dept_name2', 'dept_name', 'member_level', 'first_time', 'create_time', 'last_time',
         'last_time_diff', 'order_nums', 'order_amounts', 'order_nums_2023', 'order_amounts_2023', 'is_activity',
         'is_present', 'present_time', 'present_nums', 'member_active', 'decline_times', 'loss_times']]
    df_member_base=df_member_base
    del_sql()
    save_sql(df_member_base)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 开始时间，结束时间
    main()


