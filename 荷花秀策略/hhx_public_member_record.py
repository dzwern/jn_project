# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/8 9:35
# @Author  : diaozhiwei
# @FileName: hhx_public_member_record.py
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


# 部门员工
def get_user(user_id):
    sql='''
    SELECT
        a.sys_user_id new_sys_user_id, 
        a.dept_name,
        a.dept_name1,
        a.dept_name2 
    FROM
        hhx_dx.t_wechat_middle a
    where a.sys_user_id in ({})
    GROUP BY a.sys_user_id
    '''.format(utils.quoted_list_func(user_id))
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 公池分配记录
def get_member_log():
    sql = '''
    SELECT
        t2.member_id,
        w.wecaht_number wecaht_number1,
        b.nick_name,
        if(t2.create_time is null,'否','是') is_allocation,
        t2.create_time allocation_time,
        t2.old_sys_user_id,
        t2.new_sys_user_id
    FROM
    (
    SELECT 
    *,
    ROW_NUMBER() over(PARTITION BY a.member_id ORDER BY a.create_time DESC) AS ROW_NUM
    FROM t_member_log a
    WHERE a.tenant_id in ('25','26','27','28')
    AND a.type = 4 
    AND a.create_time > "2023-06-20 00:00:00"
    )t2
    LEFT JOIN sys_user b on t2.new_sys_user_id=b.user_id
    LEFT JOIN t_member m on m.id = t2.member_id 
    left join t_wechat w on w.id = m.wechat_id
    WHERE 1=1
    AND t2.ROW_NUM = 1
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 公池跟进记录
def get_member_follow():
    sql = '''
    SELECT
        s.member_id,
        s.track_date follow_time
    FROM
    (
    SELECT 
        *,
        ROW_NUMBER() over(PARTITION BY member_id ORDER BY track_date DESC) AS ROW_NUM
    FROM t_member_track_record
    WHERE tenant_id in ('25','26','27','28')
    AND member_id  is not null
    and track_date > "2023-06-20 00:00:00" 
    )s
    WHERE s.ROW_NUM = 1
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 公池加微记录
def get_member_wechat():
    sql = '''
    SELECT
        s.member_id,
        s.track_date wechat_time,
        w.wecaht_number
    FROM
    (
    SELECT 
        *,
        ROW_NUMBER() over(PARTITION BY member_id ORDER BY track_date DESC) AS ROW_NUM
    FROM t_member_track_record
    WHERE tenant_id in ('25','26','27','28')
    AND member_id  is not null
    and track_date > "2023-06-20 00:00:00" 
    )s
    LEFT JOIN t_member as m on  m.id = s.member_id 
    left join t_wechat as w on w.id = m.wechat_id
    WHERE s.ROW_NUM = 1
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 公池销售记录
def get_member_order(member_id):
    sql = '''
    SELECT
        a.member_id,
        min(a.create_time) first_time,
        max(a.create_time) create_time,
        count(1) amounts,
        sum(a.order_amount) order_amounts
    FROM
        t_orders_middle a
    where  a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.create_time>='2023-06-20'
    and a.order_amount>40
    and a.member_id in ({})
    GROUP BY a.member_id
    '''.format(utils.quoted_list_func(member_id))
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_public_member_record` 
     (
     `id`,`dept_name1`,`dept_name2`,`dept_name`,`old_sys_user_id`,
     `new_sys_user_id`,`wechat_number`,`nick_name`,`member_id`,`is_allocation`,
     `allocation_time`,`is_follow`,`follow_time`,`is_wechat`,`wechat_time`,
     `is_amount`,`first_time`,`create_time`,`amounts`,`order_amounts`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `old_sys_user_id`=values(`old_sys_user_id`),`new_sys_user_id`=values(`new_sys_user_id`),
         `wechat_number`=values(`wechat_number`),`nick_name`=values(`nick_name`),
         `member_id`=values(`member_id`),`is_allocation`=values(`is_allocation`),
         `allocation_time`=values(`allocation_time`),`is_follow`=values(`is_follow`),
         `follow_time`=values(`follow_time`),`is_wechat`=values(`is_wechat`),`wechat_time`=values(`wechat_time`),
         `is_amount`=values(`is_amount`),`first_time`=values(`first_time`),`create_time`=values(`create_time`),
         `amounts`=values(`amounts`),`order_amounts`=values(`order_amounts`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 公池分配记录
    df_member_log = get_member_log()
    # 公池跟进记录
    df_member_follow = get_member_follow()
    # 公池加微记录
    df_member_wechat = get_member_wechat()
    member_id = df_member_wechat['member_id'].tolist()
    # 公池销售记录
    df_member_order = get_member_order(member_id)
    user_id=df_member_log['new_sys_user_id'].tolist()
    # 关联
    df_user = get_user(user_id)
    df_user['new_sys_user_id'] = df_user['new_sys_user_id'].astype(str)
    df_member_log['new_sys_user_id'] = df_member_log['new_sys_user_id'].astype(str)
    df_member_log = df_member_log.merge(df_user, on=['new_sys_user_id'], how='left')
    df_member_log = df_member_log.merge(df_member_follow, on=['member_id'], how='left')
    df_member_log = df_member_log.merge(df_member_wechat, on=['member_id'], how='left')
    df_member_order['member_id'] = df_member_order['member_id'].astype(str)
    df_member_log['member_id'] = df_member_log['member_id'].astype(str)
    df_member_log = df_member_log.merge(df_member_order, on=['member_id'], how='left')
    df_member_log = df_member_log
    # 是否分配
    # df_member_log['is_allocation'] = df_member_log['allocation_time'].apply(lambda x: '否' if x is None else '是')
    # 是否跟进
    df_member_log['follow_time'] = df_member_log['follow_time'].astype(str).replace('NaT', None)
    df_member_log['is_follow'] = df_member_log['follow_time'].apply(lambda x: '否' if x is None else '是')
    # 是否加微
    df_member_log['wechat_time'] = df_member_log['wechat_time'].astype(str).replace('NaT', None)
    df_member_log['wecaht_number'] = df_member_log['wecaht_number'].astype(str).replace('nan', None)
    df_member_log['wecaht_number'] = df_member_log['wecaht_number'].astype(str).replace('None', None)
    df_member_log['is_wechat'] = df_member_log['wecaht_number'].apply(lambda x: '否' if x is None else '是')
    # 是否销售
    df_member_log['first_time'] = df_member_log['first_time'].astype(str).replace('nan', None)
    df_member_log['is_amount'] = df_member_log['first_time'].apply(lambda x: '否' if x is None else '是')
    # 数据处理
    df_member_log[['amounts', 'order_amounts']] = df_member_log[['amounts', 'order_amounts']].fillna(0)
    df_member_log = df_member_log.fillna(0)
    df_member_log['allocation_time'] = df_member_log['allocation_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member_log['follow_time'] = df_member_log['follow_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member_log['wechat_time'] = df_member_log['wechat_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member_log['first_time'] = df_member_log['first_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member_log['create_time'] = df_member_log['create_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member_log['id'] = df_member_log['dept_name'].astype(str) + df_member_log['new_sys_user_id'].astype(str) + df_member_log['allocation_time'].astype(str)
    df_member_log = df_member_log[['id','dept_name1', 'dept_name2', 'dept_name', 'old_sys_user_id', 'new_sys_user_id',
                                   'wecaht_number', 'nick_name', 'member_id', 'is_allocation', 'allocation_time',
                                   'is_follow', 'follow_time', 'is_wechat', 'wechat_time', 'is_amount', 'first_time',
                                   'create_time', 'amounts','order_amounts']]
    df_member_log=df_member_log
    save_sql(df_member_log)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()
