# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:00
# @Author  : diaozhiwei
# @FileName: hhx_campaign.py
# @description: 活动期间整体数据指标监控
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


# 设备客户类型
def get_campaign():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.wechat_id) group_wechats,
        count(DISTINCT a.sys_user_id)  group_users
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    and a.wechat_name not in ('玫瑰诗') 
    and a.dept_name2 !='0'
    GROUP BY a.dept_name2,a.dept_name1,a.dept_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 销售额
def get_order_campaign():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount)  order_amounts
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.activity_name='{}'
    and a.order_amount>40
    GROUP BY a.dept_name1,a.dept_name2,a.dept_name
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 销售额
def get_order_campaign2():
    sql = '''
    SELECT
        a.dept_name,
        count(DISTINCT a.member_id) members_campaign,
        sum(a.order_amount)  order_amounts_campaign
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.activity_name='{}'
    and a.order_amount>40
    GROUP BY a.dept_name
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_work_target():
    sql = '''
    SELECT
        a.dept_name,
        a.amount_target
    FROM
        t_campaign_target_log a 
    WHERE
        a.activity_name = '{}'
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`group_users`,`group_wechats`,
     `members`,`order_amounts`,`members_campaign`,`order_amounts_campaign`,
     `amount_target`,`completion_rate`,`member_price`,`user_price`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`=VALUES(`dept_name`),
         `group_users`=values(`group_users`),`group_wechats`=values(`group_wechats`),`members`=values(`members`),
         `order_amounts`=values(`order_amounts`), `members_campaign`=values(`members_campaign`),
         `order_amounts_campaign`=values(`order_amounts_campaign`), 
         `amount_target`=values(`amount_target`),`completion_rate`=values(`completion_rate`),
         `member_price`=values(`member_price`),`user_price`=values(`user_price`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_campaign;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 基础数据
    df_campaign = get_campaign()
    # 销售数据
    df_order_campaign = get_order_campaign()
    # 活动业绩
    df_order_campaign2 = get_order_campaign2()
    # 业务目标
    df_work_rarget = get_work_target()
    df_campaign = df_campaign.merge(df_order_campaign, on=['dept_name1', 'dept_name2', 'dept_name'], how='left')
    df_campaign = df_campaign.merge(df_order_campaign2, on=['dept_name'], how='left')
    df_campaign = df_campaign.merge(df_work_rarget, on=['dept_name'], how='left')
    # 客单价
    df_campaign['member_price'] = df_campaign['order_amounts'] / df_campaign['members']
    # 员工人效
    df_campaign['user_price'] = df_campaign['order_amounts'] / df_campaign['group_users']
    # 目标
    df_campaign['completion_rate'] = df_campaign['order_amounts'] / df_campaign['amount_target']
    df_campaign['activity_name'] = activity_name
    df_campaign['id'] = df_campaign['dept_name'].astype(str) + df_campaign['activity_name'].astype(str)
    df_campaign = df_campaign[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'group_users', 'group_wechats', 'members', 'order_amounts',
         'members_campaign', 'order_amounts_campaign',
         'amount_target', 'completion_rate', 'member_price', 'user_price','activity_name']]
    print(df_campaign)
    df_campaign = df_campaign.replace([np.inf, -np.inf], np.nan)
    df_campaign = df_campaign.fillna(0)
    # del_sql()
    save_sql(df_campaign)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 开始时间，结束时间
    activity_name = '2023年618活动返场'
    main()


