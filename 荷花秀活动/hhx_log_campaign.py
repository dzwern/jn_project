# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/5 10:41
# @Author  : diaozhiwei
# @FileName: hhx_log_campaign.py
# @description: 荷花秀历史活动进度，按照天进行计算
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


# 设备客户类型2
def get_campaign2():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.wechat_id) group_wechats,
        count(DISTINCT a.sys_user_id)  group_users
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
def get_order_campaign():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        left(a.create_time,10) create_time,
        row_number() over(PARTITION BY a.dept_name ORDER BY left(a.create_time,10)) rank2,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount)  order_amounts
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.activity_name='{}'
    and a.order_amount>40
    GROUP BY a.dept_name1,a.dept_name2,a.dept_name,left(a.create_time,10)
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
    INSERT INTO `t_log_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`create_time`,
     `create_day`,`group_users`,`group_wechats`,`members`,`order_amounts`,
     `member_price`,`user_price`,`activity_name`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`=VALUES(`dept_name`),
         `create_time`=values(`create_time`),`create_day`=values(`create_day`),`group_users`=values(`group_users`),
         `group_wechats`=values(`group_wechats`),`members`=values(`members`),`order_amounts`=values(`order_amounts`),
          `member_price`=values(`member_price`),`user_price`=values(`user_price`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def del_sql():
    sql = '''
    truncate table t_log_campaign;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 基础数据，参与活动设备数，人员
    df_campaign = get_campaign2()
    # 销售数据
    df_order_campaign = get_order_campaign()
    df_campaign = df_campaign.merge(df_order_campaign, on=['dept_name1', 'dept_name2', 'dept_name'], how='left')
    # 客单价
    df_campaign['member_price'] = df_campaign['order_amounts'] / df_campaign['members']
    # 员工人效
    df_campaign['user_price'] = df_campaign['order_amounts'] / df_campaign['group_users']
    # 活动第几天
    df_campaign['辅助1'] = '第'
    df_campaign['辅助2'] = '天'
    df_campaign['create_day'] = df_campaign['辅助1'] + df_campaign['rank2'].astype(str) + df_campaign['辅助2']
    df_campaign['activity_name'] = activity_name
    df_campaign['id'] = df_campaign['dept_name'] + df_campaign['create_day'] + df_campaign['activity_name']
    df_campaign = df_campaign[
        ['id','dept_name1', 'dept_name2', 'dept_name', 'create_time', 'create_day', 'group_users', 'group_wechats',
         'members', 'order_amounts', 'member_price', 'user_price', 'activity_name']]
    df_campaign = df_campaign
    print(df_campaign)
    # del_sql()
    save_sql(df_campaign)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 开始时间，结束时间
    # 2023年五一活动，2023年38女神节活动，2023年618活动
    activity_name = '2023年38女神节活动'
    main()
