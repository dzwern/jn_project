# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:00
# @Author  : diaozhiwei
# @FileName: hhx_campaign.py
# @description: 活动期间整体数据指标监控
# @update:
"""
from modules.mysql import jnmtMySQL
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from modules.func import utils
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


# 预测目标
def get_pred_target():
    sql = '''
    SELECT
        a.dept_name,
        sum(a.amount_pred) amount_target 
    FROM
        t_pred_campaign a
    GROUP BY a.dept_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 业务目标
def get_work_target():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组', '光芒部一组',
           '光华部二组', '光华部五组', '光华部1组', '光华部六组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = [160000, 160000, 1100000, 1100000,
           900000, 900000, 800000, 900000,
           155000, 145000, 540000, 340000,
           330000, 330000, 340000, 700000]
    df = {"dept_name": df1,
          'amount_target2': df2}
    data = pd.DataFrame(df)
    return data


def save_sql(df):
    sql = '''
    INSERT INTO `t_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`group_users`,`group_wechats`,
     `members`,`order_amounts`,`members_campaign`,`order_amounts_campaign`,
     `amount_target`,`amount_target2`,`completion_rate`,
     `completion_rate2`,`member_price`,`user_price`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`=VALUES(`dept_name`),
         `group_users`=values(`group_users`),`group_wechats`=values(`group_wechats`),`members`=values(`members`),
         `order_amounts`=values(`order_amounts`), `members_campaign`=values(`members_campaign`),
         `order_amounts_campaign`=values(`order_amounts_campaign`), 
         `amount_target`=values(`amount_target`),`amount_target2`=values(`amount_target2`),
         `completion_rate`=values(`completion_rate`),`completion_rate2`=values(`completion_rate2`),
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
    # 预估目标
    df_pred_target = get_pred_target()
    # 业务目标
    df_work_rarget = get_work_target()
    df_campaign = df_campaign.merge(df_order_campaign, on=['dept_name1', 'dept_name2', 'dept_name'], how='left')
    df_campaign = df_campaign.merge(df_order_campaign2, on=['dept_name'], how='left')
    df_campaign = df_campaign.merge(df_pred_target, on=['dept_name'], how='left')
    df_campaign = df_campaign.merge(df_work_rarget, on=['dept_name'], how='left')
    # 时间
    # df_campaign['days']=(datetime.now()-datetime.strptime('2023-04-01', "%Y-%m-%d")+timedelta(days=1)).days
    # 客单价
    df_campaign['member_price'] = df_campaign['order_amounts'] / df_campaign['members']
    # 员工人效
    df_campaign['user_price'] = df_campaign['order_amounts'] / df_campaign['group_users']
    # 日均价
    # df_campaign['day_price']=df_campaign['order_amounts']/df_campaign['days']
    # 目标
    df_campaign['completion_rate'] = df_campaign['order_amounts'] / df_campaign['amount_target']
    df_campaign['completion_rate2'] = df_campaign['order_amounts'] / df_campaign['amount_target2']
    df_campaign['activity_name'] = activity_name
    df_campaign['id'] = df_campaign['dept_name'].astype(str) + df_campaign['activity_name'].astype(str)
    df_campaign = df_campaign[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'group_users', 'group_wechats', 'members', 'order_amounts',
         'members_campaign', 'order_amounts_campaign',
         'amount_target', 'amount_target2', 'completion_rate', 'completion_rate2', 'member_price', 'user_price',
         'activity_name']]
    print(df_campaign)
    df_campaign = df_campaign.replace([np.inf, -np.inf], np.nan)
    df_campaign = df_campaign.fillna(0)
    del_sql()
    save_sql(df_campaign)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    # 开始时间，结束时间
    activity_name = '2023年五一活动'
    main()


