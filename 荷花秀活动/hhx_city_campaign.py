# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 14:58
# @Author  : diaozhiwei
# @FileName: hhx_city_campaign.py
# @description: 活动期间城市监控
# @update:
"""
from datetime import datetime
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd


def get_city_order():
    sql = '''
    select 
        a.dept_name,
        a.receiver_province,
        a.receiver_city,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) members_amount
    from 
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.activity_name='{}'
    and a.order_amount>40
    GROUP BY a.dept_name,a.receiver_province,a.receiver_city
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组','光芒部一组',
           '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组','光华部1组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端',
           '光芒部蜜梓源后端','光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端',
           '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉后端','光华部蜜梓源面膜老粉前端','光华部蜜梓源面膜老粉后端','光华部蜜梓源面膜进粉后端',
           '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组','光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部',
           '光芒部', '光芒部', '光芒部', '光芒部',
           '光华部', '光华部', '光华部', '光华部', '光华部','光华部','光华部',
           '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name2': df2,
          'dept_name1': df3}
    data = pd.DataFrame(df)
    return data


def save_sql(df):
    sql = '''
    INSERT INTO `t_address_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`receiver_province`,
     `receiver_city`,`members`,`members_rate`,`members_amount`,`members_amount_rate`,
     `members_value`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `receiver_province`=values(`receiver_province`),`receiver_city`=values(`receiver_city`),`members`=values(`members`),
         `members_rate`=values(`members_rate`),`members_amount`=values(`members_amount`),`members_amount_rate`=values(`members_amount_rate`),
         `members_value`=values(`members_value`),`activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础城市数据源
    df_city_order = get_city_order()
    # 部门表
    df_dept_name = get_hhx_user()
    df_city_order = df_city_order.merge(df_dept_name, on=['dept_name'], how='left')
    df_city_order = df_city_order
    df_city_order['members_sum'] = df_city_order['members'].sum()
    df_city_order['members_amount_sum'] = df_city_order['members_amount'].sum()
    # 占比
    df_city_order['members_rate'] = df_city_order['members'] / df_city_order['members_sum']
    df_city_order['members_amount_rate'] = df_city_order['members_amount'] / df_city_order['members_amount_sum']
    # 价值
    df_city_order['members_value'] = df_city_order['members_amount'] / df_city_order['members']
    df_city_order = df_city_order.fillna(0)
    df_city_order['activity_name'] = activity_name
    df_city_order['id'] = df_city_order['dept_name'] + df_city_order['receiver_province'] + df_city_order[
        'receiver_city'] + df_city_order['activity_name']
    df_city_order = df_city_order[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'receiver_province', 'receiver_city', 'members',
         'members_rate', 'members_amount', 'members_amount_rate', 'members_value', 'activity_name']]
    df_city_order=df_city_order
    save_sql(df_city_order)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    activity_name = '2023年五一活动'
    main()
