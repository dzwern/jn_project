# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/10 14:39
# @Author  : diaozhiwei
# @FileName: hhx_fans_order_day.py
# @description: 
# @update:
"""

from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd
import datetime
import numpy as np


# 基础数据，每日进粉数
def get_member_credit():
    sql = '''
    SELECT
        f.dept_name,
        e.nick_name,
        d.id wechat_id,
        d.wecaht_number wechat_number,
        left(a.new_sprint_time,10) first_time,
        sum(a.credit) fans 
    FROM t_wechat_fans_log a
    LEFT JOIN t_wechat d on d.id=a.wechat_id
    LEFT JOIN sys_user e on e.user_id=d.sys_user_id
    LEFT JOIN sys_dept f on e.dept_id=f.dept_id
    where a.tenant_id=11
    and a.new_sprint_time>='{}'
    and a.new_sprint_time<'{}'
    and a.credit>0
    GROUP BY f.dept_name,e.nick_name,d.wecaht_number,left(a.new_sprint_time,10)
    '''.format(st,et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组', '光芒部二组', '光芒部六组', '光芒部三组',
           '光芒部一组', '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端', '光芒部蜜梓源后端',
           '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光华部蜜梓源面膜进粉前端',
           '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉后端',
           '光华部蜜梓源面膜老粉前端', '光华部蜜梓源面膜老粉后端', '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组',
           '光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部', '光芒部', '光芒部', '光芒部', '光芒部', '光华部', '光华部', '光华部',
           '光华部', '光华部', '光华部', '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name1': df2,
          'dept_name2': df3}
    data = pd.DataFrame(df)
    return data


# 及时订单
def get_order_js():
    sql='''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_js,
        sum(a.order_amount) order_amount_js
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('当日首单日常成交','当日首单活动成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 及时订单
def get_order_hx():
    sql='''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_hx,
        sum(a.order_amount) order_amount_hx
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 及时订单
def get_order_campaign_hx():
    sql='''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_campaign_hx,
        sum(a.order_amount) order_amount_campaign_hx
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单活动成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 复购订单
def get_order_fg():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_fg,
        sum(a.order_amount) order_amount_fg
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('复购日常成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 复购订单
def get_order_campaign_fg():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_campaign_fg,
        sum(a.order_amount) order_amount_campaign_fg
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('复购活动成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
     INSERT INTO `t_fans_order_day` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`nick_name`,
     `wechat_id`,`wechat_number`,`first_time`,`years`,`monthly`,
     `fans`,`order_member_js`,`order_amount_js`,`order_member_hx`,`order_amount_hx`,
     `order_member_campaign_hx`,`order_amount_campaign_hx`,`order_member_fg`,`order_amount_fg`,`order_member_campaign_fg`,
     `order_amount_campaign_fg`
     )
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`= VALUES(`dept_name`),
         `nick_name`=VALUES(`nick_name`),`wechat_id`=VALUES(`wechat_id`),`wechat_number`=values(`wechat_number`),
         `first_time`=values(`first_time`),`years`=values(`years`),`monthly`=values(`monthly`),
         `fans`=values(`fans`),`order_member_js`=values(`order_member_js`),`order_amount_js`=values(`order_amount_js`),
         `order_member_hx`=values(`order_member_hx`),`order_amount_hx`=values(`order_amount_hx`),`order_member_campaign_hx`=values(`order_member_campaign_hx`),
         `order_amount_campaign_hx`=values(`order_amount_campaign_hx`), `order_member_fg`=values(`order_member_fg`), 
         `order_amount_fg`=values(`order_amount_fg`),`order_member_campaign_fg`=values(`order_member_campaign_fg`),
         `order_amount_campaign_fg`=values(`order_amount_campaign_fg`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 设备进粉数
    df_credit = get_member_credit()
    # 部门
    df_hhx_user = get_hhx_user()
    df_fans_order = df_credit.merge(df_hhx_user, on=['dept_name'], how='left')
    # 及时销售数据
    df_order_js = get_order_js()
    # 后续销售数据
    df_order_hx = get_order_hx()
    # 活动后续销售数据
    df_order_campaign_hx = get_order_campaign_hx()
    # 复购销售数据
    df_order_fg = get_order_fg()
    # 活动复购销售数据
    df_order_campaign_fg = get_order_campaign_fg()
    df_fans_order=df_fans_order.merge(df_order_js,on=['dept_name', 'wechat_number', 'first_time'], how='left')
    df_fans_order=df_fans_order.merge(df_order_hx,on=['dept_name', 'wechat_number', 'first_time'], how='left')
    df_fans_order=df_fans_order.merge(df_order_campaign_hx,on=['dept_name', 'wechat_number', 'first_time'], how='left')
    df_fans_order=df_fans_order.merge(df_order_fg,on=['dept_name', 'wechat_number', 'first_time'], how='left')
    df_fans_order=df_fans_order.merge(df_order_campaign_fg,on=['dept_name', 'wechat_number', 'first_time'], how='left')
    df_fans_order=df_fans_order.fillna(0)
    df_fans_order['id'] = df_fans_order['wechat_number'].astype(str) + df_fans_order['first_time']
    df_fans_order['first_time'] = pd.to_datetime(df_fans_order['first_time'], errors='coerce')
    df_fans_order['years'] = df_fans_order['first_time'].dt.year
    df_fans_order['monthly'] = df_fans_order['first_time'].dt.month
    df_fans_order = df_fans_order.replace([np.inf, -np.inf], np.nan)
    df_fans_order = df_fans_order[[
        "id", "dept_name1", 'dept_name2', "dept_name", "nick_name", "wechat_id", "wechat_number", "first_time", 'years',
        'monthly', "fans", 'order_member_js', 'order_amount_js', 'order_member_hx', 'order_amount_hx',
        'order_member_campaign_hx', 'order_amount_campaign_hx', 'order_member_fg', 'order_amount_fg',
        'order_member_campaign_fg', 'order_amount_campaign_fg']]
    df_fans_order=df_fans_order
    save_sql(df_fans_order)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 时间转化
    st = '2022-01-01'
    et = '2023-05-10'
    st1 = datetime.datetime.strptime(st, "%Y-%m-%d")
    et1 = datetime.datetime.strptime(et, "%Y-%m-%d")
    main()
