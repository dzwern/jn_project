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
import numpy as np
from datetime import datetime, timedelta
import sys
from dateutil.relativedelta import relativedelta


# 基础数据，每日进粉数
def get_member_credit():
    sql = '''
    SELECT
        f.dept_name,
        e.nick_name,
        d.id wechat_id,
        d.wecaht_number wechat_number,
        a.tenant_id,
        left(a.new_sprint_time,10) first_time,
        sum(a.credit) fans 
    FROM t_wechat_fans_log a
    LEFT JOIN t_wechat d on d.id=a.wechat_id
    LEFT JOIN sys_user e on e.user_id=d.sys_user_id
    LEFT JOIN sys_dept f on e.dept_id=f.dept_id
    where a.tenant_id in ( '25', '26', '27', '28' ) 
    and a.new_sprint_time>='{}'
    and a.new_sprint_time<'{}'
    and a.credit>0
    GROUP BY f.dept_name,e.nick_name,d.wecaht_number,a.tenant_id,left(a.new_sprint_time,10)
    '''.format(st,st2)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    sql = '''
    SELECT
        a.dept_name,
        a.dept_name1,
        a.dept_name2,
        a.tenant_id tenant_id2
    FROM
        t_dept_tmp a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


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


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_fans_order_day;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 设备进粉数
    df_credit = get_member_credit()
    # 部门
    df_hhx_user = get_hhx_user()
    df_fans_order = df_credit.merge(df_hhx_user, on=['dept_name'], how='left')

    # 判断
    df_fans_order=df_fans_order.fillna(0)
    # df_fans_order['fuzhu']=df_fans_order['tenant_id2']-df_fans_order['tenant_id']
    # df_fans_order=df_fans_order.loc[df_fans_order['fuzhu']==0,:]

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
    df_fans_order=df_fans_order.drop(index=df_fans_order.dept_name1[df_fans_order.dept_name1 == 0].index)
    df_fans_order=df_fans_order.drop(index=df_fans_order.dept_name1[df_fans_order.dept_name1 == '0'].index)
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
    print(df_fans_order)
    # del_sql()
    save_sql(df_fans_order)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 时间转化，需要分两次运行，一次是全量，一次是系统切割时间
    # 开始时间，结束时间
    # st='2023-01-01'
    st = '2023-01-01'
    st1 = datetime.strptime(st, "%Y-%m-%d")
    st2 = '2023-05-17'
    st2 = datetime.strptime(st2, "%Y-%m-%d")
    time1 = datetime.now()
    et = time1 + relativedelta(days=2)
    et1 = utils.date2str(et)
    print(st1,st2,et1)
    main()





