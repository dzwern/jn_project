# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/15 9:50
# @Author  : diaozhiwei
# @FileName: total_order.py
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


# 远明
def get_ym_order(st, et):
    sql = '''
    SELECT
        '远明酱酒' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) order_amounts 
    FROM
        crm_tm_ymlj.t_orders  a
    LEFT JOIN crm_tm_ymlj.t_order_rel_info b ON b.orders_id = a.id
    where 
     a.create_time >= '{}'
    and a.create_time<'{}'
    -- 筛选审核阶段、已完结、待发货、待签收
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17) 
    -- 筛选营销类型
    AND b.no_performance_type in (1,3,5,6,10) 
     -- 筛选订单金额大于0
    and  a.order_amount-refund_amount>0
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 甜橙
def get_tc_order(st, et):
    sql = '''
    SELECT
        '甜橙传媒' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    where 
        a.tenant_id = 12 
    and a.create_time >= '{}'
    and a.create_time<'{}'
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
    AND a.order_amount>100
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 赖氏
def get_ls_order(st, et):
    sql = '''
    SELECT
        '赖氏父子' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount)-sum(a.refund_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN sys_user c on a.sys_user_id=c.user_id
    LEFT JOIN sys_dept d on c.dept_id=d.dept_id
    WHERE
     a.tenant_id = 10
    and a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
     -- 订单类型
    AND b.no_performance_type in (2,3,5,6,14)
    AND (d.dept_name like "电商部" or d.dept_name like "_中心%%") 
    and  a.order_amount-refund_amount>0
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 风云
def get_fy_order1(st, et):
    sql = '''
    SELECT
        '风云酱父' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount)-sum(a.refund_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN sys_user c on a.sys_user_id=c.user_id
    LEFT JOIN sys_dept d on c.dept_id=d.dept_id
    WHERE
     a.tenant_id = 8
    and a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
     -- 订单类型
    AND b.no_performance_type in (2,3,5,6,14)
    and  a.order_amount-refund_amount>0
    and not exists (select 1 from t_order_hang_up where order_id = a.id)
    AND (d.dept_name like "%%销售%%" or d.dept_name like "离职部门")  
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 风云2
def get_fy_order2(st, et):
    sql = '''
    SELECT
        '风云酱父' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount)-sum(a.refund_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN sys_user c on a.sys_user_id=c.user_id
    LEFT JOIN sys_dept d on c.dept_id=d.dept_id
    WHERE
     a.tenant_id = 21
    and a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
     -- 订单类型
    AND b.no_performance_type in (2,3,5,6,14)
    and  a.order_amount-refund_amount>0
    and not exists (select 1 from t_order_hang_up where order_id = a.id)
    AND (d.dept_name like "%%销售%%" )  
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 荷花秀
def get_hhx_order1(st, et):
    sql = '''
    SELECT
        '荷花秀' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    WHERE
        a.tenant_id in ('25')
    and a.create_time >= '{}'
    and a.create_time<'{}'
    -- 订单状态
    and a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
    # 退款状态
    and a.refund_state not in (4) 
    and a.order_amount>40
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 荷花秀2
def get_hhx_order2(st, et):
    sql = '''
    SELECT
        '荷花秀' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    WHERE
        a.tenant_id in ('25','26','27','28')
    and a.create_time >= '{}'
    and a.create_time<'{}'
    -- 订单状态
    and a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17) 
    and a.refund_state not in (4) 
    and a.order_amount>40
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 碧春
def get_bc_order(st, et):
    sql = '''
    SELECT
        '碧春酒业' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    WHERE
     a.tenant_id = 5
    and a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
    and  a.order_amount-refund_amount>0
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 女儿红
def get_neh_order(st, et):
    sql = '''
    SELECT
        '女儿红' dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount-a.refund_amount) order_amounts
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    left join sys_user as u on u.user_id = a.sys_user_id
    left join sys_dept as d on d.dept_id = u.dept_id
    WHERE
     a.tenant_id = 3
    and a.create_time >= '{}'
    and a.create_time<'{}'
    AND a.order_state in ( 0,1,2,3,4,5,7,13,14,15,16,17)
    AND b.no_performance_type in (6,1,5,10,3) 
    and  a.order_amount-refund_amount>0
    AND a.order_amount-a.refund_amount>0 -- 选取结余金额，排除退款
    AND d.dept_id not in (107,122,110,109,112,108,113,116,118)
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
     INSERT INTO `jn_order_total` 
     (
     `dept_name`,`time_type`,`members`,`order_amounts`,`member_price`
     )
     VALUES (
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name`= VALUES(`dept_name`),`time_type`= VALUES(`time_type`),`members`= VALUES(`members`),
         `order_amounts`= VALUES(`order_amounts`),`member_price`= VALUES(`member_price`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def del_sql():
    sql = '''
    truncate table jn_order_total;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 今日
    df_ym_order_d = get_ym_order(st, et)
    df_tc_order_d = get_tc_order(st, et)
    df_ls_order_d = get_ls_order(st, et)
    df_fy_order_d1 = get_fy_order1(st, et)
    df_fy_order_d2 = get_fy_order2(st, et)
    df_hhx_order_d = get_hhx_order2(st, et)
    df_bc_order_d = get_bc_order(st, et)
    df_neh_order_d = get_neh_order(st, et)
    df_order_d = pd.concat(
        [df_ym_order_d, df_tc_order_d, df_ls_order_d, df_fy_order_d1, df_fy_order_d2, df_hhx_order_d, df_bc_order_d,
         df_neh_order_d])
    df_order_d = df_order_d.fillna(0)
    df_order_d = df_order_d.groupby(['dept_name'])['members', 'order_amounts'].sum().reset_index()
    df_order_d['time_type'] = '当日'
    # 昨日
    df_ym_order_2d = get_ym_order(st2, st)
    df_tc_order_2d = get_tc_order(st2, st)
    df_ls_order_2d = get_ls_order(st2, st)
    df_fy_order_2d1 = get_fy_order1(st2, st)
    df_fy_order_2d2 = get_fy_order2(st2, st)
    df_hhx_order_2d = get_hhx_order2(st2, st)
    df_bc_order_2d = get_bc_order(st2, st)
    df_neh_order_2d = get_neh_order(st2, st)
    df_order_2d = pd.concat(
        [df_ym_order_2d, df_tc_order_2d, df_ls_order_2d, df_fy_order_2d1, df_fy_order_2d2, df_hhx_order_2d,
         df_bc_order_2d, df_neh_order_2d])
    df_order_2d = df_order_2d.fillna(0)
    df_order_2d = df_order_2d.groupby(['dept_name'])['members', 'order_amounts'].sum().reset_index()
    df_order_2d['time_type'] = '昨日'
    # 本周
    df_ym_order_w = get_ym_order(st3, et)
    df_tc_order_w = get_tc_order(st3, et)
    df_ls_order_w = get_ls_order(st3, et)
    df_fy_order_w1 = get_fy_order1(st3, et)
    df_fy_order_w2 = get_fy_order2(st3, et)
    df_hhx_order_w = get_hhx_order2(st3, et)
    df_bc_order_w = get_bc_order(st3, et)
    df_neh_order_w = get_neh_order(st3, et)
    df_order_w = pd.concat(
        [df_ym_order_w, df_tc_order_w, df_ls_order_w, df_fy_order_w1, df_fy_order_w2, df_hhx_order_w, df_bc_order_w,
         df_neh_order_w])
    df_order_w = df_order_w.fillna(0)
    df_order_w = df_order_w.groupby(['dept_name'])['members', 'order_amounts'].sum().reset_index()
    df_order_w['time_type'] = '本周'
    # 本月
    df_ym_order_m = get_ym_order(st4, et)
    df_tc_order_m = get_tc_order(st4, et)
    df_ls_order_m = get_ls_order(st4, et)
    df_fy_order_m1 = get_fy_order1(st4, et)
    df_fy_order_m2 = get_fy_order2(st4, et)
    df_hhx_order_m = get_hhx_order2(st4, et)
    df_bc_order_m = get_bc_order(st4, et)
    df_neh_order_m = get_neh_order(st4, et)
    df_order_m = pd.concat(
        [df_ym_order_m, df_tc_order_m, df_ls_order_m, df_fy_order_m1, df_fy_order_m2, df_hhx_order_m, df_bc_order_m,
         df_neh_order_m])
    df_order_m = df_order_m.fillna(0)
    df_order_m = df_order_m.groupby(['dept_name'])['members', 'order_amounts'].sum().reset_index()
    df_order_m['time_type'] = '本月'
    # 本年
    df_ym_order_y = get_ym_order(st5, et)
    df_tc_order_y = get_tc_order(st5, et)
    df_ls_order_y = get_ls_order(st5, et)
    df_fy_order_y1 = get_fy_order1(st5, et)
    df_fy_order_y2 = get_fy_order2(st5, et)
    df_hhx_order_y1 = get_hhx_order1(st5, st6)
    df_hhx_order_y2 = get_hhx_order2(st6, et)
    df_bc_order_y = get_bc_order(st5, et)
    df_neh_order_y = get_neh_order(st5, et)
    df_order_y = pd.concat(
        [df_ym_order_y, df_tc_order_y, df_ls_order_y, df_fy_order_y1, df_fy_order_y2, df_hhx_order_y1, df_hhx_order_y2,
         df_bc_order_y, df_neh_order_y])
    df_order_y = df_order_y.fillna(0)
    df_order_y = df_order_y.groupby(['dept_name'])['members', 'order_amounts'].sum().reset_index()
    df_order_y['time_type'] = '本年'
    df_order = pd.concat([df_order_d, df_order_2d, df_order_w, df_order_m, df_order_y])
    df_order['member_price'] = df_order['order_amounts'] / df_order['members']
    df_order = df_order[['dept_name', 'time_type', 'members', 'order_amounts', 'member_price']]
    df_order = df_order.fillna(0)
    print(df_order)
    del_sql()
    save_sql(df_order)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'wangkai', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 时间
    now = datetime.now().date()
    # 前一天
    st = now
    # 前2天
    st2 = now - timedelta(days=1)
    # 本周
    st3 = now - timedelta(days=now.weekday())
    # 本月
    st4 = datetime(now.year, now.month, 1)
    st5 = '2023-01-01'
    st6 = '2023-05-17'
    et = now + timedelta(days=1)
    print(st, st2, st3, st4, st5, et)
    main()
