# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/7 8:43
# @Author  : diaozhiwei
# @FileName: total_jianjiu_order.py
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


def get_order():
    sql = '''
    with
    order_qianduan as (
    SELECT 
         planlog.order_no,
         planlog.product,
         planlog.order_amount,
         planlog.order_create_time,
         left(planlog.order_create_time,7) as order_month,
         planlog.customer_exists,
         planlog.member_id,
         plan.name,
         budget.budget_amount -- 无推广费用，里面有少量金额
    FROM 
        t_online_retailer_plan_log as planlog
    LEFT JOIN  t_online_retailer_plan_budget as budget ON budget.id = planlog.budget_id and budget.tenant_id =10
    LEFT JOIN t_online_retailer_plan as plan  ON plan.id = budget.online_retailer_plan_id and plan.tenant_id =10
    
    WHERE 1=1
        AND planlog.tenant_id =10
        and plan.id in (322,323,328) -- (322,323,328)
        and	budget.is_del = 0
    ),
    order_houduan as (
    SELECT  DISTINCT
        order_month,
        count(DISTINCT p.order_id) order_num,
        count(DISTINCT p.member_id) members_num,
        sum(jieyu_amount) jieyu_amount
    FROM
    (
        SELECT  DISTINCT
        o.id as order_id,
        left(o.create_time,7) as order_month,
        o.member_id,
        (o.order_amount - o.refund_amount) jieyu_amount
        FROM
            t_orders o 
        LEFT JOIN t_order_rel_info b on o.id=b.orders_id
        LEFT JOIN sys_user as u on u.user_id = o.sys_user_id and u.tenant_id = 10
        left JOIN sys_dept as d on d.dept_id = u.dept_id and d.tenant_id = 10
    
        INNER JOIN (SELECT DISTINCT member_id,order_create_time FROM order_qianduan) as p on p.member_id =  o.member_id AND  p.order_create_time <  o.create_time
    
        WHERE
            o.tenant_id = 10
            and o.create_time  >= '2023-01-01 00:00:00'
            -- 订单状态
            AND o.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
            -- 订单类型
            AND b.no_performance_type in (2,3,5,6,14)
            AND (d.dept_name like "电商部" or d.dept_name like "_中心%%")
        HAVING jieyu_amount  >  0
    )p GROUP BY 1
    )
    SELECT '赖氏父子' dept_name,'qianduan' types,order_month,COUNT(DISTINCT order_no) as order_num,COUNT(DISTINCT member_id) as  members_num,SUM(order_amount)  as jieyu_amount FROM  order_qianduan GROUP BY 1,2,3
    union
    SELECT '赖氏父子' dept_name,'houduan' types,order_month,order_houduan.order_num,order_houduan.members_num,order_houduan.jieyu_amount FROM order_houduan
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def get_order2():
    sql = '''
    with order_qianduan as (
    SELECT 
     planlog.order_no,
     planlog.product,
     planlog.order_amount,
     planlog.order_create_time,
     left(planlog.order_create_time,7) as order_month,
     planlog.customer_exists,
     planlog.member_id,
     plan.name,
     budget.budget_amount
    FROM 
        t_online_retailer_plan_log as planlog
    LEFT JOIN  t_online_retailer_plan_budget as budget ON budget.id = planlog.budget_id and budget.tenant_id =8
    LEFT JOIN t_online_retailer_plan as plan  ON plan.id = budget.online_retailer_plan_id and plan.tenant_id =8
    WHERE 1=1
        AND planlog.tenant_id =8
        and plan.id in (226,241,253,265,279,266)
        and	budget.is_del = 0
    )
    ,order_houduan as (
    SELECT  DISTINCT
        order_month,
        count(DISTINCT p.order_id) order_num,
        count(DISTINCT p.member_id) members_num,
        sum(jieyu_amount) jieyu_amount
    FROM
    (
        SELECT  DISTINCT
        o.id as order_id,
        left(o.create_time,7) as order_month,
        o.member_id,
        (o.order_amount - o.refund_amount) jieyu_amount
        FROM
            t_orders o 
        LEFT JOIN t_order_rel_info b on o.id=b.orders_id
        LEFT JOIN sys_user as u on u.user_id = o.sys_user_id and u.tenant_id = 8
        left JOIN sys_dept as d on d.dept_id = u.dept_id and d.tenant_id = 8
        INNER JOIN (SELECT DISTINCT member_id,order_create_time FROM order_qianduan) as p on p.member_id =  o.member_id AND  p.order_create_time <  o.create_time
        WHERE
            o.tenant_id = 8
            and o.create_time  >= '2023-01-01 00:00:00'
            -- 订单状态
            AND o.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
            -- 订单类型
            -- AND b.no_performance_type in (2,3,5,6,14)
            -- AND (d.dept_name like "%%四部%%" or d.dept_name like "%%一部%%")
        HAVING jieyu_amount  >  0
    )p
    group by 1
    )
    SELECT '风云酱父' dept_name
    ,'qianduan' types
    ,order_month
    ,COUNT(DISTINCT order_no) as order_num
    ,COUNT(DISTINCT member_id) as  members_num
    ,SUM(order_amount)  as jieyu_amount FROM  order_qianduan GROUP BY 1,2,3
    union
    SELECT '风云酱父' dept_name
    ,'houduan' types
    ,order_month
    ,order_houduan.order_num
    ,order_houduan.members_num
    ,order_houduan.jieyu_amount FROM order_houduan
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
     INSERT INTO `jn_jianjiu_fans_total` 
     (
     `dept_name`,`monthly`,`order_amounts_start`,`order_amounts_end`
     )
     VALUES (
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name`= VALUES(`dept_name`),`monthly`= VALUES(`monthly`),`order_amounts_start`= VALUES(`order_amounts_start`),
         `order_amounts_end`= VALUES(`order_amounts_end`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def del_sql():
    sql = '''
    truncate table jn_jianjiu_fans_total;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    df_order = get_order()
    df_order2 = get_order2()
    df_order = pd.concat([df_order, df_order2])
    df_order = df_order[['dept_name', 'types', 'order_month', 'jieyu_amount']]
    df_order = df_order.set_index(['dept_name', 'order_month', 'types'])['jieyu_amount']
    df_order = df_order.unstack()
    df_order = df_order.reset_index()
    df_order = df_order.rename(
        columns={'order_month': 'monthly', 'qianduan': 'order_amounts_start', 'houduan': 'order_amounts_end'})
    print(df_order)
    df_order = df_order[['dept_name', 'monthly', 'order_amounts_start', 'order_amounts_end']]
    del_sql()
    save_sql(df_order)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'wangkai', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 时间
    main()




