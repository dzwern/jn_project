# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/15 14:08
# @Author  : diaozhiwei
# @FileName: total_fans.py
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


# 远明电销预算
def get_ym_budget(st, et):
    sql = '''
    SELECT
        '远明酱酒' dept_name,
        '电销' promotion_type, 
        left(a.promotion_date,7) monthly,
        IFNULL( sum(a.promotion_budget ), 0.00 ) promotion_budget
    FROM crm_tm_ymlj.t_promotion a
    WHERE a.promotion_date >= '{}' 
        AND a.promotion_date < '{}' 
        AND a.promotion_type = 1 
        AND a.deleted = 0 
    GROUP BY left(a.promotion_date,7)
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 远明网销预算
def get_ym_budget2(st, et):
    sql = '''
    SELECT
        '远明酱酒' dept_name,
        '网销' promotion_type, 
        left(b.create_time,7) monthly,
        IFNULL( sum( budget_amount ), 0.00 ) AS promotion_budget 
    FROM
        crm_tm_ymlj.t_online_retailer_plan a
    INNER JOIN crm_tm_ymlj.t_online_retailer_plan_budget b ON a.id = b.online_retailer_plan_id 
    WHERE
        b.create_time >= '{}' 
    AND b.create_time < '{}' 
    GROUP BY left(b.create_time,7)
    UNION ALL
    SELECT
        '远明酱酒' dept_name,
        '网销' promotion_type, 
        left(a.promotion_date,7) monthly,
        IFNULL( sum(a.promotion_budget ), 0.00 ) promotion_budget
    FROM
        crm_tm_ymlj.t_promotion a
    WHERE a.promotion_date >= '{}' 
        AND a.promotion_date  < '{}'  
        AND a.promotion_type = 2 
        AND a.deleted = 0 
    GROUP BY left(a.promotion_date,7)
    '''.format(st, et, st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户电销预算
def get_zh_budget(st, et, tenant_id):
    sql = '''
    SELECT
        a.tenant_id dept_name,
        '电销' promotion_type, 
        left(a.promotion_date,7) monthly,
        IFNULL( sum(a.promotion_budget ), 0.00 ) promotion_budget
    FROM t_promotion a
    WHERE a.promotion_date >= '{}' 
        AND a.promotion_date < '{}' 
        AND a.promotion_type = 1 
        AND a.deleted = 0 
        and a.tenant_id in ({})
    GROUP BY a.tenant_id,left(a.promotion_date,7)
    '''.format(st, et, utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户网销预算
def get_zh_budget2(st, et, tenant_id):
    sql = '''
    SELECT
        a.tenant_id dept_name,
        '网销' promotion_type, 
        left(b.create_time,7) monthly,
        IFNULL( sum( budget_amount ), 0.00 ) AS promotion_budget 
    FROM
        t_online_retailer_plan a
    INNER JOIN t_online_retailer_plan_budget b ON a.id = b.online_retailer_plan_id 
    WHERE
        b.create_time >= '{}' 
    AND b.create_time <= '{}' 
    and a.tenant_id in ({})
    GROUP BY a.tenant_id,left(b.create_time,7)
    UNION ALL
    SELECT
        a.tenant_id dept_name,
        '网销' promotion_type, 
        left(a.promotion_date,7) monthly,
        IFNULL( sum(a.promotion_budget ), 0.00 ) promotion_budget
    FROM
        t_promotion a
    WHERE a.promotion_date >= '{}' 
        AND a.promotion_date <= '{}' 
        AND a.promotion_type = 2 
        AND a.deleted = 0 
        and a.tenant_id in ({})
    GROUP BY a.tenant_id,left(a.promotion_date,7)
    '''.format(st, et, utils.quoted_list_func(tenant_id), st, et, utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 远明电销进粉
def get_ym_fans(st, et):
    sql = '''
    SELECT
        '远明酱酒' dept_name, 
        '电销' promotion_type, 
        left(a.promotion_date,7) monthly,
        count(b.id) fans
        FROM crm_tm_ymlj.t_promotion a
    INNER JOIN crm_tm_ymlj.t_member_clue b ON b.promotion_id = a.promotion_id 
    where a.promotion_date>='{}'
        and a.promotion_date<'{}'
        and a.deleted=0
        AND a.promotion_type = 1
    GROUP BY left(a.promotion_date,7) 
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 远明网销进粉
def get_ym_fans2(st, et):
    sql = '''
    SELECT
        '远明酱酒' dept_name, 
        '网销' promotion_type, 
        left(a.create_time,7) monthly,
         count(DISTINCT a.id) fans
    FROM
        crm_tm_ymlj.t_member a 
    WHERE
        a.member_source=3
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY left(a.create_time,7)
    UNION ALL	
    SELECT
        '远明酱酒' dept_name, 
        '网销' promotion_type, 
        left(a.promotion_date,7) monthly,
        sum(c.credit)  
    FROM
        t_promotion a
    LEFT JOIN t_promotion_allocation b on a.promotion_id=b.promotion_id
    LEFT JOIN t_wechat_fans_log c on b.we_chat_id=c.wechat_id and a.promotion_date=c.new_sprint_time
    where  a.promotion_date>='{}'
    and a.promotion_date<'{}'
    and b.deleted=0
    AND promotion_type = 2 
    GROUP BY left(a.promotion_date,7)
    '''.format(st, et, st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户电销进粉
def get_zh_fans(st, et, tenant_id):
    sql = '''
    SELECT
        a.tenant_id dept_name, 
        '电销' promotion_type, 
        left(a.promotion_date,7) monthly,
        count(b.id) fans
    FROM t_promotion a
    INNER JOIN t_member_clue b ON b.promotion_id = a.promotion_id and b.follow_state in (0,1)
    where a.promotion_date>='{}'
    and a.promotion_date<'{}'
    and a.deleted=0
    AND a.promotion_type = 1
    and a.tenant_id in ({})
    GROUP BY a.tenant_id,left(a.promotion_date,7) 
    '''.format(st, et, utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户网销进粉
def get_zh_fans2(st, et, tenant_id):
    sql = '''
    SELECT
        a.tenant_id dept_name,
        '网销' promotion_type, 
        left(a.create_time,7) monthly,
         count(DISTINCT a.id) fans
    FROM
        t_member a 
    WHERE
        a.member_source=3
        and a.create_time>='{}'
        and a.create_time<'{}'
        and a.tenant_id in ({})
    GROUP BY a.tenant_id,left(a.create_time,7)
    UNION ALL
    SELECT
        a.tenant_id dept_name,
        '网销' promotion_type, 
        left(a.promotion_date,7) monthly,
        sum(c.credit)  
    FROM
        t_promotion a
    LEFT JOIN t_promotion_allocation b on a.promotion_id=b.promotion_id
    LEFT JOIN t_wechat_fans_log c on b.we_chat_id=c.wechat_id and a.promotion_date=c.new_sprint_time
    where  a.promotion_date>='{}'
        and a.promotion_date<'{}'
        and b.deleted=0
        AND promotion_type = 2 
        and a.tenant_id in ({})
    GROUP BY  a.tenant_id,left(a.promotion_date,7)
    '''.format(st, et, utils.quoted_list_func(tenant_id), st, et, utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 远明电销产出
def get_ym_order(st, et):
    sql = '''
    select 
        '远明酱酒' dept_name, 
        '电销' promotion_type, 
        left(t.first_time,7) monthly,
        sum(t.order_amount) order_amounts_t
    FROM	
    (
    SELECT
        a.member_id,
        IF(c.add_wechat_time is NULL,c.incoming_line_time,c.add_wechat_time) first_time,
        a.create_time,
        a.order_amount
    FROM
        crm_tm_ymlj.t_orders a 
    LEFT JOIN crm_tm_ymlj.t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN crm_tm_ymlj.t_member c on a.member_id=c.id
    WHERE
     a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
     -- 订单类型
    AND b.no_performance_type in (2,3,5,6,14)
    and c.member_source in ('3')
    GROUP BY a.id
    )t
    where t.first_time>='{}'
    and t.first_time<'{}'
    and t.create_time>='{}'
    and t.create_time<'{}'
    GROUP BY left(t.first_time,7)
    '''.format(st, et, st, et, st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 远明网销产出
def get_ym_order2(st, et):
    sql = '''
    select 
        '远明酱酒' dept_name,
        '网销' promotion_type,  
        left(t.first_time,7) monthly,
        sum(t.order_amount) order_amounts_t
    FROM	
    (
    SELECT
        a.member_id,
        IF(c.add_wechat_time is NULL,c.incoming_line_time,c.add_wechat_time) first_time,
        a.create_time,
        a.order_amount
    FROM
        crm_tm_ymlj.t_orders a 
    LEFT JOIN crm_tm_ymlj.t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN crm_tm_ymlj.t_member c on a.member_id=c.id
    WHERE
     a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
     -- 订单类型
    AND b.no_performance_type in (2,3,5,6,14)
    and c.member_source in ('1','2')
    GROUP BY a.id
    )t
    where t.first_time>='{}'
    and t.first_time<'{}'
    and t.create_time>='{}'
    and t.create_time<'{}'
    GROUP BY left(t.first_time,7)
    '''.format(st, et, st, et, st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户电销产出
def get_zh_order(st, et, tenant_id):
    sql = '''
    select 
        t.tenant_id dept_name,
        '电销' promotion_type, 
        left(t.first_time,7) monthly,
        sum(t.order_amount) order_amounts_t
    FROM	
    (
    SELECT
            a.member_id,
            a.tenant_id,
            IF(c.add_wechat_time is NULL,c.incoming_line_time,c.add_wechat_time) first_time,
            a.create_time,
            a.order_amount
    FROM
            t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN t_member c on a.member_id=c.id
    WHERE
     a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
     -- 订单类型
    AND b.no_performance_type in (2,3,5,6,14)
    and c.member_source in ('3')
    and a.tenant_id in ({})
    GROUP BY a.id
    )t
    where t.first_time>='{}'
    and t.first_time<'{}'
    and t.create_time>='{}'
    and t.create_time<'{}'
    GROUP BY t.tenant_id,left(t.first_time,7)
    '''.format(st, et, utils.quoted_list_func(tenant_id), st, et, st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户网销产出
def get_zh_order2(st, et, tenant_id):
    sql = '''
    select 
        t.tenant_id dept_name,
        '网销' promotion_type, 
        left(t.first_time,7) monthly,
        sum(t.order_amount) order_amounts_t
    FROM	
    (
    SELECT
            a.member_id,
            a.tenant_id,
            IF(c.add_wechat_time is NULL,c.incoming_line_time,c.add_wechat_time) first_time,
            a.create_time,
            a.order_amount
    FROM
            t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN t_member c on a.member_id=c.id
    WHERE
     a.create_time >= '{}'
    and a.create_time<'{}'
     -- 订单状态
    AND a.order_state in (0,1,2,3,4,5,7,12,13,14,15,16,17)
     -- 订单类型
    AND b.no_performance_type in (2,3,5,6,14)
    and c.member_source in ('1','2')
    and a.tenant_id in ({})
    GROUP BY a.id
    )t
    where t.first_time>='{}'
    and t.first_time<'{}'
    and t.create_time>='{}'
    and t.create_time<'{}'
    GROUP BY t.tenant_id,left(t.first_time,7)
    '''.format(st, et, utils.quoted_list_func(tenant_id), st, et, st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户枚举表
def get_tenant(x):
    if x == 3:
        return '女儿红'
    elif x == 5:
        return '碧春酒业'
    elif x == 8:
        return '风云酱父'
    elif x == 21:
        return '风云酱父'
    elif x == 12:
        return '甜橙传媒'
    elif x == 10:
        return '赖氏父子'
    elif x == 25:
        return '荷花秀'
    elif x == 26:
        return '荷花秀'
    elif x == 27:
        return '荷花秀'
    elif x == 28:
        return '荷花秀'
    elif x == 11:
        return '荷花秀'
    elif x == '远明酱酒':
        return '远明酱酒'


def save_sql(df):
    sql = '''
     INSERT INTO `jn_fans_total` 
     (
     `dept_name`,`monthly`,`promotion_type`,`promotion_budget`,`fans`,
     `fans_price`,`order_amounts_t`,`amounts_ratio_t`
     )
     VALUES (
     %s,%s,%s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name`= VALUES(`dept_name`),`monthly`= VALUES(`monthly`),`promotion_type`= VALUES(`promotion_type`),
         `promotion_budget`= VALUES(`promotion_budget`),`fans`= VALUES(`fans`),`fans_price`= VALUES(`fans_price`),
         `order_amounts_t`= VALUES(`order_amounts_t`),`amounts_ratio_t`= VALUES(`amounts_ratio_t`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def del_sql():
    sql = '''
    truncate table jn_fans_total;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 预算统计
    # 远明电销推广预算
    df_ym_budget = get_ym_budget(st1, et)
    # 远明网销推广预算
    df_ym_budget2 = get_ym_budget2(st1, et)
    # 租户电销推广预算
    df_zh_budget = get_zh_budget(st1, et, tenant_id1)
    # 租户网销推广预算
    df_zh_budget2 = get_zh_budget2(st1, et, tenant_id1)
    # 荷花秀电销推广预算，未切割系统
    df_hhx_budget = get_zh_budget(st1, st3, tenant_id2)
    # 切割系统
    df_hhx_budget2 = get_zh_budget(st3, et, tenant_id3)
    # 荷花秀网销推广预算，未切割系统
    df_hhx_budget3 = get_zh_budget2(st1, st3, tenant_id2)
    # 切割系统
    df_hhx_budget4 = get_zh_budget2(st3, et, tenant_id3)
    df_budget = pd.concat(
        [df_ym_budget, df_ym_budget2, df_zh_budget, df_zh_budget2, df_hhx_budget, df_hhx_budget2, df_hhx_budget3,
         df_hhx_budget4])
    df_budget['dept_name'] = df_budget.apply(lambda x: get_tenant(x['dept_name']), axis=1)
    df_budget = df_budget.groupby(['dept_name', 'promotion_type', 'monthly'])['promotion_budget'].sum().reset_index()
    # 进粉
    # 远明电销进粉
    df_ym_fans = get_ym_fans(st1, et)
    # 远明网销进粉
    df_ym_fans2 = get_ym_fans2(st1, et)
    # 租户电销进粉
    df_zh_fans = get_zh_fans(st1, et, tenant_id1)
    # 组户网销进粉
    df_zh_fans2 = get_zh_fans2(st1, et, tenant_id1)
    # 荷花秀电销进粉，未切割系统
    df_hhx_fans = get_zh_fans(st1, st3, tenant_id2)
    # 切割系统
    df_hhx_fans2 = get_zh_fans(st3, et, tenant_id3)
    # 荷花秀网销进粉，未切割系统
    df_hhx_fans3 = get_zh_fans2(st1, st3, tenant_id2)
    # 切割系统
    df_hhx_fans4 = get_zh_fans2(st3, et, tenant_id3)
    df_fans = pd.concat(
        [df_ym_fans, df_ym_fans2, df_zh_fans, df_zh_fans2, df_hhx_fans, df_hhx_fans2, df_hhx_fans3, df_hhx_fans4])
    df_fans['dept_name'] = df_fans.apply(lambda x: get_tenant(x['dept_name']), axis=1)
    df_fans = df_fans.groupby(['dept_name', 'promotion_type', 'monthly'])['fans'].sum().reset_index()
    df_fans = df_fans
    # 产出
    # 远明电销累计产出
    df_ym_order = get_ym_order(st1, et)
    # 远明网销累计产出
    df_ym_order2 = get_ym_order2(st1, et)
    # 组户电销累计产出
    df_zh_order = get_zh_order(st1, et, tenant_id1)
    # 组合网销累计产出
    df_zh_order2 = get_zh_order2(st1, et, tenant_id1)
    # 荷花秀电销产出，未切割系统
    df_hhx_order = get_zh_order(st1, st3, tenant_id2)
    # 切割系统
    df_hhx_order2 = get_zh_order(st3, et, tenant_id3)
    # 荷花秀网销产出，未切割系统
    df_hhx_order3 = get_zh_order2(st1, st3, tenant_id2)
    # 切割系统
    df_hhx_order4 = get_zh_order2(st3, et, tenant_id3)
    df_order = pd.concat(
        [df_ym_order, df_ym_order2, df_zh_order, df_zh_order2, df_hhx_order, df_hhx_order2, df_hhx_order3,
         df_hhx_order4])
    df_order['dept_name'] = df_order.apply(lambda x: get_tenant(x['dept_name']), axis=1)
    df_order = df_order.groupby(['dept_name', 'promotion_type', 'monthly'])['order_amounts_t'].sum().reset_index()
    df_order = df_order
    df_fans_total1 = df_budget[['dept_name', 'promotion_type', 'monthly']]
    df_fans_total2 = df_fans[['dept_name', 'promotion_type', 'monthly']]
    df_fans_total3 = df_order[['dept_name', 'promotion_type', 'monthly']]
    df_fans_total = pd.concat([df_fans_total1, df_fans_total2, df_fans_total3])
    df_fans_total = df_fans_total.drop_duplicates()
    df_fans_total = df_fans_total.merge(df_fans, on=['dept_name', 'promotion_type', 'monthly'], how='left')
    df_fans_total = df_fans_total.merge(df_budget, on=['dept_name', 'promotion_type', 'monthly'], how='left')
    df_fans_total = df_fans_total.merge(df_order, on=['dept_name', 'promotion_type', 'monthly'], how='left')
    df_fans_total=df_fans_total.fillna(0)
    df_fans_total['fans_price']=df_fans_total['promotion_budget']/df_fans_total['fans']
    df_fans_total['amounts_ratio_t']=df_fans_total['order_amounts_t']/df_fans_total['promotion_budget']
    df_fans_total=df_fans_total[['dept_name', 'monthly','promotion_type','promotion_budget','fans','fans_price','order_amounts_t','amounts_ratio_t']]
    df_fans_total = df_fans_total.replace([np.inf, -np.inf], np.nan)
    df_fans_total=df_fans_total.fillna(0)
    print(df_fans_total)
    del_sql()
    save_sql(df_fans_total)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'wangkai', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 时间
    now = datetime.now().date()
    st1 = '2023-01-01'
    # 本月
    st2 = datetime(now.year, now.month, 1)
    st3 = '2023-05-17'
    et = now + timedelta(days=1)
    print(st1, st2, st3, et)
    tenant_id1 = ['3', '5', '8', '21', '12', '10']
    tenant_id2 = ['11']
    tenant_id3 = ['25', '26', '27', '28']
    main()




