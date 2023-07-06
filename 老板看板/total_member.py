# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/15 14:08
# @Author  : diaozhiwei
# @FileName: total_member.py
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


# 远明成交客户数
def get_ym_members():
    sql = '''
    select 
        '远明酱酒' dept_name,
        left(t.first_time,4) yearly,
        '成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        crm_tm_ymlj.t_member a
    where  a.amount>0
    )t
    GROUP BY left(t.first_time,4)
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 远明未成交客户数
def get_ym_members2():
    sql = '''
    select 
        '远明酱酒' dept_name,
        left(t.first_time,4) yearly,
        '未成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        crm_tm_ymlj.t_member a
    where  a.amount=0
    )t
    GROUP BY left(t.first_time,4)
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户成交客户数
def get_zh_members(tenant_id):
    sql = '''
    select 
        t.tenant_id dept_name,
        left(t.first_time,4) yearly,
        '成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    where a.tenant_id in ({})
    and a.amount>0
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户未成交客户数
def get_zh_members2(tenant_id):
    sql = '''
    select 
        t.tenant_id dept_name,
        left(t.first_time,4) yearly,
        '未成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    where a.tenant_id in ({})
    and a.amount=0
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户成交客户数
def get_fy_members(tenant_id):
    sql = '''
    select 
        '风云酱父' dept_name,
        left(t.first_time,4) yearly,
        '成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on b.dept_id=c.dept_id
    where a.tenant_id in ({})
    and a.amount>0
    AND (c.dept_name like "%%销售客户一部%%" or c.dept_name like "%%销售客户四部%%")  
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户未成交客户数
def get_ls_members2(tenant_id):
    sql = '''
    select 
        '赖氏父子' dept_name,
        left(t.first_time,4) yearly,
        '未成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on b.dept_id=c.dept_id
    where a.tenant_id in ({})
    and a.amount=0
    AND (c.dept_name like "电商部" or c.dept_name like "_中心%%" or c.dept_name like "运营部") 
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户成交客户数
def get_ls_members(tenant_id):
    sql = '''
    select 
        '赖氏父子' dept_name,
        left(t.first_time,4) yearly,
        '成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on b.dept_id=c.dept_id
    where a.tenant_id in ({})
    and a.amount>0
    AND (c.dept_name like "电商部" or c.dept_name like "_中心%%" or c.dept_name like "运营部") 
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户未成交客户数
def get_fy_members2(tenant_id):
    sql = '''
    select 
        '风云酱父' dept_name,
        left(t.first_time,4) yearly,
        '未成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on b.dept_id=c.dept_id
    where a.tenant_id in ({})
    and a.amount=0
    AND (c.dept_name like "%%销售客户一部%%" or c.dept_name like "%%销售客户四部%%")   
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户成交客户数
def get_fy_members3(tenant_id):
    sql = '''
    select 
        '风云酱父' dept_name,
        left(t.first_time,4) yearly,
        '成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on b.dept_id=c.dept_id
    where a.tenant_id in ({})
    and a.amount>0
    AND (c.dept_name like "%%销售客户二部%%" or c.dept_name like "%%销售客户三部%%" or c.dept_name like "%%销售三部%%")  
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 租户未成交客户数
def get_fy_members4(tenant_id):
    sql = '''
    select 
        '风云酱父' dept_name,
        left(t.first_time,4) yearly,
        '未成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    SELECT
         a.id,
         a.tenant_id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on b.dept_id=c.dept_id
    where a.tenant_id in ({})
    and a.amount=0
    AND (c.dept_name like "%%销售客户二部%%" or c.dept_name like "%%销售客户三部%%")  
    )t
    GROUP BY t.tenant_id,left(t.first_time,4)
    '''.format(utils.quoted_list_func(tenant_id))
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 甜橙前端成交客户
def get_tc_members():
    sql = '''
    SELECT
        '甜橙传媒' dept_name,
        left(a.create_time,4) yearly,
        '成交客户数' member_type,
        count(DISTINCT a.member_id) members
    FROM
        t_online_retailer_plan_log a
    LEFT JOIN sys_user b on a.member_id=b.user_id
    LEFT JOIN t_online_retailer_plan c on a.online_retailer_plan_id=c.id
    LEFT JOIN sys_dept d on b.dept_id=d.dept_id
    where a.tenant_id=12
    and customer_exists=0
    and order_amount>0
    GROUP BY left(a.create_time,4) 
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 甜橙前端未成交客户
def get_tc_members2():
    sql = '''
    SELECT
        '甜橙传媒' dept_name,
        left(a.create_time,4) yearly,
        '未成交客户数' member_type,
        count(DISTINCT a.member_id) members
    FROM
        t_online_retailer_plan_log a
    LEFT JOIN sys_user b on a.member_id=b.user_id
    LEFT JOIN t_online_retailer_plan c on a.online_retailer_plan_id=c.id
    LEFT JOIN sys_dept d on b.dept_id=d.dept_id
    where a.tenant_id=12
    and customer_exists=0
    and order_amount=0
    GROUP BY left(a.create_time,4) 
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 荷花秀成交客户数
def get_hhx_members():
    sql = '''
    SELECT
        '荷花秀' dept_name,
        left(t.first_time,4) yearly,
        '成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    (
    SELECT
         a.id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    where a.tenant_id=25
    and a.create_time<='2023-05-17'
    and a.amount>0
    )
    UNION
    (
    SELECT
         a.id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    where a.tenant_id in ('25','26','27','28')
    and a.create_time>'2023-05-17'
    and a.amount>0
    )
    )t
    GROUP BY left(t.first_time,4)
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 荷花秀未成交客户数
def get_hhx_members2():
    sql = '''
    SELECT
        '荷花秀' dept_name,
        left(t.first_time,4) yearly,
        '未成交客户数' member_type,
        count(DISTINCT t.id) members
    FROM
    (
    (
    SELECT
         a.id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    where a.tenant_id=25
    and a.create_time<='2023-05-17'
    and a.amount=0
    )
    UNION
    (
    SELECT
         a.id,
         IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time,
         a.amount
    FROM
        t_member a
    where a.tenant_id in ('25','26','27','28')
    and a.create_time>'2023-05-17'
    and a.amount=0
    )
    )t
    GROUP BY left(t.first_time,4)
    '''
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
    elif x == '荷花秀':
        return '荷花秀'
    elif x == '风云酱父':
        return '风云酱父'
    elif x == '赖氏父子':
        return '赖氏父子'
    elif x == '甜橙传媒':
        return '甜橙传媒'


def save_sql(df):
    sql = '''
     INSERT INTO `jn_member_total` 
     (
     `dept_name`,`yearly`,`member_type`,`members`
     )
     VALUES (
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name`= VALUES(`dept_name`),`yearly`= VALUES(`yearly`),`member_type`= VALUES(`member_type`),
         `members`= VALUES(`members`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def del_sql():
    sql = '''
    truncate table jn_member_total;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 远明成交客户
    df_ym_member = get_ym_members()
    # 远明未成交客户
    df_ym_member2 = get_ym_members2()
    # 租户成交客户
    df_zh_member = get_zh_members(tenant_id1)
    # 租户未成交客户
    df_zh_member2 = get_zh_members2(tenant_id1)
    # 风云酱父客户
    df_fy_member = get_fy_members(tenant_id4)
    # 风云酱父未成交客户
    df_fy_member2 = get_fy_members2(tenant_id4)
    # 风云酱父客户
    df_fy_member3 = get_fy_members3(tenant_id6)
    # 风云酱父未成交客户
    df_fy_member4 = get_fy_members4(tenant_id6)
    # 赖氏成交客户
    df_ls_member = get_ls_members(tenant_id5)
    # 赖氏未成交客户
    df_ls_member2 = get_ls_members2(tenant_id5)
    # 甜橙成交客户
    df_tc_member = get_tc_members()
    # 甜橙未成交客户
    df_tc_member2 = get_tc_members2()
    # 荷花秀成交客户
    df_hhx_member = get_hhx_members()
    # 荷花秀未成交客户
    df_hhx_member2 = get_hhx_members2()
    df_member = pd.concat(
        [df_ym_member, df_ym_member2, df_zh_member, df_zh_member2, df_hhx_member, df_hhx_member2, df_fy_member,
         df_fy_member2, df_ls_member, df_ls_member2, df_tc_member, df_tc_member2, df_fy_member3, df_fy_member4])
    df_member = df_member
    df_member['dept_name'] = df_member.apply(lambda x: get_tenant(x['dept_name']), axis=1)
    df_member=df_member.fillna(0)
    # 将小于2017年的客户转换2017
    df_member.loc[df_member['yearly'].astype(int) < 2017, 'yearly'] = 2017
    df_member.loc[df_member['yearly'].astype(int) > 2023, 'yearly'] = 2023
    df_member = df_member.groupby(['dept_name', 'yearly', 'member_type'])['members'].sum().reset_index()
    df_member = df_member[['dept_name', 'yearly', 'member_type', 'members']]
    print(df_member)
    del_sql()
    save_sql(df_member)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'wangkai', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    tenant_id1 = ['3', '5']
    tenant_id2 = ['11']
    tenant_id3 = ['25', '26', '27', '28']
    tenant_id4 = ['8']
    tenant_id5 = ['10']
    tenant_id6 = ['21']
    main()
