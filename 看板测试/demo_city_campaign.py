# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 14:58
# @Author  : diaozhiwei
# @FileName: demo_city_campaign.py
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

def get_dept(x):
    if x == '光辉部':
        return '1部门'
    elif x == '光华部':
        return '2部门'
    elif x == '光源部':
        return '3部门'
    elif x == '光芒部':
        return '4部门'
    else:
        return '1部门'


def get_dept2(x):
    if x == '光华部1组':
        return '小组1'
    elif x == '光华部二组':
        return '小组2'
    elif x == '光华部六组':
        return '小组3'
    elif x == '光华部五组':
        return '小组4'
    elif x == '光华部一组1':
        return '小组5'
    elif x == '光华部三组':
        return '小组6'
    elif x == '光华部七组':
        return '小组7'
    elif x == '光华部一组':
        return '小组8'
    elif x == '光辉部八组':
        return '小组1'
    elif x == '光辉部七组':
        return '小组2'
    elif x == '光辉部三组':
        return '小组3'
    elif x == '光辉部一组':
        return '小组4'
    elif x == '光辉部二组':
        return '小组5'
    elif x == '光辉部五组':
        return '小组6'
    elif x == '光辉部六组':
        return '小组7'
    elif x == '光辉组九组':
        return '小组8'
    elif x == '光芒部二组':
        return '小组1'
    elif x == '光芒部六组':
        return '小组2'
    elif x == '光芒部三组':
        return '小组3'
    elif x == '光芒部一组':
        return '小组4'
    elif x == '光源部蜂蜜八组':
        return '小组1'
    elif x == '光源部蜂蜜九组':
        return '小组2'
    elif x == '光源部蜂蜜四组':
        return '小组3'
    elif x == '光源部蜂蜜五组':
        return '小组4'
    elif x == '光源部海参七组':
        return '小组5'
    else:
        return '小组1'


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
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_address_campaign;
    '''
    hhx_sql3.executeSqlByConn(sql)


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
    df_city_order['members_rate'] = df_city_order['members'] / df_city_order['members_sum']*0.923
    df_city_order['members_amount_rate'] = df_city_order['members_amount'] / df_city_order['members_amount_sum']*1.242
    # 价值
    df_city_order['members_value'] = df_city_order['members_amount'] / df_city_order['members']*0.123
    df_city_order = df_city_order.fillna(0)
    df_city_order['activity_name'] = activity_name
    df_city_order['id'] = df_city_order['dept_name'] + df_city_order['receiver_province'] + df_city_order[
        'receiver_city'] + df_city_order['activity_name']
    df_city_order = df_city_order[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'receiver_province', 'receiver_city', 'members',
         'members_rate', 'members_amount', 'members_amount_rate', 'members_value', 'activity_name']]
    df_city_order=df_city_order
    df_city_order['dept_name1'] = df_city_order.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_city_order['dept_name'] = df_city_order.apply(lambda x: get_dept2(x['dept_name']), axis=1)
    df_city_order['members_amount']=df_city_order['members_amount']*0.5
    # del_sql()
    save_sql(df_city_order)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 活动名称  2023年五一活动，2023年38女神节活动，2023年618活动
    activity_name = '2023年618活动'
    main()
