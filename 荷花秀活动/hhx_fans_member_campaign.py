# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 14:55
# @Author  : diaozhiwei
# @FileName: hhx_fans_member_campaign.py
# @description: 【不同客户在活动期间的表现】活动粉丝转化，分为活动进粉转化，活动期间新粉转化，活动期间老粉转化，实际粉丝数
# @update:
"""
from modules.mysql import jnmtMySQL
import pandas as pd
import numpy as np


# 员工基础信息
def get_user_base():
    sql = '''
    SELECT
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.wechat_id) wechat_nums
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    and a.wechat_name not in ('玫瑰诗') 
    GROUP BY a.sys_user_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 员工粉丝，客户数据源
def get_user_fans():
    sql = '''
    SELECT
        a.sys_user_id,
        a.member_category,
        sum(a.members) fans
    FROM
        t_pred_campaign_tmp a
    GROUP BY a.sys_user_id,a.member_category
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户成交金额,成交数，粉丝转换，新进粉成交
def get_member_strike():
    sql = '''
    SELECT 
        a.sys_user_id,
        'add_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.first_time>='{}'
    and a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    and a.activity_name='2023年38女神节活动'
    GROUP BY a.sys_user_id
    '''.format(st2, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 老粉成交
def get_member_strike2():
    sql = '''
    SELECT 
        a.sys_user_id,
        'old_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE  a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    and a.activity_name='2023年38女神节活动'
    GROUP BY a.sys_user_id
    '''.format(st2, et, st)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 新粉成交
def get_member_strike3():
    sql = '''
    SELECT 
        a.sys_user_id,
        'new_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.first_time>='{}'
    and a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    and a.activity_name='2023年38女神节活动'
    GROUP BY a.sys_user_id
    '''.format(st2, et, st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户转换
def get_member_struck():
    sql = '''
    SELECT 
        a.sys_user_id,
        b.member_level member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a
    LEFT JOIN  t_member_middle b on a.member_id=b.member_id
    where a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('复购日常成交','复购活动成交')
    and a.activity_name='2023年38女神节活动'
    GROUP BY a.sys_user_id,b.member_level
    ORDER BY a.sys_user_id
    '''.format(st2, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_fans_member_campaign` 
     (`id`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`,
     `dept_name2`,`dept_name`,`wechat_nums`,`member_category`,`fans`,
     `members_develop`,`member_rate`,`members_amount`,`member_price`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `wechat_nums`=values(`wechat_nums`),`member_category`=values(`member_category`),`fans`=values(`fans`),
         `members_develop`=values(`members_develop`),`member_rate`=values(`member_rate`),`members_amount`=values(`members_amount`),
         `member_price`=values(`member_price`),`activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础用户
    df_user_base=get_user_base()
    # 用户的客户数
    df_user_fans=get_user_fans()
    df_user_base = df_user_base.merge(df_user_fans, on=['sys_user_id'], how='left')
    # 员工真实消费情况
    df_member_strike = get_member_strike()
    df_member_strike2 = get_member_strike2()
    df_member_strike3 = get_member_strike3()
    df_member_struck = get_member_struck()
    df_member_strike = pd.concat([df_member_strike, df_member_strike2, df_member_strike3, df_member_struck])
    df_user_base = df_user_base.merge(df_member_strike, on=['sys_user_id', 'member_category'], how='left')
    df_user_base = df_user_base.fillna(0)
    # 转化率
    df_user_base['member_rate']=df_user_base['members_develop']/df_user_base['fans']
    # 客单价
    df_user_base['member_price']=df_user_base['members_amount']/df_user_base['members_develop']
    # 活动名称
    df_user_base['activity_name'] = '2023年5.1活动'
    df_user_base = df_user_base.replace([np.inf, -np.inf], np.nan)
    df_user_base = df_user_base.fillna(0)
    df_user_base['id'] = df_user_base['sys_user_id'].astype(str) + df_user_base['member_category'].astype(str)
    df_user_base = df_user_base[['id', 'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
                                 'wechat_nums', 'member_category', 'fans', 'members_develop', 'member_rate',
                                 'members_amount', 'member_price', 'activity_name']]
    df_user_base=df_user_base
    save_sql(df_user_base)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    st = '2022-12-20'
    st2 = '2023-02-15'
    et = '2023-03-01'
    main()
