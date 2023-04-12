# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:31
# @Author  : diaozhiwei
# @FileName: hhx_order_pred_campaign2.py
# @description: 活动预估，使用预估的数据进行实时监控，实时监控表
# @update：更新时间在，活动中监控
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


# 活动时长
def get_campaign_time():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组', '光芒部一组',
           '光华部二组', '光华部五组', '光华部1组', '光华部六组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = [7, 7, 12, 12,
           11, 11, 11, 11,
           9, 9, 9, 9,
           10, 10, 10, 10]
    df = {"dept_name": df1,
          'activity_duration': df2}
    data = pd.DataFrame(df)
    return data


# 员工预测
def get_user_pred():
    sql = '''
    SELECT
        a.dept_name,
        a.member_category,
        avg(a.activity_duration) activity_duration2,
        sum(a.members_develop)/sum(a.members) member_rate,
        sum(a.members_amount)/sum(a.members_develop) member_price
    FROM
        t_pred_campaign_tmp a
    GROUP BY a.dept_name,member_category
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
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.first_time>='{}'
    and a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    GROUP BY a.sys_user_id
    '''.format(st2, et, st2, et)
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
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    GROUP BY a.sys_user_id
    '''.format(st2, et, st)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 新粉转换
def get_member_strike3():
    sql = '''
    SELECT 
        a.sys_user_id,
        'new_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.first_time>='{}'
    and a.first_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
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
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    and a.clinch_type in ('复购日常成交','复购活动成交')
    GROUP BY a.sys_user_id,b.member_level
    ORDER BY a.sys_user_id
    '''.format(st2, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 真实成交，完成率
def get_user_order():
    sql = '''
    SELECT
        a.sys_user_id,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount) order_amounts
    FROM
        t_orders_middle a 
    WHERE a.create_time>='{}'
    and a.create_time<'{}'
    and a.order_state not in ('订单取消','订单驳回','拒收途中','拒收完结无异常','拒收完结有异常')
    GROUP BY a.sys_user_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_pred_campaign` 
     (`id`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`,
     `dept_name2`,`dept_name`,`wechat_nums`,`member_category`,`fans`,
     `activity_duration`,`member_rate`,`member_pred`,`members_develop`,`member_price`,
     `amount_pred`,`members_amount`,`completion_rate`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `wechat_nums`=values(`wechat_nums`),`member_category`=values(`member_category`),`fans`=values(`fans`),
         `activity_duration`=values(`activity_duration`),`member_rate`=values(`member_rate`),`member_pred`=values(`member_pred`),
         `members_develop`=values(`members_develop`),`member_price`=values(`member_price`),`amount_pred`=values(`amount_pred`),
         `members_amount`=values(`members_amount`),`completion_rate`=values(`completion_rate`),`activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 员工基础信息
    df_user_base = get_user_base()
    # 员工粉丝客户情况
    df_user_fans = get_user_fans()
    df_user_base = df_user_base.merge(df_user_fans, on=['sys_user_id'], how='left')
    # 员工转化，预测情况
    df_user_pred = get_user_pred()
    df_user_base = df_user_base.merge(df_user_pred, on=['dept_name', 'member_category'], how='left')
    # 活动时长
    df_campaign_time = get_campaign_time()
    df_user_base = df_user_base.merge(df_campaign_time, on=['dept_name'], how='left')
    # 员工真实消费情况
    df_member_strike = get_member_strike()
    df_member_strike2 = get_member_strike2()
    df_member_strike3 = get_member_strike3()
    df_member_struck = get_member_struck()
    df_member_strike = pd.concat([df_member_strike, df_member_strike2, df_member_strike3, df_member_struck])
    df_user_base = df_user_base.merge(df_member_strike, on=['sys_user_id', 'member_category'], how='left')
    df_user_base = df_user_base.fillna(0)
    df_user_base['activity_duration_fuzhu']=df_user_base['activity_duration']/df_user_base['activity_duration2']
    # 预估成交客户数
    df_user_base['member_pred'] = df_user_base['member_rate'] * df_user_base['fans']*df_user_base['activity_duration_fuzhu']
    # 预估成交金额
    df_user_base['amount_pred'] = df_user_base['member_pred'] * df_user_base['member_price']*df_user_base['activity_duration_fuzhu']
    # 完成率
    df_user_base['completion_rate'] = df_user_base['amount_pred'] / df_user_base['members_amount']
    # 活动名称
    df_user_base['activity_name'] = '2023年5.1活动'
    df_user_base = df_user_base.replace([np.inf, -np.inf], np.nan)
    df_user_base = df_user_base.fillna(0)
    df_user_base['id'] = df_user_base['sys_user_id'].astype(str) + df_user_base['member_category'].astype(str)
    df_user_base = df_user_base[['id', 'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
                                 'wechat_nums', 'member_category', 'fans', 'activity_duration', 'member_rate',
                                 'member_pred', 'members_develop', 'member_price', 'amount_pred', 'members_amount',
                                 'completion_rate', 'activity_name']]
    df_user_base = df_user_base
    save_sql(df_user_base)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    st = '2022-12-20'
    st2 = '2023-02-15'
    et = '2023-03-01'
    main()







