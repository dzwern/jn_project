# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:31
# @Author  : diaozhiwei
# @FileName: hhx_order_pred_campaign.py
# @description: 活动预估，使用预估的数据进行实时监控，实时监控表，到员工
# @update：更新时间在，活动中监控

注意事项：光芒部，光源部的老粉客户以销售提供为准，预测维度为销售
"""

import pandas as pd
from datetime import datetime, timedelta
import sys
from dateutil.relativedelta import relativedelta
from urllib.parse import quote_plus as urlquote
import numpy as np
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils


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
    and a.dept_name1 not in ('0')
    GROUP BY a.sys_user_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，活动总粉丝数
def get_member_category():
    sql = '''
    SELECT
        a.sys_user_id,
        sum(a.fans) reality_fans
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    GROUP BY a.sys_user_id
        '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，新粉和活动期间新进粉丝
@utils.print_execute_time
def get_wechat_fans(st, et):
    sql = '''
    SELECT 
        a.sys_user_id,
        sum(a.credit) new_fans
    FROM 
        t_wechat_fans_log a
    LEFT JOIN t_wechat d on a.wechat_id=d.id
    WHERE a.tenant_id in ( '25', '26', '27', '28' ) 
    AND a.new_sprint_time >= '{}' 
    AND a.new_sprint_time < '{}'
    and d.valid_state=1
    GROUP BY a.sys_user_id
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 老粉客户
def get_wechat_old():
    sql = '''
    SELECT
        a.sys_user_id,
        sum(a.member_trans) old_fans2 
    FROM
        t_wechat_middle_tmp a
    where a.sys_user_id is not null
    GROUP BY a.sys_user_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，客户分层人数，要到38节期间的客户数
def get_member_level():
    sql = '''
    SELECT
        a.sys_user_id,
        a.member_level,
        count(1) level_members
    FROM
        t_member_middle a
    GROUP BY a.sys_user_id,a.member_level
    ORDER BY a.sys_user_id
    '''.format(log_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 五一活动时长
def get_campaign_time():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组', '光芒部一组',
           '光华部二组', '光华部五组', '光华部1组', '光华部六组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = [8, 8, 13, 13,
           13, 13, 13, 13,
           7, 7, 12, 12,
           13, 13, 13, 13]
    df = {"dept_name": df1,
          'activity_duration': df2}
    data = pd.DataFrame(df)
    return data


# 员工预测，以部门整体为准
def get_user_pred():
    sql = '''
    SELECT
        a.dept_name,
        a.member_category,
        avg(a.activity_duration) activity_duration2,
        sum(a.members_develop)/sum(a.members) member_rate,
        sum(a.members_amount)/sum(a.members_develop) member_price
    FROM
        t_pred_campaign_log a
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
    where a.first_time>='{}'
    and a.first_time<'{}'
    and a.activity_name='{}'
    and a.order_amount>40
    and a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交')
    GROUP BY a.sys_user_id
    '''.format(st2, et, activity_name)
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
    WHERE a.first_time<'{}'
    and a.activity_name='{}'
    and a.order_amount>40
    and a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交')
    GROUP BY a.sys_user_id
    '''.format(st, activity_name)
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
    WHERE  a.first_time>='{}'
    and a.first_time<'{}'
    and a.activity_name='{}'
    and a.order_amount>40
    and a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交')
    GROUP BY a.sys_user_id
    '''.format(st, st2, activity_name)
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
    WHERE a.activity_name='{}'
    and a.order_amount>40
    and a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('复购日常成交','复购活动成交')
    GROUP BY a.sys_user_id,b.member_level
    ORDER BY a.sys_user_id
    '''.format(activity_name)
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
    and a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    GROUP BY a.sys_user_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_pred_campaign;
    '''
    hhx_sql2.executeSqlByConn(sql)


def save_sql(df):
    sql = '''
    INSERT INTO `t_pred_campaign` 
     (`id`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`,
     `dept_name2`,`dept_name`,`wechat_nums`,`member_category`,`members`,
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
         `wechat_nums`=values(`wechat_nums`),`member_category`=values(`member_category`),`members`=values(`members`),
         `activity_duration`=values(`activity_duration`),`member_rate`=values(`member_rate`),`member_pred`=values(`member_pred`),
         `members_develop`=values(`members_develop`),`member_price`=values(`member_price`),`amount_pred`=values(`amount_pred`),
         `members_amount`=values(`members_amount`),`completion_rate`=values(`completion_rate`),`activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 员工基础信息
    df_user_base = get_user_base()
    # 员工粉丝客户情况
    # 活动前客户数据，总粉，老粉，新粉，新进粉，V1客户，V2客户，V3客户，V4客户，V5客户
    # 总粉丝
    df_member_category = get_member_category()
    # 新粉，新进粉品【38节新粉】
    df_wechat_fans_new = get_wechat_fans(st, st2)
    # 新进粉，使用38期间的新进粉，然后按照比例进行缩减
    df_wechat_fans_new2 = get_wechat_fans(st2, et)
    # 客户分层
    df_member_level = get_member_level()
    # 汇总
    df_user_base = df_user_base.merge(df_member_category, on=['sys_user_id'], how='left')
    df_user_base['sys_user_id'] = df_user_base['sys_user_id'].astype(int)
    df_user_base = df_user_base.merge(df_wechat_fans_new, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.merge(df_wechat_fans_new2, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.rename(columns={'new_fans_x': 'new_fans', 'new_fans_y': 'add_fans'})
    # 客户转换
    df_member_level = df_member_level.groupby(["sys_user_id", "member_level", ])['level_members'].sum().reset_index()
    df_member_level = df_member_level.set_index(["sys_user_id", "member_level", ])["level_members"]
    df_member_level = df_member_level.unstack().reset_index()
    df_member_level['sys_user_id'] = df_member_level['sys_user_id'].astype(int)
    df_user_base = df_user_base.merge(df_member_level, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.fillna(0)
    # 老粉客户数
    df_wechat_old = get_wechat_old()
    df_wechat_old['sys_user_id'] = df_wechat_old['sys_user_id'].astype(int)
    df_user_base=df_user_base.merge(df_wechat_old, on=['sys_user_id'], how='left')
    df_user_base = df_user_base.fillna(0)
    # 转换数值，条件赋值
    df_user_base.loc[(df_user_base["old_fans2"] > 0), "reality_fans"] = df_user_base['old_fans2']
    # 相差，光辉，光华老粉
    df1 = df_user_base[(df_user_base['dept_name1'] == '光辉部') | (df_user_base['dept_name1'] == '光华部')]
    df1['old_fans'] = df1['reality_fans'] - df1['new_fans'] - df1['add_fans']  - df1['V0'] - df1['V1'] - df1['V2'] - df1['V3'] - df1['V4'] - df1['V5']
    # 光芒，光源相差
    df2 = df_user_base[(df_user_base['dept_name1'] == '光源部') | (df_user_base['dept_name1'] == '光芒部')]
    df2['old_fans'] = df2['reality_fans']  - df1['V0'] - df2['V1'] - df2['V2'] - df2['V3'] - df2['V4'] - df2['V5']
    df_user_base = pd.concat([df1, df2])
    df_user_base = df_user_base[[
        'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name', 'wechat_nums', 'old_fans',
        'new_fans', 'add_fans', 'V0','V1', 'V2', 'V3', 'V4', 'V5']]
    # 转换，重命名
    df_user_base = pd.melt(df_user_base,
                           id_vars=['sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
                                    'wechat_nums'])
    df_user_base = df_user_base.rename(columns={'variable': 'member_category', 'value': 'members'})
    # 员工真实消费情况
    df_member_strike = get_member_strike()
    df_member_strike2 = get_member_strike2()
    df_member_strike3 = get_member_strike3()
    df_member_struck = get_member_struck()
    df_member_strike = pd.concat([df_member_strike, df_member_strike2, df_member_strike3, df_member_struck])
    df_member_strike['sys_user_id'] = df_member_strike['sys_user_id'].astype(int)
    df_user_base = df_user_base.merge(df_member_strike, on=['sys_user_id', 'member_category'], how='left')
    df_user_base = df_user_base.fillna(0)
    # 员工转化，预测情况
    df_user_pred = get_user_pred()
    df_user_base = df_user_base.merge(df_user_pred, on=['dept_name', 'member_category'], how='left')
    # 活动时长
    df_campaign_time = get_campaign_time()
    df_user_base = df_user_base.merge(df_campaign_time, on=['dept_name'], how='left')
    # 活动时长辅助列
    df_user_base['activity_duration_fuzhu'] = df_user_base['activity_duration'] / df_user_base['activity_duration2']
    # 客户过滤，将客户为负的转换为0
    df_user_base.loc[(df_user_base["members"] < 0), "members"] = 0
    # 转化率过滤，将负值转换为0，>100%的转化为100%
    df_user_base.loc[(df_user_base["member_rate"] < 0), "member_rate"] = 0
    df_user_base.loc[(df_user_base["member_rate"] > 1), "member_rate"] = 1
    # 修改客户类型为add_fans的客户数据
    df_user_base.loc[(df_user_base["member_category"] == 'add_fans'), "members"] = df_user_base['members'] * \
                                                                                   df_user_base[
                                                                                       'activity_duration_fuzhu']
    # 预测转化率
    df_user_base['member_rate'] = df_user_base['member_rate'] * df_user_base['activity_duration_fuzhu']
    # 预估成交客户数
    df_user_base['member_pred'] = df_user_base['member_rate'] * df_user_base['members']
    # 预估客单价
    df_user_base['member_price'] = df_user_base['member_price']
    # 预估成交金额
    df_user_base['amount_pred'] = df_user_base['member_pred'] * df_user_base['member_price']
    # 完成率
    df_user_base['completion_rate'] = df_user_base['members_amount'] / df_user_base['amount_pred']
    # 活动名称
    df_user_base['activity_name'] = activity_name
    df_user_base = df_user_base.replace([np.inf, -np.inf], np.nan)
    df_user_base = df_user_base.fillna(0)
    df_user_base['id'] = df_user_base['sys_user_id'].astype(str) + df_user_base['member_category'].astype(str) + \
                         df_user_base['activity_name']
    df_user_base = df_user_base[['id', 'sys_user_id', 'user_name', 'nick_name', 'dept_name1', 'dept_name2', 'dept_name',
                                 'wechat_nums', 'member_category', 'members', 'activity_duration', 'member_rate',
                                 'member_pred', 'members_develop', 'member_price', 'amount_pred', 'members_amount',
                                 'completion_rate', 'activity_name']]
    df_user_base = df_user_base
    print(df_user_base)
    save_sql(df_user_base)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    st = '2023-04-18'
    st2 = '2023-05-31'
    et = '2023-06-15'
    log_name='2023年618活动前客户等级'
    # 活动名称  2023年五一活动，2023年38女神节活动，2023年618活动
    activity_name = '2023年618活动'
    main()








