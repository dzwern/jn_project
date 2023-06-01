# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/23 16:29
# @Author  : diaozhiwei
# @FileName: hhx_order_pred_target_campaign.py
# @description: 
# @update:活动目标预测，根据总目标反推出转化率与客单价，监控转化率与客单价的情况

注意事项：光芒部，光源部的老粉客户以销售提供为准
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
def get_member_base():
    sql = '''
    SELECT
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
    GROUP BY a.dept_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，活动总粉丝数
def get_member_category():
    sql = '''
    SELECT
        a.dept_name,
        sum(a.fans) reality_fans
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    and a.dept_name1 not in ('0')
    GROUP BY a.dept_name
        '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，新粉和活动期间新进粉丝
def get_wechat_fans(st, et):
    sql = '''
    SELECT 
        f.dept_name,
        sum(a.credit) new_fans
    FROM 
        t_wechat_fans_log a
    LEFT JOIN t_wechat d on a.wechat_id=d.id
    LEFT JOIN sys_user e on a.sys_user_id=e.user_id
    LEFT JOIN sys_dept f on e.dept_id=f.dept_id 
    WHERE a.tenant_id = 11 
    and d.valid_state=1
    AND a.new_sprint_time >= '{}' 
    AND a.new_sprint_time < '{}'
    GROUP BY f.dept_name
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 老粉客户，针对与光芒与光源部门
def get_wechat_old():
    sql = '''
    SELECT
        a.dept_name,
        sum(a.member_trans) old_fans2
    FROM
        t_wechat_middle_tmp a
    where a.sys_user_id is not null
    GROUP BY a.dept_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，客户分层人数，要到38节期间的客户数
def get_member_level():
    sql = '''
    SELECT
        a.dept_name,
        a.member_level,
        count(1) level_members
    FROM
        t_member_middle_log a
    where a.log_name='{}'
    and a.dept_name1 not in ('0')
    GROUP BY a.dept_name,a.member_level
    ORDER BY a.dept_name
    '''.format(log_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 业务目标值
def get_dept_target():
    sql = '''
    SELECT
        a.dept_name,
        a.member_category,
        a.amount_target,
        a.member_rate,
        a.member_price 
    FROM
        t_campaign_target_log2 a
    where a.activity_name='{}'
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 历史转化率与历史客单价
def get_history_rate():
    sql = '''
    SELECT
        a.dept_name,
        a.member_category,
        sum(a.members_develop)/sum(a.members) member_history_rate,
        sum(a.members_amount)/sum(a.members_develop) member_history_price
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
        a.dept_name,
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
    GROUP BY a.dept_name
    '''.format(st2, et, activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 老粉成交
def get_member_strike2():
    sql = '''
    SELECT 
        a.dept_name,
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
    GROUP BY a.dept_name
    '''.format(st, activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 新粉转换
def get_member_strike3():
    sql = '''
    SELECT 
        a.dept_name,
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
    GROUP BY a.dept_name
    '''.format(st, st2, activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户转换
def get_member_struck():
    sql = '''
    SELECT 
        a.dept_name,
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
    GROUP BY a.dept_name,b.member_level
    ORDER BY a.dept_name
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `t_pred_target_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`member_category`,
     `members`,`amount_target`,`member_rate`,`member_price`,`member_history_rate`,
     `member_history_price`,`member_current_rate`,`member_current_price`,`members_develop`,`members_amount`,
     `completion_rate`,`activity_name`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `member_category`=values(`member_category`),`members`=values(`members`),`amount_target`=values(`amount_target`),
         `member_rate`=values(`member_rate`),`member_price`=values(`member_price`),`member_history_rate`=values(`member_history_rate`),
         `member_history_price`=values(`member_history_price`),`member_current_rate`=values(`member_current_rate`),`member_current_price`=values(`member_current_price`),
         `members_develop`=values(`members_develop`),`members_amount`=values(`members_amount`),`completion_rate`=values(`completion_rate`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 当前客户数
    df_member_base = get_member_base()
    # 总粉丝数
    df_member_category = get_member_category()
    # 新粉
    df_member_new = get_wechat_fans(st, st2)
    # 新进粉
    df_member_new2 = get_wechat_fans(st2, et)
    # 老粉数
    df_old_fans=get_wechat_old()
    # 客户数
    df_member_level = get_member_level()
    # 计算各部门不同等级的客户数
    df_member_base = df_member_base.merge(df_member_category, on=['dept_name'], how='left')
    df_member_base = df_member_base.merge(df_member_new, on=['dept_name'], how='left')
    df_member_base = df_member_base.merge(df_member_new2, on=['dept_name'], how='left')
    df_member_base = df_member_base.rename(columns={'new_fans_x': 'new_fans', 'new_fans_y': 'add_fans'})
    # 客户转换
    df_member_level = df_member_level.groupby(["dept_name", "member_level", ])['level_members'].sum().reset_index()
    df_member_level = df_member_level.set_index(["dept_name", "member_level", ])["level_members"]
    df_member_level = df_member_level.unstack().reset_index()
    df_member_base = df_member_base.merge(df_member_level, on=['dept_name'], how='left')
    # 老粉
    df_member_base=df_member_base.merge(df_old_fans, on=['dept_name'], how='left')
    df_member_base = df_member_base.fillna(0)
    # 转换数值，条件赋值
    df_member_base.loc[(df_member_base["old_fans2"] > 0), "reality_fans"] = df_member_base['old_fans2']
    # 相差，光辉，光华老粉
    df1 = df_member_base[(df_member_base['dept_name1'] == '光辉部') | (df_member_base['dept_name1'] == '光华部')]
    df1['old_fans'] = df1['reality_fans'] - df1['new_fans'] - df1['add_fans'] - df1['V0'] - df1['V1'] - df1['V2'] - \
                      df1['V3'] - df1['V4'] - df1['V5']
    # 光芒，光源相差
    df2 = df_member_base[(df_member_base['dept_name1'] == '光源部') | (df_member_base['dept_name1'] == '光芒部')]
    df2['old_fans'] = df2['reality_fans'] - df2['V0'] - df2['V1'] - df2['V2'] - df2['V3'] - df2['V4'] - df2['V5']
    df_member_base = pd.concat([df1, df2])
    df_member_base = df_member_base[[
        'dept_name1', 'dept_name2', 'dept_name', 'wechat_nums', 'old_fans','new_fans', 'add_fans', 'V0','V1', 'V2', 'V3', 'V4', 'V5']]
    df_member_base=df_member_base
    # 转换，重命名
    df_member_base = pd.melt(df_member_base,id_vars=['dept_name1', 'dept_name2', 'dept_name','wechat_nums'])
    df_member_base = df_member_base.rename(columns={'variable': 'member_category', 'value': 'members'})
    # 业务目标
    df_dept_target=get_dept_target()
    # 历史转化客单
    df_history_rate=get_history_rate()
    df_member_base=df_member_base.merge(df_dept_target,on=['dept_name','member_category'],how='left')
    df_member_base=df_member_base.merge(df_history_rate,on=['dept_name','member_category'],how='left')
    # 员工真实消费情况
    df_member_strike = get_member_strike()
    df_member_strike2 = get_member_strike2()
    df_member_strike3 = get_member_strike3()
    df_member_struck = get_member_struck()
    df_member_strike = pd.concat([df_member_strike, df_member_strike2, df_member_strike3, df_member_struck])
    df_member_base = df_member_base.merge(df_member_strike, on=['dept_name', 'member_category'], how='left')
    df_member_base = df_member_base.fillna(0)
    # 当前转化率
    df_member_base['member_current_rate']=df_member_base['members_develop']/df_member_base['members']
    # 当前客单价
    df_member_base['member_current_price']=df_member_base['members_amount']/df_member_base['members_develop']
    # 完成率
    df_member_base['completion_rate']=df_member_base['members_amount']/df_member_base['amount_target']
    df_member_base = df_member_base.replace([np.inf, -np.inf], np.nan)
    df_member_base=df_member_base.fillna(0)
    df_member_base['activity_name']=activity_name
    df_member_base['id']=df_member_base['dept_name']+df_member_base['member_category']+df_member_base['activity_name']
    df_member_base = df_member_base[
        ['id','dept_name1', 'dept_name2', 'dept_name', 'member_category', 'members', 'amount_target', 'member_rate',
         'member_price', 'member_history_rate', 'member_history_price', 'member_current_rate', 'member_current_price',
         'members_develop', 'members_amount', 'completion_rate', 'activity_name']]
    save_sql(df_member_base)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    st = '2023-04-18'
    st2 = '2023-05-31'
    et = '2023-06-15'
    log_name = '2023年618活动前客户等级'
    activity_name = '2023年618活动'
    main()


