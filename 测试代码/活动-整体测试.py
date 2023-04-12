# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:00
# @Author  : diaozhiwei
# @FileName: hhx_campaign.py
# @description: 活动期间整体数据指标监控
# @update:
"""
import pandas as pd
from datetime import  datetime,timedelta
import sys
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote


userName = 'dzw'
password = 'dsf#4oHGd'
dbHost = 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com'
dbPort = 3306
URL = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/'
schema = 'crm_tm_jnmt'
schema2 = 'hhx_dx'
engine = create_engine(URL + schema + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)
engine2 = create_engine(URL + schema2 + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)


# 加载数据到df
def get_DataFrame_PD(sql='SELECT * FROM DUAL'):
    conn = engine.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 加载数据到df
def get_DataFrame_PD2(sql='SELECT * FROM DUAL'):
    conn = engine2.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 批量执行更新sql语句
def executeSqlManyByConn(sql, data):
    conn = engine2.connect()
    if len(data) > 0:
        with conn as connection:
            return connection.execute(sql, data)


# 时间转化字符串
def date2str(parameter, format='%Y-%m-%d'):
    if isinstance(parameter, str):
        return parameter
    return parameter.strftime(format)


# 设备客户类型
def get_campaign():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.wechat_id) group_wechats,
        count(DISTINCT a.sys_user_id)  group_users
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    and a.wechat_name not in ('玫瑰诗') 
    and a.dept_name2 !='0'
    GROUP BY a.dept_name2,a.dept_name1,a.dept_name
    '''
    df = get_DataFrame_PD2(sql)
    return df


# 销售额
def get_order_campaign():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount)  order_amounts
    FROM
        t_orders_middle a
    where a.create_time>='{}'
    and a.create_time<'{}'
    # 状态
    and a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    GROUP BY a.dept_name1,a.dept_name2,a.dept_name
    '''.format(st, et)
    df = get_DataFrame_PD2(sql)
    return df


# 预测目标
def get_pred_target():
    sql = '''
    SELECT
        a.dept_name,
        sum(a.members_amount) amount_target 
    FROM
        t_pred_campaign a
    GROUP BY a.dept_name
    '''
    df = get_DataFrame_PD2(sql)
    return df


# 业务目标
def get_work_target():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组', '光芒部一组',
           '光华部二组', '光华部五组', '光华部1组', '光华部六组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = [160000, 160000, 1100000, 1100000,
           900000, 900000, 800000, 900000,
           155000, 145000, 340000, 540000,
           330000, 330000, 340000, 800000]
    df = {"dept_name": df1,
          'amount_target2': df2}
    data = pd.DataFrame(df)
    return data


def save_sql(df):
    sql = '''
    INSERT INTO `t_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`group_users`,`group_wechats`,
     `members`,`order_amounts`,`amount_target`,`amount_target2`,`completion_rate`,
     `completion_rate2`,`member_price`,`user_price`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`=VALUES(`dept_name`),
         `group_users`=values(`group_users`),`group_wechats`=values(`group_wechats`),`members`=values(`members`),
         `order_amounts`=values(`order_amounts`), `amount_target`=values(`amount_target`),`amount_target2`=values(`amount_target2`),
         `completion_rate`=values(`completion_rate`),`completion_rate2`=values(`completion_rate2`),
         `member_price`=values(`member_price`),`user_price`=values(`user_price`),
         `activity_name`=values(`activity_name`)
         '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础数据
    df_campaign = get_campaign()
    # 销售数据
    df_order_campaign = get_order_campaign()
    # 预估目标
    df_pred_target = get_pred_target()
    # 业务目标
    df_work_rarget = get_work_target()
    df_campaign = df_campaign.merge(df_order_campaign, on=['dept_name1', 'dept_name2', 'dept_name'], how='left')
    df_campaign = df_campaign.merge(df_pred_target, on=['dept_name'], how='left')
    df_campaign = df_campaign.merge(df_work_rarget, on=['dept_name'], how='left')
    # 时间
    # df_campaign['days']=(datetime.now()-datetime.strptime('2023-04-01', "%Y-%m-%d")+timedelta(days=1)).days
    # 客单价
    df_campaign['member_price'] = df_campaign['order_amounts'] / df_campaign['members']
    # 员工人效
    df_campaign['user_price'] = df_campaign['order_amounts'] / df_campaign['group_users']
    # 日均价
    # df_campaign['day_price']=df_campaign['order_amounts']/df_campaign['days']
    # 目标
    df_campaign['completion_rate'] = df_campaign['order_amounts'] / df_campaign['amount_target']
    df_campaign['completion_rate2'] = df_campaign['order_amounts'] / df_campaign['amount_target2']
    df_campaign['activity_name'] = '2023年五一活动'
    df_campaign['id'] = df_campaign['dept_name'].astype(str)+df_campaign['activity_name'].astype(str)
    df_campaign = df_campaign[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'group_users', 'group_wechats', 'members', 'order_amounts',
         'amount_target', 'amount_target2', 'completion_rate', 'completion_rate2', 'member_price', 'user_price',
         'activity_name']]
    print(df_campaign)
    df_campaign = df_campaign.fillna(0)
    save_sql(df_campaign)


if __name__ == '__main__':
    # 开始时间，结束时间
    time1 = datetime.now()
    st = time1 - relativedelta(days=5)
    et = time1 + relativedelta(days=1)
    print(st,et)
    main()
