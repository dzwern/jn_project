# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/14 16:11
# @Author  : diaozhiwei
# @FileName: jnmt_member_loss.py
# @description:
客户衰退，流失
"""

import pandas as pd
from modules import jnmtMySQL


# 客户最近购买时间，时间间隔
def get_member_jiange():
    sql = '''
    SELECT
        a.member_id,
        min(a.create_time) min_time,
        max(a.create_time) max_time,
        TIMESTAMPDIFF(DAY,max(a.create_time),CURDATE()) jiange
    FROM
        jnmt_t_orders a 
    WHERE
        a.tenant_id = 11
    GROUP BY a.member_id
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 客户平均购买时间间隔
def get_member_avenger():
    sql = '''
    select 
        t2.member_id,
        avg(t2.jiange) avg_interval,
        max(t2.jiange) max_interval,
        min(t2.jiange) min_interval
    FROM
    (
    SELECT
        *,
        TIMESTAMPDIFF(DAY,t.create_time2,t.create_time) jiange
    FROM
    (
    SELECT
        a.member_id,
        a.create_time,
        lag(a.create_time,1) over(PARTITION BY a.member_id ORDER BY a.create_time) create_time2
    FROM
        jnmt_t_orders a 
    WHERE
        a.tenant_id = 11
    GROUP BY a.member_id,a.create_time
    )t
    GROUP BY t.member_id,t.create_time
    )t2
    GROUP BY 	t2.member_id
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 客户数据源
def get_member():
    sql = '''
    SELECT
        a.member_id,
        count( 1 ) cishu,
        sum(a.order_amount) orders
    FROM
        jnmt_t_orders a 
    LEFT JOIN jnmt_sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN jnmt_sys_dept c on b.dept_id=c.dept_id
    WHERE a.tenant_id=11
    GROUP BY a.member_id
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 部门
def get_dept_name():
    sql = '''
    SELECT
        a.id member_id,
        c.dept_name 
    FROM
        jnmt_t_member a
    LEFT JOIN jnmt_sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN jnmt_sys_dept c on b.dept_id=c.dept_id
    where a.tenant_id=11
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 客户分层
def member_divide1(x):
    if x < 1000:
        return 'V1'
    elif 1000 <= x < 2000:
        return 'V2'
    elif 2000 <= x < 5000:
        return 'V3'
    elif 5000 <= x < 20000:
        return 'V4'
    else:
        return 'V5'


def member_divide2(x, y):
    if x < 500:
        return 'V1'
    elif 1000 > x >= 500:
        return 'V2'
    elif 2000 > x >= 1000 and y < 4:
        return 'V2'
    elif 2000 > x >= 1000 and y >= 4:
        return 'V3'
    elif 5000 > x >= 2000:
        return 'V3'
    elif 10000 > x >= 5000:
        return 'V4'
    elif x >= 10000 and y < 7:
        return 'V4'
    elif x >= 10000 and y >= 7:
        return 'V5'


def member_divide4(x, y):
    if x < 500:
        return 'V1'
    elif 500 <= x < 1000 and y < 5:
        return 'V1'
    elif 500 <= x < 1000 and y >= 5:
        return 'V2'
    elif 1000 <= x < 2000:
        return 'V2'
    elif 2000 <= x < 5000 and y < 3:
        return 'V2'
    elif 2000 <= x < 5000 and y >= 3:
        return 'V3'
    elif 5000 <= x and y < 6:
        return 'V4'
    elif 5000 <= x and y >= 6:
        return 'V5'


def member_divide5(x, y):
    if x < 2000:
        return 'V1'
    elif 2000 <= x < 5000 and y < 2:
        return 'V1'
    elif 2000 <= x < 5000 and y >= 2:
        return 'V2'
    elif 5000 <= x < 10000:
        return 'V2'
    elif 10000 <= x < 50000 and y < 11:
        return 'V3'
    elif 10000 <= x < 50000 and y > 10:
        return 'V4'
    elif 50000 <= x < 100000:
        return 'V4'
    elif 100000 <= x:
        return 'V5'


def get_member_loss1(x, y):
    if x > y:
        return '已衰退'
    elif y > x > y * 2 / 3:
        return '衰退预警'
    elif x < y * 2 / 3:
        return '未衰退'


def get_member_loss2(x, y, z):
    if x>y:
        return '已衰退'
    elif y>x>z:
        return '衰退预警'
    elif x<=z:
        return '未衰退'

'''
1月内1-3月 4-6月	7-12月	一年以上
'''


def get_member_loss_time(x):
    if x < 30:
        return '1个月内'
    elif 30 <= x < 90:
        return '1-3月'
    elif 90 <= x < 180:
        return '4-6月'
    elif 180 <= x < 360:
        return '7-12月'
    elif x >= 360:
        return '1年以上'


def main():
    # 客户购买次数，金额
    df_member = get_member()
    # 最近购买时间
    df_member_interval = get_member_jiange()
    # 最大最小购买时间
    df_member_avenger = get_member_avenger()
    # 部门
    df_dept_name = get_dept_name()
    df_member = df_member.merge(df_dept_name, how='left', on='member_id')
    df_member = df_member.merge(df_member_interval, how='left', on='member_id')
    df_member = df_member.merge(df_member_avenger, how='left', on='member_id')
    # 客户分层
    df_member.loc[df_member['dept_name'].str.contains('光辉', na=False), 'dept_name2'] = '光辉部'
    df_member.loc[df_member['dept_name'].str.contains('光芒', na=False), 'dept_name2'] = '光芒部'
    df_member.loc[df_member['dept_name'].str.contains('光华', na=False), 'dept_name2'] = '光华部'
    df_member.loc[df_member['dept_name'].str.contains('蜂蜜', na=False), 'dept_name2'] = '光源蜂蜜部'
    df_member.loc[df_member['dept_name'].str.contains('海参', na=False), 'dept_name2'] = '光源海参部'
    # 光辉部
    df1 = df_member[df_member['dept_name2'] == '光辉部']
    df1['客户等级'] = df1['orders'].apply(lambda x: member_divide1(x))
    # print(df1)
    # 光芒部
    df2 = df_member[df_member['dept_name2'] == '光芒部']
    df2['客户等级'] = df2.apply(lambda x: member_divide2(x['orders'], x['cishu']), axis=1)
    # print(df2)
    # 光华部
    df3 = df_member[df_member['dept_name2'] == '光华部']
    df3['客户等级'] = df3.apply(lambda x: member_divide2(x['orders'], x['cishu']), axis=1)
    # print(df3)
    # 光源部蜂蜜
    df4 = df_member[df_member['dept_name2'] == '光源蜂蜜部']
    df4['客户等级'] = df4.apply(lambda x: member_divide4(x['orders'], x['cishu']), axis=1)
    # print(df4)
    # 光源部海参
    df5 = df_member[df_member['dept_name2'] == '光源海参部']
    df5['客户等级'] = df5.apply(lambda x: member_divide5(x['orders'], x['cishu']), axis=1)
    df_member2 = pd.concat([df1, df2, df3, df4, df5])
    df_member2 = df_member2.fillna(0)
    # 求客户的平均下单间隔
    df_member2['avg_interval2'] = df_member2.loc[df_member2['avg_interval'] == 0, 'avg_interval'] = df_member['jiange']
    df_member2['avg_interval2'] = (df_member2['avg_interval'] + df_member2['jiange']) / 2
    df_member3 = df_member2.groupby(['dept_name2', '客户等级'])['avg_interval2'].mean().reset_index()
    df_member2['fuzhu1'] = df_member2['dept_name2'] + df_member2['客户等级']
    df_member3['fuzhu1'] = df_member3['dept_name2'] + df_member3['客户等级']
    df_member3 = df_member3.rename(columns={'avg_interval2': '各层级平均下单间隔'})
    df_member3 = df_member3[['fuzhu1', '各层级平均下单间隔']]
    df_member2 = df_member2.merge(df_member3, how='left', on='fuzhu1')
    df_member2 = df_member2.rename(
        columns={'jiange': '最近下单间隔', 'avg_interval': '平均下单金额', 'max_interval': '最大下单间隔'})
    print(df_member2)
    df_member_loss1 = df_member2[df_member2['cishu'] == 1]
    df_member_loss1['衰退等级'] = df_member_loss1.apply(lambda x: get_member_loss1(x['最近下单间隔'], x['各层级平均下单间隔']), axis=1)
    df_member_loss1['衰退时长']=df_member_loss1['最近下单间隔']-df_member_loss1['各层级平均下单间隔']
    df_member_loss2 = df_member2[df_member2['cishu'] > 1]
    df_member_loss2['最大值']=df_member_loss2[['平均下单金额','最大下单间隔','各层级平均下单间隔']].max(axis=1)
    df_member_loss2['最小值']=df_member_loss2[['平均下单金额','最大下单间隔','各层级平均下单间隔']].min(axis=1)
    df_member_loss2['衰退等级']=df_member_loss2.apply(lambda x: get_member_loss2(x['最近下单间隔'], x['最大值'],x['最小值']), axis=1)
    df_member_loss2['衰退时长']=df_member_loss2['最近下单间隔']-df_member_loss2['最大值']
    df_member_loss = pd.concat([df_member_loss1, df_member_loss2])
    df_member_loss=df_member_loss.apply(lambda x:get_member_loss_time(x['衰退时长']),axis=1)
    df_member_loss=df_member_loss
    # 各部门各客户分层分组


if __name__ == '__main__':
    jnmt_sql = jnmtMySQL.QunaMysql('jnmt_sql')
    main()
