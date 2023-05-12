# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:40
# @Author  : diaozhiwei
# @FileName: hhx_member_level_middle2.py
客户消费等级，客户消费等级变为更新表，每次按照近5日的销售情况进行分级，
（1）购买次数，购买金额：按照5日进行更新，防止状态变更导致的影响，在活动复盘时期，可以提前运行
（2）最近购买时间：实时更新，选择符合状态进行更新
数据更新：首次使用hhx_member_level_middle进行全量更新，之后在使用此脚本进行增量更新【每次运行5天数据】
"""


from modules.mysql import jnmtMySQL
import pandas as pd
from datetime import  datetime
from modules.func import utils
import sys
from dateutil.relativedelta import relativedelta


# 光辉部，蜜肤语项目
def member_divide1(x):
    if x < 1000:
        return 'V1'
    elif 1000 <= x < 2000:
        return 'V2'
    elif 2000 <= x < 5000:
        return 'V3'
    elif 5000 <= x < 20000:
        return 'V4'
    elif x >= 20000:
        return 'V5'
    else:
        return 'V0'


# 光芒部，光华部，蜜梓源项目
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
    else:
        return 'V0'


# 光源部蜂蜜
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
    else:
        return 'V0'


# 海参
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
    else:
        return 'V0'


# 员工信息
def get_hhx_user():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组','光芒部一组',
           '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组','光华部1组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端',
           '光芒部蜜梓源后端','光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端',
           '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉后端','光华部蜜梓源面膜老粉前端','光华部蜜梓源面膜老粉后端','光华部蜜梓源面膜进粉后端',
           '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组','光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部',
           '光芒部', '光芒部', '光芒部', '光芒部',
           '光华部', '光华部', '光华部', '光华部', '光华部','光华部','光华部',
           '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name2': df2,
          'dept_name1': df3}
    data = pd.DataFrame(df)
    return data


# 客户基础数据
def get_member_base():
    sql = '''
    SELECT
        a.id member_id,
        b.wechat_name,
        b.wecaht_number wechat_number,
        c.user_name,
        c.nick_name,
        d.dept_name
    FROM
        t_member a
    LEFT JOIN t_wechat b on a.wechat_id=b.id
    LEFT JOIN sys_user c on a.sys_user_id=c.user_id
    LEFT JOIN sys_dept d on c.dept_id=d.dept_id
    where a.tenant_id=11
    '''
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 客户销售数据【新系统】
def get_member_order(st,et):
    sql = '''
    SELECT
        a.member_id,
        count(DISTINCT LEFT(a.create_time,10)) order_nums_new,
        sum(a.order_amount) order_amounts_new
    FROM t_orders a
    WHERE
        a.tenant_id = 11 
    # 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.member_id
    '''.format(st,et)
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 客户2023年销售数据
def get_member_order2(st,et):
    sql = '''
    SELECT
        a.member_id,
        count(DISTINCT LEFT(a.create_time,10)) order_nums_2023_new,
        sum(a.order_amount) order_amounts_2023_new
    FROM t_orders a
    WHERE
        a.tenant_id = 11 
    # 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.member_id
    '''.format(st,et)
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 客户最近购买时间
def get_member_new_time(st,et):
    sql = '''
    SELECT
        a.member_id,
        max(a.create_time) last_time_new
    FROM t_orders a
    WHERE
        a.tenant_id = 11 
    -- 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.member_id
    '''.format(st,et)
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 客户总表
def get_member_old():
    sql = '''
    SELECT
        a.member_id,
        a.wechat_name,
        a.wechat_number,
        a.user_name,
        a.nick_name,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.order_nums,
        a.order_amounts,
        a.order_nums_2023,
        a.order_amounts_2023,
        a.last_time
    FROM
        t_member_level_middle a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 临时表读取
def get_member_tmp():
    sql = '''
    SELECT
        a.member_id,
        a.order_nums order_nums_tmp,
        a.order_amounts order_amounts_tmp,
        a.order_nums_2023 order_nums_2023_tmp,
        a.order_amounts_2023  order_amounts_2023_tmp
    FROM
        t_member_level_tmp a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_member_level_middle` 
     (`member_id`,`wechat_name`,`wechat_number`,`user_name`,`nick_name`,
     `dept_name1`,`dept_name2`,`dept_name`,`member_level`,`order_nums`,
     `order_amounts`,`order_nums_2023`,`order_amounts_2023`,`last_time`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `member_id`= VALUES(`member_id`),`wechat_name`= VALUES(`wechat_name`),`wechat_number`=VALUES(`wechat_number`),
         `user_name`=values(`user_name`),`nick_name`=values(`nick_name`),`dept_name1`=values(`dept_name1`),
         `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),`member_level`=values(`member_level`),
         `order_nums`=values(`order_nums`),`order_amounts`=values(`order_amounts`),`order_nums_2023`=values(`order_nums_2023`),
         `order_amounts_2023`=values(`order_amounts_2023`),`last_time`=values(`last_time`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表的储存
def save_sql2(df):
    sql = '''
    INSERT INTO `t_member_level_tmp` 
     (`member_id`,`order_nums`,`order_amounts`,`order_nums_2023`,`order_amounts_2023`) 
     VALUES (%s,%s,%s,%s,%s)
     ON DUPLICATE KEY UPDATE
         `member_id`= VALUES(`member_id`),`order_nums`=values(`order_nums`),`order_amounts`=values(`order_amounts`),
         `order_nums_2023`=values(`order_nums_2023`),`order_amounts_2023`=values(`order_amounts_2023`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_member_level_tmp
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    '''最新销售数据，近N天'''
    # 客户基础数据
    df_hhx_member = get_member_base()
    # 客户所属部门
    df_hhx_user = get_hhx_user()
    df_hhx_member = df_hhx_member.merge(df_hhx_user, on=['dept_name'], how='left')
    # 客户销售数据，总计
    df_hhx_order = get_member_order(st,et)
    df_hhx_member = df_hhx_member.merge(df_hhx_order, on=['member_id'], how='left')
    # 客户销售数据2，2023年
    df_hhx_order2 = get_member_order2(st,et)
    df_hhx_member = df_hhx_member.merge(df_hhx_order2, on=['member_id'], how='left')
    # 临时表数据，历史之前的数据，存储前几天的数据
    df_member_tmp=get_member_tmp()
    '''历史销售数据，截至到昨日的数据'''
    df_member_old = get_member_old()
    # 客户最近购买时间
    df_hhx_order_time = get_member_new_time(st,et)
    # 当前客户数据
    df_hhx_member = df_hhx_member.merge(df_hhx_order_time, on=['member_id'], how='left')
    # 汇总-临时
    df_member_old=df_member_old.merge(df_member_tmp,on=['member_id'],how='left')
    df_member_old=df_member_old[['member_id','order_nums', 'order_amounts','order_nums_2023', 'order_amounts_2023',
                                 'last_time','order_nums_tmp','order_amounts_tmp','order_nums_2023_tmp',
                                 'order_amounts_2023_tmp']]
    '''储存临时表，当前时间前2天-10天数据'''
    # 当前客户关联之前客户，更新数据
    df_member_old['member_id']=df_hhx_member['member_id'].astype(int)
    df_hhx_member=df_hhx_member.merge(df_member_old,on=['member_id'], how='left')
    df_hhx_member=df_hhx_member.fillna(0)
    # 时间转换，转换为时间类型
    df_hhx_member['last_time_new'] = df_hhx_member['last_time_new'].apply(lambda x: '1900-01-01 00:00:00' if x == 0 else x)
    df_hhx_member['last_time'] = df_hhx_member['last_time'].apply(lambda x: '1900-01-01 00:00:00' if x == 0 else x)
    # 计算差值
    '''计算公式=历史销售数据-【2-10天】销售数据（储存表）+【1-10天】销售数据（脚本更新）'''
    df_hhx_member['order_numsV']=df_hhx_member['order_nums']-df_hhx_member['order_nums_tmp']+df_hhx_member['order_nums_new']
    df_hhx_member['order_amountsV'] = df_hhx_member['order_amounts'] - df_hhx_member['order_amounts_tmp'] + df_hhx_member['order_amounts_new']
    df_hhx_member['order_nums_2023V'] = df_hhx_member['order_nums_2023'] - df_hhx_member['order_nums_2023_tmp'] + df_hhx_member['order_nums_2023_new']
    df_hhx_member['order_amounts_2023V'] = df_hhx_member['order_amounts_2023'] - df_hhx_member['order_amounts_2023_tmp'] + df_hhx_member['order_amounts_2023_new']
    # 选择最近时间
    df_hhx_member['time_diff'] = ((pd.to_datetime(df_hhx_member['last_time_new']) - pd.to_datetime(df_hhx_member['last_time']))/pd.Timedelta(1, 'D')).fillna(0).astype(int)
    df_hhx_member['last_timeV']=df_hhx_member.apply(lambda x:x['last_time_new']if x['time_diff']>=0 else x['last_time'],axis=1)
    # 删除临时表数据
    del_sql()
    # 创建临时表
    df_hhx_member_tmp=df_hhx_member[['member_id','order_nums_new','order_amounts_new','order_nums_2023_new','order_amounts_2023_new']]
    save_sql2(df_hhx_member_tmp)
    # 客户等级
    df_hhx_member = df_hhx_member[['member_id', 'wechat_name', 'wechat_number', 'user_name', 'nick_name', 'dept_name1',
                                   'dept_name2', 'dept_name', 'order_numsV', 'order_amountsV',
                                   'order_nums_2023V', 'order_amounts_2023V', 'last_timeV']]
    print(df_hhx_member)
    # 光辉部
    df1 = df_hhx_member[df_hhx_member['dept_name1'] == '光辉部']
    df1['member_level'] = df1['order_amountsV'].apply(lambda x: member_divide1(x))
    # 光芒部
    df2 = df_hhx_member[df_hhx_member['dept_name1'] == '光芒部']
    df2['member_level'] = df2.apply(lambda x: member_divide2(x['order_amountsV'], x['order_numsV']), axis=1)
    # 光华部
    df3 = df_hhx_member[df_hhx_member['dept_name1'] == '光华部']
    df3['member_level'] = df3.apply(lambda x: member_divide2(x['order_amountsV'], x['order_numsV']), axis=1)
    # 光源部蜂蜜
    df4 = df_hhx_member[df_hhx_member['dept_name2'] == '光源部蜂蜜组']
    df4['member_level'] = df4.apply(lambda x: member_divide4(x['order_amountsV'], x['order_numsV']), axis=1)
    # 光源部海参
    df5 = df_hhx_member[df_hhx_member['dept_name2'] == '光源部海参组']
    df5['member_level'] = df5.apply(lambda x: member_divide5(x['order_amountsV'], x['order_numsV']), axis=1)
    df_hhx_member = pd.concat([df1, df2, df3, df4, df5])
    df_hhx_member = df_hhx_member[['member_id', 'wechat_name', 'wechat_number', 'user_name', 'nick_name', 'dept_name1',
                                   'dept_name2', 'dept_name', 'member_level', 'order_numsV', 'order_amountsV',
                                   'order_nums_2023V', 'order_amounts_2023V', 'last_timeV']]
    df_hhx_member = df_hhx_member.fillna(0)
    save_sql(df_hhx_member)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    # 开始时间，结束时间
    startTime = utils.get_time_args(sys.argv)
    time1 = startTime
    st = time1 - relativedelta(days=10)
    et = time1 - relativedelta(days=0)
    st = utils.date2str(st)
    et = utils.date2str(et)
    print(st,et)
    main()



