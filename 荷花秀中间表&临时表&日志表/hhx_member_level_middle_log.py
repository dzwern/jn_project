# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/30 10:51
# @Author  : diaozhiwei
# @FileName: hhx_member_level_middle.py
# @description:首次全量更新,可以使用特定时间,，客户统计只统计所属部门的客户，不属于销售客服的客户不计入内
数据更新：
"""

from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd


# 光辉部，蜜肤语项目
def member_divide1(x):
    if x < 40:
        return 'V0'
    elif 40 <= x < 1000:
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
    if x < 40:
        return 'V0'
    elif 500 > x >= 40:
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
    if x < 40:
        return 'V0'
    elif 500 > x >= 40:
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
    if x < 40:
        return 'V0'
    elif 2000 > x >= 40:
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
    and a.add_wechat_time<'{}'
    '''.format(st)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 客户销售数据【老系统】
def get_member_order_old():
    sql='''
    SELECT
        a.member_id,
        count(DISTINCT LEFT(a.ORDER_DATE,10)) order_nums_old,
        sum(a.ORDER_MONEY) order_amounts_old
    FROM
        oldcrm_t_orders a 
    WHERE
        a.tenant_id = 11 
    AND a.ORDER_STATE in ('已签收','已发货') 
    and a.member_id>1
    and a.ORDER_DATE<'{}'
    GROUP BY a.member_id
    '''.format(st)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 客户销售数据【新系统】
def get_member_order():
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
    and a.create_time<'{}'
    GROUP BY a.member_id
    '''.format(st)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 客户2023年销售数据
def get_member_order2():
    sql = '''
    SELECT
        a.member_id,
        count(DISTINCT LEFT(a.create_time,10)) order_nums_2023,
        sum(a.order_amount) order_amounts_2023
    FROM t_orders a
    WHERE
        a.tenant_id = 11 
    and a.create_time>='2023-01-01'
    and a.create_time<'{}'
    # 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    GROUP BY a.member_id
    '''.format(st)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 老系统最近购买时间
def get_member_new_time_old():
    sql='''
    SELECT
        a.member_id,
        max(ORDER_DATE) last_time_old
    FROM
        oldcrm_t_orders a 
    WHERE
        a.tenant_id = 11 
    AND a.ORDER_STATE in ('已签收','已发货') 
    and a.member_id>1
    GROUP BY a.member_id
    '''
    df=hhx_sql1.get_DataFrame_PD(sql)
    return df


# 客户最近购买时间
def get_member_new_time():
    sql = '''
    SELECT
        a.member_id,
        max(a.create_time) last_time
    FROM t_orders a
    WHERE
        a.tenant_id = 11 
    -- 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time<'{}'
    GROUP BY a.member_id
    '''.format(st)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_member_level_middle_log` 
     (`id`,`member_id`,`wechat_name`,`wechat_number`,`user_name`,`nick_name`,
     `dept_name1`,`dept_name2`,`dept_name`,`member_level`,`order_nums`,
     `order_amounts`,`order_nums_2023`,`order_amounts_2023`,`last_time`,`log_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `id`= VALUES(`id`),`member_id`= VALUES(`member_id`),`wechat_name`= VALUES(`wechat_name`),
         `wechat_number`=VALUES(`wechat_number`),
         `user_name`=values(`user_name`),`nick_name`=values(`nick_name`),`dept_name1`=values(`dept_name1`),
         `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),`member_level`=values(`member_level`),
         `order_nums`=values(`order_nums`),`order_amounts`=values(`order_amounts`),`order_nums_2023`=values(`order_nums_2023`),
         `order_amounts_2023`=values(`order_amounts_2023`),`last_time`=values(`last_time`),`log_name`=values(`log_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 客户基础数据
    df_hhx_member = get_member_base()
    # 客户所属部门
    df_hhx_user = get_hhx_user()
    df_hhx_member = df_hhx_member.merge(df_hhx_user, on=['dept_name'], how='left')
    # 客户新系统销售数据
    df_hhx_order = get_member_order()
    df_hhx_member = df_hhx_member.merge(df_hhx_order, on=['member_id'], how='left')
    # 客户老系统销售数据
    df_hhx_order_old=get_member_order_old()
    df_hhx_member=df_hhx_member.merge(df_hhx_order_old,on=['member_id'],how='left')
    # 销售金额累加
    df_hhx_member=df_hhx_member.fillna(0)
    df_hhx_member['order_nums'] = df_hhx_member['order_nums_new'] + df_hhx_member['order_nums_old']
    df_hhx_member['order_amounts'] = df_hhx_member['order_amounts_new'] + df_hhx_member['order_amounts_old']
    # 客户销售数据2
    df_hhx_order2 = get_member_order2()
    df_hhx_member = df_hhx_member.merge(df_hhx_order2, on=['member_id'], how='left')
    # 增量更新，在之前的数据基础上加数据，需要把之前的数据减之后在加
    # 新客户最近购买时间
    df_hhx_order_time = get_member_new_time()
    df_hhx_member = df_hhx_member.merge(df_hhx_order_time, on=['member_id'], how='left')
    # 老系统最近购买时间
    df_hhx_order_time_old=get_member_new_time_old()
    # 光辉部
    df1 = df_hhx_member[df_hhx_member['dept_name1'] == '光辉部']
    df1['member_level'] = df1['order_amounts'].apply(lambda x: member_divide1(x))
    # 光芒部
    df2 = df_hhx_member[df_hhx_member['dept_name1'] == '光芒部']
    df2['member_level'] = df2.apply(lambda x: member_divide2(x['order_amounts'], x['order_nums']), axis=1)
    # 光华部
    df3 = df_hhx_member[df_hhx_member['dept_name1'] == '光华部']
    df3['member_level'] = df3.apply(lambda x: member_divide2(x['order_amounts'], x['order_nums']), axis=1)
    # 光源部蜂蜜
    df4 = df_hhx_member[df_hhx_member['dept_name2'] == '光源部蜂蜜组']
    df4['member_level'] = df4.apply(lambda x: member_divide4(x['order_amounts'], x['order_nums']), axis=1)
    # 光源部海参
    df5 = df_hhx_member[df_hhx_member['dept_name2'] == '光源部海参组']
    df5['member_level'] = df5.apply(lambda x: member_divide5(x['order_amounts'], x['order_nums']), axis=1)
    df_hhx_member = pd.concat([df1, df2, df3, df4, df5])
    df_hhx_member = df_hhx_member[['member_id', 'wechat_name', 'wechat_number', 'user_name', 'nick_name', 'dept_name1',
                                   'dept_name2', 'dept_name', 'member_level', 'order_nums', 'order_amounts',
                                   'order_nums_2023', 'order_amounts_2023', 'last_time']]
    df_hhx_member = df_hhx_member.fillna(0)
    print(df_hhx_member)
    df_hhx_member['last_time'] = df_hhx_member['last_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_hhx_member['date'] = st
    df_hhx_member['log_name'] = log_name
    df_hhx_member['id'] = df_hhx_member['member_id'].astype(str) + df_hhx_member['date']
    df_hhx_member = df_hhx_member[['id', 'member_id', 'wechat_name', 'wechat_number', 'user_name', 'nick_name',
                                   'dept_name1', 'dept_name2', 'dept_name', 'member_level', 'order_nums',
                                   'order_amounts', 'order_nums_2023', 'order_amounts_2023', 'last_time' ,'log_name']]
    save_sql(df_hhx_member)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 活动开始时间
    st = '2023-05-02'
    '''
    2023年1月初客户等级，2023年2月初客户等级，2023年3月初客户等级，2023年4月初客户等级，2023年5月初客户等级
    2023年38女神节活动前客户等级（2.15），2023年38女神节活动后客户等级（3.2），2023年51活动前客户等级（4.18），2023年51活动后客户等级（5.2）
    '''
    log_name='2023年51活动后客户等级'
    main()







