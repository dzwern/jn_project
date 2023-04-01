# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:40
# @Author  : diaozhiwei
# @FileName: hhx_member_order_middle2.py
# @description: 
# @update:
"""


from modules.mysql import jnmtMySQL
import pandas as pd


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
    elif x >= 5000 and y < 7:
        return 'V4'
    elif x >= 5000 and y >= 7:
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
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组', '光芒部二组', '光芒部六组', '光芒部三组',
           '光芒部一组', '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端', '光芒部蜜梓源后端',
           '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光华部蜜梓源面膜进粉前端',
           '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉后端',
           '光华部蜜梓源面膜老粉前端', '光华部蜜梓源面膜老粉后端', '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组',
           '光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部', '光芒部', '光芒部', '光芒部', '光芒部', '光华部', '光华部', '光华部',
           '光华部', '光华部', '光华部', '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name1': df2,
          'dept_name2': df3}
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


# 客户销售数据
def get_member_order():
    sql = '''
    SELECT
        a.member_id,
        count(DISTINCT LEFT(a.create_time,10)) order_nums,
        sum(a.order_amount) order_amounts
    FROM t_orders a
    WHERE
        a.tenant_id = 11 
    # 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time>='2023-02-16'
    and a.create_time<'2023-02-17'
    GROUP BY a.member_id
    '''
    df = hhx_sql.get_DataFrame_PD(sql)
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
    # 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time>='2023-02-16'
    and a.create_time<'2023-02-17'
    GROUP BY a.member_id
    '''
    df = hhx_sql.get_DataFrame_PD(sql)
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
    GROUP BY a.member_id
    '''
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 客户总表
def get_member():
    sql='''
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
        a.order_amounts_2023
    FROM
        t_member_level a
    '''




def save_sql(df):
    sql = '''
    INSERT INTO `t_member_level` 
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


def main():
    # 客户基础数据
    df_hhx_member = get_member_base()
    # 客户所属部门
    df_hhx_user = get_hhx_user()
    df_hhx_member = df_hhx_member.merge(df_hhx_user, on=['dept_name'], how='left')
    # 客户销售数据
    df_hhx_order = get_member_order()
    df_hhx_member = df_hhx_member.merge(df_hhx_order, on=['member_id'], how='left')
    # 客户销售数据2
    df_hhx_order2 = get_member_order2()
    df_hhx_member = df_hhx_member.merge(df_hhx_order2, on=['member_id'], how='left')
    # 增量更新，在之前的数据基础上加数据，需要把之前的数据减之后在加
    # 客户最近购买时间
    df_hhx_order_time = get_member_new_time()
    # 历史

    df_hhx_member = df_hhx_member.merge(df_hhx_order_time, on=['member_id'], how='left')
    # 光辉部
    df1 = df_hhx_member[df_hhx_member['dept_name2'] == '光辉部']
    df1['member_level'] = df1['order_amounts'].apply(lambda x: member_divide1(x))
    # 光芒部
    df2 = df_hhx_member[df_hhx_member['dept_name2'] == '光芒部']
    df2['member_level'] = df2.apply(lambda x: member_divide2(x['order_amounts'], x['order_nums']), axis=1)
    # 光华部
    df3 = df_hhx_member[df_hhx_member['dept_name2'] == '光华部']
    df3['member_level'] = df3.apply(lambda x: member_divide2(x['order_amounts'], x['order_nums']), axis=1)
    # 光源部蜂蜜
    df4 = df_hhx_member[df_hhx_member['dept_name1'] == '光源部蜂蜜组']
    df4['member_level'] = df4.apply(lambda x: member_divide4(x['order_amounts'], x['order_nums']), axis=1)
    # 光源部海参
    df5 = df_hhx_member[df_hhx_member['dept_name1'] == '光源部海参组']
    df5['member_level'] = df5.apply(lambda x: member_divide5(x['order_amounts'], x['order_nums']), axis=1)
    df_hhx_member = pd.concat([df1, df2, df3, df4, df5])
    df_hhx_member = df_hhx_member[['member_id', 'wechat_name', 'wechat_number', 'user_name', 'nick_name', 'dept_name1',
                                   'dept_name2', 'dept_name', 'member_level', 'order_nums', 'order_amounts',
                                   'order_nums_2023', 'order_amounts_2023', 'last_time']]
    df_hhx_member = df_hhx_member.fillna(0)
    print(df_hhx_member)
    df_hhx_member['last_time'] = df_hhx_member['last_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    save_sql(df_hhx_member)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    # 开始时间，结束时间
    # startTime = utils.get_time_args(sys.argv)
    # time1 = startTime
    # st = time1 - relativedelta(days=1)
    # et = time1 - relativedelta(days=0)
    # 时间转化
    st = '2023-02-01'
    et = '2023-03-01'
    main()
