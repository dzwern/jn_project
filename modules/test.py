# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/2/15 15:39
# @Author  : diaozhiwei
# @FileName: jnmt_merge.py
# @description:
"""
import pandas as pd
from modules import jnmtMySQL


# 新系统订单
def jnmt_new_order():
    sql = '''
    SELECT
        a.member_id,
        b.add_wechat_time first_time,
        a.create_time,
        d.dept_name,
        a.order_amount 
    FROM
        jnmt_t_orders a
    LEFT JOIN  jnmt_t_member b on a.member_id=b.id
    LEFT JOIN jnmt_sys_user c on b.sys_user_id=c.user_id
    LEFT JOIN jnmt_sys_dept d on c.dept_id=d.dept_id
    WHERE
        a.tenant_id = 11
    -- 订单状态
    and a.order_state NOT IN (6,8,10,11)
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 老系统订单
def jnmt_old_order():
    sql = '''
    SELECT
        if(a.member_id=-1,a.CUSTOM_UUIDS,a.member_id) member_id,
        a.CUSTOM_ADD_DATE first_time,
        a.ORDER_DATE create_time,
        a.DEPT_NAME dept_name,
        a.ITEMS_NAME items_name,
        a.ORDER_MONEY order_amount
    FROM
        oldcrm_t_orders a 
    WHERE
        a.tenant_id = 11 
    and a.ORDER_DATE>='2021-08-01'
    AND a.ORDER_STATE in ('已签收','已发货')    
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 手动导入数据
def jnmt_old_order2():
    sql = '''
    SELECT
        a.member_id,
        a.first_time,
        a.create_time,
        a.dept_name2 dept_name,
        a.order_amount 
    FROM
        jnmt_t_orders3 a
    where a.order_state in ('已签收','已发货')
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 蜂蜜项目
def jnmt_fengmi_order():
    sql = '''
    SELECT
        a.member_id,
        a.first_time,
        a.create_time,
        a.dept_name,
        a.order_amount 
    FROM
        jnmt_t_orders_fengmi a
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 海参项目
def jnmt_haishen_order():
    sql = '''
    SELECT
        a.member_id,
        a.first_time,
        a.create_time,
        a.dept_name,
        a.order_amount 
    FROM
        jnmt_t_orders_haishen  a
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
     INSERT INTO `jnmt_t_orders_total` 
     (`member_id`,`first_time`,`create_time`,`dept_name`,`dept_name2`,`order_amount`) 
     VALUES (%s,%s,%s,%s,%s,%s)
     ON DUPLICATE KEY UPDATE
         `member_id`= VALUES(`member_id`),`first_time`= VALUES(`first_time`),`create_time`=VALUES(`create_time`),
         `dept_name`=values(`dept_name`),`dept_name2`=values(`dept_name2`),`order_amount`=values(`order_amount`)
     '''
    df.loc[df['first_time'] == 0, 'first_time'] = '2000-01-01'
    df.loc[df['create_time'] == 0, 'create_time'] = '2000-01-01'
    jnmt_sql.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 新系统数据
    df_new_order = jnmt_new_order()
    # 老系统数据
    df_old_order = jnmt_old_order()
    # print(df_old_order)
    df_old_order.loc[df_old_order['items_name'] == '蜜梓源蜂蜜项目', 'dept_name'] = '光源蜂蜜部'
    df_old_order = df_old_order[['member_id', 'first_time', 'create_time', 'dept_name', 'order_amount']]
    # 手动导入数据
    df_old_order2 = jnmt_old_order2()
    # 蜂蜜数据
    df_fengmi_order = jnmt_fengmi_order()
    df_fengmi_order['dept_name'] = '光源蜂蜜部'
    # 海参
    df_haishen_order = jnmt_haishen_order()
    df_haishen_order['dept_name'] = '光源海参部'
    df_order = pd.concat([df_new_order, df_old_order, df_old_order2, df_fengmi_order, df_haishen_order])
    df_order = df_order.fillna(0)
    # 增加辅助部门
    df_order.loc[df_order['dept_name'].str.contains('光辉', na=False), 'dept_name2'] = '光辉部'
    df_order.loc[df_order['dept_name'].str.contains('光芒', na=False), 'dept_name2'] = '光芒部'
    df_order.loc[df_order['dept_name'].str.contains('光华', na=False), 'dept_name2'] = '光华部'
    df_order.loc[df_order['dept_name'].str.contains('蜂蜜', na=False), 'dept_name2'] = '光源蜂蜜部'
    df_order.loc[df_order['dept_name'].str.contains('海参', na=False), 'dept_name2'] = '光源海参部'
    df_order = df_order[['member_id', 'first_time', 'create_time', 'dept_name', 'dept_name2', 'order_amount']]
    df_order = df_order.fillna(0)
    print(df_order)
    save_sql(df_order)


if __name__ == '__main__':
    jnmt_sql = jnmtMySQL.QunaMysql('jnmt_sql')
    main()




