# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:00
# @Author  : diaozhiwei
# @FileName: hhx_campaign.py
# @description: 活动期间整体数据指标监控
# @update:
"""
from modules.mysql import jnmtMySQL
import pandas as pd
import numpy as np
from datetime import datetime,timedelta


# 设备客户类型
def get_campaign():
    sql = '''
    SELECT
        a.dept_name2,
        a.dept_name1,
        a.dept_name,
        count(DISTINCT a.wechat_id) group_wechats,
        count(DISTINCT a.sys_user_id)  group_users
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    and a.dept_name2 !='0'
    GROUP BY a.dept_name2,a.dept_name1,a.dept_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 销售额
def get_order_campaign():
    sql='''
    SELECT
        a.dept_name2,
        a.dept_name1,
        a.dept_name,
        count(DISTINCT a.member_id) members,
        sum(a.order_amount)  order_amounts
    FROM
        t_orders_middle a
    where a.create_time>='2023-02=01'
    and a.create_time<'2023-03-01'
    # 状态
    and a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    GROUP BY 	a.dept_name2,a.dept_name1,a.dept_name
    '''
    df=hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_campaign` 
     (`id`,`dept_name2`,`dept_name1`,`dept_name`,`group_users`,`group_wechats`,
     `members`,`order_amounts`,`amount_target`,`completion_rate`,`member_price`,
     `day_price`,`user_price`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name2`= VALUES(`dept_name2`),`dept_name1`= VALUES(`dept_name1`),`dept_name`=VALUES(`dept_name`),
         `group_users`=values(`group_users`),`group_wechats`=values(`group_wechats`),`members`=values(`members`),
         `order_amounts`=values(`order_amounts`), `amount_target`=values(`amount_target`),`completion_rate`=values(`completion_rate`),
         `member_price`=values(`member_price`),`day_price`=values(`day_price`),`user_price`=values(`user_price`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础数据
    df_campaign=get_campaign()
    # 销售数据
    df_order_campaign=get_order_campaign()
    df_campaign=df_campaign.merge(df_order_campaign,on=['dept_name2','dept_name1','dept_name'],how='left')
    # 时间
    df_campaign['days']=(datetime.now()-datetime.strptime('2023-04-01', "%Y-%m-%d")+timedelta(days=1)).days
    # 客单价
    df_campaign['member_price']=df_campaign['order_amounts']/df_campaign['members']
    # 员工人效
    df_campaign['user_price']=df_campaign['order_amounts']/df_campaign['group_users']
    # 日均价
    df_campaign['day_price']=df_campaign['order_amounts']/df_campaign['days']
    df_campaign['amount_target']=100000
    df_campaign['completion_rate']=df_campaign['order_amounts']/df_campaign['amount_target']
    df_campaign['activity_name']='2023年女神节活动'
    df_campaign['id']=df_campaign['dept_name']
    df_campaign=df_campaign[['id','dept_name2','dept_name1','dept_name','group_users','group_wechats','members','order_amounts',
                             'amount_target','completion_rate','member_price','day_price','user_price','activity_name']]
    print(df_campaign)
    df_campaign=df_campaign
    save_sql(df_campaign)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    main()