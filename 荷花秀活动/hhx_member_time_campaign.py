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
import datetime
import sys
from modules.func import utils
from dateutil.relativedelta import relativedelta


# 设备客户类型
def get_campaign_time():
    sql = '''
    SELECT
        a.id,
        a.order_sn,
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.wechat_id,
        a.wechat_name,
        a.wechat_number,
        a.member_id,
        a.first_time,
        left(a.first_time,7) year_months,
        year(a.first_time) years,
        a.create_time,
        a.time_diff,
        a.order_amount 
    FROM
        t_orders_middle a
    WHERE a.activity_name='2023年五一活动'
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_time_level(x):
    if datetime.datetime.strptime('2023-01-01','%Y-%m-%d') <= x:
        return '增量'
    else:
        return '存量'


def save_sql(df):
    sql = '''
    INSERT INTO `t_member_time_campaign` 
     (`id`,`order_sn`,`dept_name1`,`dept_name2`,`dept_name`,
     `sys_user_id`,`user_name`,`nick_name`,`wechat_id`,`wechat_name`,
     `wechat_number`,`member_id`,`first_time`,`stock_increment`,`year_months`,
     `years`,`create_time`,`time_diff`,`order_amount`,`activity_name`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_sn`= VALUES(`order_sn`),`dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),
         `dept_name`=values(`dept_name`),`sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),
         `nick_name`=values(`nick_name`),`wechat_id`=values(`wechat_id`),`wechat_name`=values(`wechat_name`),
         `wechat_number`=values(`wechat_number`),`member_id`=values(`member_id`),`first_time`=values(`first_time`),
         `stock_increment`=values(`stock_increment`),`year_months`=values(`year_months`),`years`=values(`years`),
         `create_time`=values(`create_time`),
         `time_diff`=values(`time_diff`),`order_amount`=values(`order_amount`),`activity_name`=values(`activity_name`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础数据
    df_member_time=get_campaign_time()
    df_member_time['activity_name'] = '2023年5.1活动'
    # 存量，增量
    df_member_time['stock_increment'] = df_member_time.apply(lambda x: get_time_level(x['first_time']), axis=1)
    df_member_time=df_member_time[['id','order_sn','dept_name1','dept_name2','dept_name','sys_user_id','user_name',
                                   'nick_name','wechat_id','wechat_name','wechat_number','member_id','first_time',
                                   'stock_increment','year_months','years','create_time','time_diff','order_amount',
                                   'activity_name']]
    save_sql(df_member_time)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    main()



