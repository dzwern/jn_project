# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:00
# @Author  : diaozhiwei
# @FileName: demo_campaign.py
# @description: 活动期间整体数据指标监控
# @update:
"""
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd
import numpy as np
import datetime
import sys
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
        a.order_amount,
        a.activity_name 
    FROM
        t_orders_middle a
    WHERE a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.activity_name='{}'
    and a.order_amount>40
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_time_level(x):
    if datetime.datetime.strptime('2023-01-01','%Y-%m-%d') <= x:
        return '增量'
    else:
        return '存量'


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_member_time_campaign;
    '''
    hhx_sql3.executeSqlByConn(sql)

def get_dept(x):
    if x == '光辉部':
        return '1部门'
    elif x == '光华部':
        return '2部门'
    elif x == '光源部':
        return '3部门'
    elif x == '光芒部':
        return '4部门'
    else:
        return '1部门'


def get_dept2(x):
    if x == '光华部1组':
        return '小组1'
    elif x == '光华部二组':
        return '小组2'
    elif x == '光华部六组':
        return '小组3'
    elif x == '光华部五组':
        return '小组4'
    elif x == '光华部一组1':
        return '小组5'
    elif x == '光华部三组':
        return '小组6'
    elif x == '光华部七组':
        return '小组7'
    elif x == '光华部一组':
        return '小组8'
    elif x == '光辉部八组':
        return '小组1'
    elif x == '光辉部七组':
        return '小组2'
    elif x == '光辉部三组':
        return '小组3'
    elif x == '光辉部一组':
        return '小组4'
    elif x == '光辉部二组':
        return '小组5'
    elif x == '光辉部五组':
        return '小组6'
    elif x == '光辉部六组':
        return '小组7'
    elif x == '光辉组九组':
        return '小组8'
    elif x == '光芒部二组':
        return '小组1'
    elif x == '光芒部六组':
        return '小组2'
    elif x == '光芒部三组':
        return '小组3'
    elif x == '光芒部一组':
        return '小组4'
    elif x == '光源部蜂蜜八组':
        return '小组1'
    elif x == '光源部蜂蜜九组':
        return '小组2'
    elif x == '光源部蜂蜜四组':
        return '小组3'
    elif x == '光源部蜂蜜五组':
        return '小组4'
    elif x == '光源部海参七组':
        return '小组5'
    else:
        return '小组1'

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
         `id`= VALUES(`id`),
         `order_sn`= VALUES(`order_sn`),`dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),
         `dept_name`=values(`dept_name`),`sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),
         `nick_name`=values(`nick_name`),`wechat_id`=values(`wechat_id`),`wechat_name`=values(`wechat_name`),
         `wechat_number`=values(`wechat_number`),`member_id`=values(`member_id`),`first_time`=values(`first_time`),
         `stock_increment`=values(`stock_increment`),`year_months`=values(`year_months`),`years`=values(`years`),
         `create_time`=values(`create_time`),
         `time_diff`=values(`time_diff`),`order_amount`=values(`order_amount`),`activity_name`=values(`activity_name`)
     '''
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础数据
    df_member_time=get_campaign_time()
    # 存量，增量
    df_member_time['stock_increment'] = df_member_time.apply(lambda x: get_time_level(x['first_time']), axis=1)

    df_member_time=df_member_time[['id','order_sn','dept_name1','dept_name2','dept_name','sys_user_id','user_name',
                                   'nick_name','wechat_id','wechat_name','wechat_number','member_id','first_time',
                                   'stock_increment','year_months','years','create_time','time_diff','order_amount',
                                   'activity_name']]

    df_member_time['dept_name1'] = df_member_time.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_member_time['dept_name'] = df_member_time.apply(lambda x: get_dept2(x['dept_name']), axis=1)
    # del_sql()
    print(df_member_time)
    save_sql(df_member_time)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 2023年五一活动，2023年38女神节活动，2023年618活动
    activity_name = '2023年618活动'
    main()




