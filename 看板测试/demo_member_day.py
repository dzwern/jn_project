# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:26
# @Author  : diaozhiwei
# @FileName: demo_member_day.py
# @description: 荷花秀客户整体监控，主要内容：客户数，客户分层数，客户购买频次，客户购买金额
"""
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd
import datetime
import numpy as np


def get_member_level():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.nick_name,
        a.wechat_id,
        a.wechat_number,
        a.member_level,
        count(DISTINCT a.member_id) members,
        sum(a.order_amounts) member_orders
    FROM
        t_member_middle a
    where  a.dept_name1 !='0'
    GROUP BY a.wechat_number,a.member_level
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


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
     INSERT INTO `t_member_day` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`nick_name`,
     `wechat_id`,`wechat_number`,`member_level`,`members`,`member_orders`
     )
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`= VALUES(`dept_name`),
         `nick_name`=VALUES(`nick_name`),`wechat_id`=VALUES(`wechat_id`),`wechat_number`=values(`wechat_number`),
         `member_level`=values(`member_level`),`members`=values(`members`),`member_orders`=values(`member_orders`)
     '''
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_member_day;
    '''
    hhx_sql3.executeSqlByConn(sql)


def main():
    # 客户等级
    df_member_level = get_member_level()
    df_member_level['id'] = df_member_level['wechat_number'] + df_member_level['member_level']
    df_member_level = df_member_level[['id', 'dept_name1', 'dept_name2', 'dept_name', 'nick_name', 'wechat_id',
                                       'wechat_number', 'member_level', 'members', 'member_orders']]
    df_member_level['dept_name1'] = df_member_level.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_member_level['dept_name'] = df_member_level.apply(lambda x: get_dept2(x['dept_name']), axis=1)
    del_sql()
    save_sql(df_member_level)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()


