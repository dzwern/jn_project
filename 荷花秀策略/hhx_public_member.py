# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/7/11 17:47
# @Author  : diaozhiwei
# @FileName: hhx_public_member.py
# @description: 
# @update:
"""
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from dateutil.relativedelta import relativedelta


def get_public_order():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        left(a.allocation_time,10) allocation_time,
        a.nick_name,
        sum(if(a.is_allocation='是',1,0)) allocations,
        sum(if(a.is_follow='是',1,0)) follows,
        sum(if(a.is_wechat='是',1,0)) wechats,
        sum(if(a.is_amount='是',1,0)) amount,
        sum(a.amounts) amounts,
        sum(a.order_amounts) order_amounts
    FROM
        t_public_member_record a
    GROUP BY a.nick_name,left(a.allocation_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_public_member` 
     (
     `id`,`dept_name1`,`dept_name2`,`dept_name`,`allocation_time`,
     `nick_name`,`allocations`,`follows`,`wechats`,`amount`,
     `amounts`,`order_amounts`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `allocation_time`=values(`allocation_time`),`nick_name`=values(`nick_name`),
         `allocations`=values(`allocations`),`follows`=values(`follows`),
         `wechats`=values(`wechats`),`amount`=values(`amount`),
         `amounts`=values(`amounts`),`order_amounts`=values(`order_amounts`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    df_public_member = get_public_order()
    df_public_member['id'] = df_public_member['dept_name'] + df_public_member['allocation_time'] + df_public_member[
        'nick_name']
    df_public_member = df_public_member[['id', 'dept_name1', 'dept_name2', 'dept_name', 'allocation_time', 'nick_name',
                                         'allocations', 'follows', 'wechats', 'amount', 'amounts', 'order_amounts']]
    save_sql(df_public_member)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()
