# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:26
# @Author  : diaozhiwei
# @FileName: hhx_member_day.py
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
    GROUP BY a.wechat_number,a.member_level
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


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
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 客户等级
    df_member_level = get_member_level()
    df_member_level['id'] = df_member_level['wechat_number'] + df_member_level['member_level']
    df_member_level = df_member_level[['id', 'dept_name1', 'dept_name2', 'dept_name', 'nick_name', 'wechat_id',
                                       'wechat_number', 'member_level', 'members', 'member_orders']]
    save_sql(df_member_level)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()
