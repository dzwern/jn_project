# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/8 17:27
# @Author  : diaozhiwei
# @FileName: hhx_nickname_rank_total.py
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


def get_order():
    sql='''
    SELECT
        a.dept_name,
        a.nick_name,
        count(1),
        sum(a.order_amount) 
    FROM
        t_orders_middle a 
    WHERE a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.order_amount>40
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name,a.nick_name
    '''.format(st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def main():
    pass


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    st = '2023-01-01'
    et = '2023-02-01'
    main()
