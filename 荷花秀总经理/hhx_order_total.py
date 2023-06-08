# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/8 17:26
# @Author  : diaozhiwei
# @FileName: hhx_order_total.py
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


def get_fans():
    sql = '''
    SELECT
        f.dept_name,
        a.tenant_id,
        sum(a.credit) fans 
    FROM t_wechat_fans_log a
    LEFT JOIN t_wechat d on d.id=a.wechat_id
    LEFT JOIN sys_user e on e.user_id=d.sys_user_id
    LEFT JOIN sys_dept f on e.dept_id=f.dept_id
    where a.tenant_id in ( '25', '26', '27', '28' ) 
    and a.new_sprint_time>='{}'
    and a.new_sprint_time<'{}'
    and a.credit>0
    GROUP BY f.dept_name,a.tenant_id
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def get_order():
    sql = '''
    SELECT
        a.dept_name,
        count(1),
        sum(a.order_amount) 
    FROM
        t_orders_middle a 
    WHERE a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.order_amount>40
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name
    '''.format(st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_target():
    sql = '''
    SELECT
        a.dept_name2,
        sum(a.complate_amount),
        sum(a.target_amount)
    FROM
        t_target_day a
    GROUP BY a.dept_name2
    '''
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
