# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/5 8:46
# @Author  : diaozhiwei
# @FileName: 周报.py
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


# 目标进度
def get_target():
    sql = '''
    SELECT
        a.dept_name2,
        sum(a.complate_amount),
        sum(a.target_amount)
    FROM
        t_target_day a
    where a.monthly='6月'
    GROUP BY a.dept_name2
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 销售对比
def get_order(st, et):
    sql = '''
    SELECT
        a.dept_name2,
        count(1),
        sum(a.order_amount) 
    FROM
        t_orders_middle a 
    WHERE a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.order_amount>40
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name2
    '''.format(st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 进粉
def get_fans(st, et):
    sql = '''
    SELECT
        f.dept_name,
        a.tenant_id,
        left(a.new_sprint_time,10) first_time,
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


# 部门信息
def get_hhx_user():
    sql = '''
    SELECT
        a.dept_name,
        a.dept_name1,
        a.dept_name2,
        a.tenant_id tenant_id2
    FROM
        t_dept_tmp a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 及时订单
def get_order_js():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_js,
        sum(a.order_amount) order_amount_js
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('当日首单日常成交','当日首单活动成交')
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 及时订单
def get_order_hx():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_hx,
        sum(a.order_amount) order_amount_hx
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交')
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 及时订单
def get_order_campaign_hx():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_campaign_hx,
        sum(a.order_amount) order_amount_campaign_hx
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单活动成交')
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 复购订单
def get_order_fg():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_fg,
        sum(a.order_amount) order_amount_fg
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('复购日常成交')
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 复购订单
def get_order_campaign_fg():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.first_time,10) first_time,
        count(DISTINCT a.member_id) order_member_campaign_fg,
        sum(a.order_amount) order_amount_campaign_fg
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('复购活动成交')
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name,a.wechat_number,left(a.first_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 产品
def get_product():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.product_name,
        sum(a.quantity) quantitys
    FROM
        t_order_item_middle a 
    WHERE
        a.create_time >= '2023-04-01' 
    AND a.create_time < '2023-05-01'
    GROUP BY a.dept_name,a.product_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户
def get_member1():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.member_level,
        count(1)
    FROM
        t_member_active_day a
    GROUP BY a.dept_name,a.member_level
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户
def get_member2():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.member_active,
        count(1)
    FROM
        t_member_active_day a
    GROUP BY a.dept_name,a.member_active

    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 员工
def get_user():
    sql = '''
    SELECT
        a.dept_name2,
        a.nick_name,
        count(1),
        sum(a.order_amount) 
    FROM
        t_orders_middle a 
    WHERE a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交','复购日常成交','复购活动成交')
    and a.order_amount>40
    and a.create_time>='2023-06-01'
    and a.create_time<'2023-06-06'
    GROUP BY a.dept_name2,a.nick_name
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def main():
    # 整体
    # 对比
    # 进粉
    # 产品
    # 员工
    # 订单
    # 客户

    logDir = r"D:/工作文件/预测/"
    path = logDir + '城市近1周销售数据指标对比.xlsx'
    write = pd.ExcelWriter(path=path,
                           engine='xlsxwriter',
                           datetime_format='YYYY-MM-DD HH:MM:SS',
                           date_format='YYYY-MM-DD')
    df_city_data.to_excel(write, '城市销售数据', index=False)
    df_city_machine.to_excel(write, '城市在线点位数', index=False)
    write.save()


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    st = '2023-01-01'
    et = '2023-02-01'
    main()
