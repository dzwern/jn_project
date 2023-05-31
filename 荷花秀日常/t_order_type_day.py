# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/9 13:49
# @Author  : diaozhiwei
# @FileName: t_order_type_day.py
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


def get_order_total():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.wechat_id,
        a.wechat_name,
        a.wechat_number,
        left(a.create_time,10) create_time,
        year(a.create_time) years,
        QUARTER(a.create_time) quarterly,
        MONTH(a.create_time) monthly,
        WEEKOFYEAR(a.create_time)weekly,
        a.is_activity,
        a.activity_name
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.dept_name1 not in ('0')
    GROUP BY a.dept_name,a.wechat_number,left(a.create_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 进粉 基础数据，每日进粉数
def get_member_credit():
    sql = '''
    SELECT
        f.dept_name,
        d.wecaht_number wechat_number,
        a.tenant_id,
        left(a.new_sprint_time,10) create_time,
        sum(a.credit) fans 
    FROM t_wechat_fans_log a
    LEFT JOIN t_wechat d on d.id=a.wechat_id
    LEFT JOIN sys_user e on e.user_id=d.sys_user_id
    LEFT JOIN sys_dept f on e.dept_id=f.dept_id
    where a.tenant_id in ( '25', '26', '27', '28' ) 
    and a.new_sprint_time>='{}'
    and a.new_sprint_time<'{}'
    and a.credit>0
    GROUP BY f.dept_name,e.nick_name,d.wecaht_number,a.tenant_id,left(a.new_sprint_time,10)
    '''.format(st,et)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    sql = '''
    SELECT
        a.dept_name,
        a.tenant_id tenant_id2
    FROM
        t_dept_tmp a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 及时订单
def get_order_js():
    sql='''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.create_time,10) create_time,
        count(DISTINCT a.member_id) order_member_js,
        sum(a.order_amount) order_amount_js
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('当日首单日常成交','当日首单活动成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.create_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 后续订单
def get_order_hx():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.create_time,10) create_time,
        count(DISTINCT a.member_id) order_member_hx,
        sum(a.order_amount) order_amount_hx
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('后续首单日常成交','后续首单活动成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.create_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 复购订单
def get_order_fg():
    sql = '''
    SELECT
        a.dept_name,
        a.wechat_number,
        left(a.create_time,10) create_time,
        count(DISTINCT a.member_id) order_member_fg,
        sum(a.order_amount) order_amount_fg
    FROM
        t_orders_middle a
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('复购日常成交','复购活动成交')
    GROUP BY a.dept_name,a.wechat_number,left(a.create_time,10)
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `t_order_type_day` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`sys_user_id`,
     `user_name`,`nick_name`,`wechat_id`,`wechat_name`,`wechat_number`,
     `create_time`,`years`,`quarterly`,`monthly`,`weekly`,
     `fans`,`order_member_js`,`order_amount_js`,`member_price_js`,`oid_price_js`,
     `member_rate_js`,`order_member_hx`,`order_amount_hx`,`member_price_hx`,`order_member_fg`,
     `order_amount_fg`,`member_price_fg`,`is_activity`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `sys_user_id`=values(`sys_user_id`), `user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `wechat_id`=values(`wechat_id`),`wechat_name`=values(`wechat_name`),`wechat_number`=values(`wechat_number`),
         `create_time`=values(`create_time`),`years`=values(`years`),`quarterly`=values(`quarterly`),
         `monthly`=values(`monthly`),`weekly`=values(`weekly`),`fans`=values(`fans`),
         `order_member_js`=values(`order_member_js`),`order_amount_js`=values(`order_amount_js`),`member_price_js`=values(`member_price_js`),
         `oid_price_js`=values(`oid_price_js`),`member_rate_js`=values(`member_rate_js`),`order_member_hx`=values(`order_member_hx`),
         `order_amount_hx`=values(`order_amount_hx`),`member_price_hx`=values(`member_price_hx`),`order_member_fg`=values(`order_member_fg`),
         `order_amount_fg`=values(`order_amount_fg`),`member_price_fg`=values(`member_price_fg`),`is_activity`=values(`is_activity`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_order_type_day;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 基础数据
    df_order_base=get_order_total()
    # 进粉数
    df_order_credit=get_member_credit()
    df_order_base = df_order_base.merge(df_order_credit, on=['dept_name', 'wechat_number', 'create_time'], how='left')
    # 部门
    df_hhx_user = get_hhx_user()
    df_order_base = df_order_base.merge(df_hhx_user, on=['dept_name'], how='left')

    # 判断
    df_order_base=df_order_base.fillna(0)
    df_order_base['fuzhu']=df_order_base['tenant_id2']-df_order_base['tenant_id']
    df_order_base=df_order_base.loc[df_order_base['fuzhu']==0,:]

    # js成交
    df_order_js=get_order_js()
    # hx成交
    df_order_hx=get_order_hx()
    # fg成交
    df_order_fg=get_order_fg()
    # 融合
    df_order_base=df_order_base.merge(df_order_js, on=['dept_name', 'wechat_number', 'create_time'], how='left')
    df_order_base=df_order_base.merge(df_order_hx, on=['dept_name', 'wechat_number', 'create_time'], how='left')
    df_order_base=df_order_base.merge(df_order_fg, on=['dept_name', 'wechat_number', 'create_time'], how='left')
    df_order_base=df_order_base.fillna(0)
    # 进粉单产
    df_order_base['oid_price_js']=df_order_base['order_amount_js']/df_order_base['fans']
    # 及时开发率
    df_order_base['member_rate_js'] = df_order_base['order_member_js'] / df_order_base['fans']
    # 及时客单价
    df_order_base['member_price_js']=df_order_base['order_amount_js']/df_order_base['order_member_js']
    # 后续客单价
    df_order_base['member_price_hx']=df_order_base['order_amount_hx']/df_order_base['order_member_hx']
    # 复购客单价
    df_order_base['member_price_fg'] = df_order_base['order_amount_fg'] / df_order_base['order_member_fg']
    df_order_base = df_order_base.replace([np.inf, -np.inf], np.nan)
    df_order_base=df_order_base.fillna(0)
    df_order_base['id'] = df_order_base['wechat_number'] + df_order_base['create_time']
    df_order_base = df_order_base[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'sys_user_id', 'user_name', 'nick_name',
         'wechat_id', 'wechat_name', 'wechat_number', 'create_time', 'years', 'quarterly', 'monthly',
         'weekly', 'fans', 'order_member_js', 'order_amount_js', 'member_price_js', 'oid_price_js',
         'member_rate_js', 'order_member_hx', 'order_amount_hx', 'member_price_hx', 'order_member_fg',
         'order_amount_fg', 'member_price_fg', 'is_activity', 'activity_name']]
    df_order_base=df_order_base
    print(df_order_base)
    del_sql()
    save_sql(df_order_base)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 开始时间，结束时间
    st = '2023-01-01'
    st1 = datetime.strptime(st, "%Y-%m-%d")
    time1 = datetime.now()
    et = time1 + relativedelta(days=1)
    et1 = utils.date2str(et)
    print(st1,et1)
    main()





