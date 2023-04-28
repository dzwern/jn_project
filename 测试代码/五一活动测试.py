# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:31
# @Author  : diaozhiwei
# @FileName: hhx_order_pred_campaign.py
# @description: 活动预估，使用预估的数据进行实时监控，实时监控表，到员工
# @update：更新时间在，活动中监控
"""

import pandas as pd
from datetime import  datetime,timedelta
import sys
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import numpy as np

userName = 'dzw'
password = 'dsf#4oHGd'
dbHost = 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com'
dbPort = 3306
URL = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/'
schema = 'crm_tm_jnmt'
schema2 = 'hhx_dx'
engine = create_engine(URL + schema + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)
engine2 = create_engine(URL + schema2 + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)


# 加载数据到df
def get_DataFrame_PD(sql='SELECT * FROM DUAL'):
    conn = engine.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 加载数据到df
def get_DataFrame_PD2(sql='SELECT * FROM DUAL'):
    conn = engine2.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 批量执行更新sql语句
def executeSqlManyByConn(sql, data):
    conn = engine2.connect()
    if len(data) > 0:
        with conn as connection:
            return connection.execute(sql, data)


# 时间转化字符串
def date2str(parameter, format='%Y-%m-%d'):
    if isinstance(parameter, str):
        return parameter
    return parameter.strftime(format)


# 执行sql
def executeSqlByConn(sql='SELECT * FROM DUAL', conn=None):
    conn = engine2.connect()
    with conn as connection:
        return connection.execute(sql)


def get_order_base():
    sql = '''
    SELECT
        *
    FROM
        t_orders_middle a
    # 状态
    where a.activity_name='{}'
    '''.format(activity_name)
    df = get_DataFrame_PD2(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `t_orders_campaign_tmp_51` 
     (`id`,`order_sn`,`original_order_sn`,`order_type`,`no_performance_type`,
     `clinch_type`,`dept_name1`,`dept_name2`,`dept_name`,`sys_user_id`,
     `user_name`,`nick_name`,`wechat_id`,`wechat_name`,`wechat_number`,
     `member_id`,`member_source`,`first_time`,`create_time`,`time_diff`,
     `receiver_name`,`receiver_phone`,`receiver_detail_address`,`receiver_province`,`receiver_city`,
     `receiver_region`,`product_name`,`order_amount`, `amount_paid`,`refund_amount`,
     `pay_type_name`,`order_interval`,`order_state`, `review_state`,`refund_state`,
     `trade_time`,`complate_date`,`project_category_id`,`is_activity`,`activity_name`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s)
     ON DUPLICATE KEY UPDATE
         `order_sn`= VALUES(`order_sn`),`original_order_sn`= VALUES(`original_order_sn`),`order_type`=VALUES(`order_type`),
         `no_performance_type`=values(`no_performance_type`),`clinch_type`=values(`clinch_type`),`dept_name1`=values(`dept_name1`),
         `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),`sys_user_id`=values(`sys_user_id`),
         `user_name`=values(`user_name`),`nick_name`=values(`nick_name`),`wechat_id`=values(`wechat_id`),
         `wechat_name`=values(`wechat_name`),`wechat_number`=values(`wechat_number`),`member_id`=values(`member_id`),
         `member_source`=values(`member_source`),`first_time`=values(`first_time`),`create_time`=values(`create_time`),
         `time_diff`=values(`time_diff`),`receiver_name`=values(`receiver_name`),`receiver_phone`=values(`receiver_phone`),
         `receiver_detail_address`=values(`receiver_detail_address`),`receiver_province`=values(`receiver_province`),`receiver_city`=values(`receiver_city`),
         `receiver_region`=values(`receiver_region`),`product_name`=values(`product_name`),`order_amount`=values(`order_amount`),
         `amount_paid`=values(`amount_paid`),`refund_amount`=values(`refund_amount`),`pay_type_name`=values(`pay_type_name`),
         `order_interval`=values(`order_interval`),`order_state`=values(`order_state`),`review_state`=values(`review_state`),
         `refund_state`=values(`refund_state`),`trade_time`=values(`trade_time`),`complate_date`=values(`complate_date`),
         `project_category_id`=values(`project_category_id`),`is_activity`=values(`is_activity`),`activity_name`=values(`activity_name`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 51基础数据
    df_order_base = get_order_base()
    df_order_base = df_order_base[
        ['id', 'order_sn', 'original_order_sn', 'order_type', 'no_performance_type', 'clinch_type',
         'dept_name1', 'dept_name2', 'dept_name', 'sys_user_id', 'user_name', 'nick_name',
         'wechat_id', 'wechat_name', 'wechat_number','member_id', 'member_source', 'first_time',
         'create_time', 'time_diff','receiver_name', 'receiver_phone', 'receiver_detail_address', 'receiver_province',
         'receiver_city', 'receiver_region', 'product_name', 'order_amount', 'amount_paid', 'refund_amount',
         'pay_type_name', 'order_interval', 'order_state', 'review_state', 'refund_state',
         'trade_time', 'complate_date', 'project_category_id', 'is_activity', 'activity_name']]
    save_sql(df_order_base)


if __name__ == '__main__':
    activity_name = '2023年五一活动'
    main()
