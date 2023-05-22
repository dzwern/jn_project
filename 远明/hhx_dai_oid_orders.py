# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/16 16:29
# @Author  : diaozhiwei
# @FileName: hhx_dai_oid_orders.py
# @description: 
# @update:
"""
import pandas as pd
from datetime import datetime, timedelta
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
schema2 = 'ymlj_dx'
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


def get_oid_orders():
    sql = '''
     SELECT 
      don.*,
      CASE 
        WHEN don.order_amount <= 500 THEN '0-0.5K'
        WHEN don.order_amount <= 2000 THEN '0.5k-2k'
        WHEN don.order_amount <= 4000 THEN '2k-4k'
        WHEN don.order_amount <= 6000 THEN '4k-6k'
        WHEN don.order_amount <= 8000 THEN '6k-8k'
        WHEN don.order_amount <= 10000 THEN '8k-1w'
        WHEN don.order_amount <= 20000 THEN '1w-2w'
        WHEN don.order_amount <= 30000 THEN '2w-3w'
        WHEN don.order_amount <= 50000 THEN '3w-5w'
        WHEN don.order_amount <= 100000 THEN '5w-10w'
        ELSE '10w'
      END AS '订单金额区间',
      dd.channel, dd.centel, 
      CASE WHEN dd.department IS NULL THEN '无部门' ELSE dd.department END '部门',
      CASE WHEN dd.gro IS NULL THEN '无小组' ELSE dd.gro END '小组'
    FROM ymlj_dx.dai_orders_new don
    left JOIN ymlj_dx.dai_dept dd ON don.dept_name = dd.dept_name
    WHERE
      don.dept_name LIKE '销售%%'
      AND don.create_time >= '2023-01-01'
    '''
    df = get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `dai_oid_orders` 
     (`id`,`order_id`,`order_state`,`order_pro`,`member_id`,
     `member_source`,`member_source_level2`,`member_identity`,`first_communicate_time`,`incoming_line_time`,
     `dept_name`,`nick_name`,`wechat_number`,`user_name`,`create_time`,
     `trade_time`,`complate_date`,`order_type`,`yx_type`,`is_fission`,
     `receiver_province`,`receiver_city`,`order_amount`,`refund_amount`,`jieyu_amount`,
     `memo`,`on_member_rank_id`,`on_member_status`,`update_time`,`order_period`,
     `order_cycle`,`act_name`,`member_wzd`,`order_interval`,`channel`,
     `centel`,`department`,`gro`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_id`= VALUES(`order_id`),`order_state`= VALUES(`order_state`),`order_pro`=VALUES(`order_pro`),
         `member_id`=values(`member_id`),`member_source`=values(`member_source`),`member_source_level2`=values(`member_source_level2`),
         `member_identity`=values(`member_identity`),`first_communicate_time`=values(`first_communicate_time`),`incoming_line_time`=values(`incoming_line_time`),
         `dept_name`=values(`dept_name`),`nick_name`=values(`nick_name`),`wechat_number`=values(`wechat_number`),
         `user_name`=values(`user_name`),`create_time`=values(`create_time`),`trade_time`=values(`trade_time`),
         `complate_date`=values(`complate_date`),`order_type`=values(`order_type`),`yx_type`=values(`yx_type`),
         `is_fission`=values(`is_fission`),`receiver_province`=values(`receiver_province`),`receiver_city`=values(`receiver_city`),
         `order_amount`=values(`order_amount`),`refund_amount`=values(`refund_amount`),`jieyu_amount`=values(`jieyu_amount`),
         `memo`=values(`memo`),`on_member_rank_id`=values(`on_member_rank_id`),`on_member_status`=values(`on_member_status`),
         `update_time`=values(`update_time`),`order_period`=values(`order_period`),`order_cycle`=values(`order_cycle`),
         `act_name`=values(`act_name`),`member_wzd`=values(`member_wzd`),`order_interval`=values(`order_interval`),
         `channel`=values(`channel`),`centel`=values(`centel`),`department`=values(`department`),
         `gro`=values(`gro`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table dai_oid_orders;
    '''
    executeSqlByConn(sql)


def main():
    df_oid_order = get_oid_orders()
    df_oid_order['id'] = df_oid_order['order_id']
    df_oid_order = df_oid_order[
        ['id', 'order_id', 'order_state', 'order_pro', 'member_id', 'member_source', 'member_source_level2',
         'member_identity','first_communicate_time', 'incoming_line_time', 'dept_name', 'nick_name', 'wechat_number',
         'user_name','create_time','trade_time', 'complate_date', 'order_type', 'yx_type', 'is_fission',
         'receiver_province', 'receiver_city','order_amount','refund_amount', 'jieyu_amount', 'memo', 'on_member_rank_id',
         'on_member_status', 'update_time', 'order_period', 'order_cycle', 'act_name', 'member_wzd', '订单金额区间',
         'channel','centel', '部门', '小组']]
    df_oid_order["complate_date"].replace("NaT", np.NaN, inplace=True)
    df_oid_order["incoming_line_time"].replace("NaT", np.NaN, inplace=True)
    df_oid_order = df_oid_order.fillna(0)
    df_oid_order['update_time'] = df_oid_order['update_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_oid_order['complate_date'] = df_oid_order['complate_date'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_oid_order['incoming_line_time'] = df_oid_order['incoming_line_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    del_sql()
    save_sql(df_oid_order)


if __name__ == '__main__':
    main()
