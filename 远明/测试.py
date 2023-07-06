# -*-conding:utf-8


# -*-conding:utf-8 -*-


"""
# @Time    : 2023/07/03
# @Author  : gengpeng
# @FileName: dai_oid_orders_day.py
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


def get_orders():
    sql = '''
     SELECT 
      don.*,
      dd.channel,tori.is_first_order,
      tori.is_zhaoshang_first_order, tori.is_follow_order, tori.is_repurchase,
      tori.is_fission
    FROM ymlj_dx.dai_orders_new don
    left JOIN ymlj_dx.dai_dept dd ON don.dept_name = dd.dept_name
    left join crm_tm_ymlj.t_orders to2 ON to2.order_sn = don.order_id
    LEFT JOIN crm_tm_ymlj.t_order_rel_info tori  ON tori.orders_id = to2.id
    WHERE
      don.create_time >= '2023-01-01'
      and dd.channel in ('销售部','市场部')
    '''
    df = get_DataFrame_PD2(sql)
    return df


def get_members():
    sql = '''
     select member_id,dept_name as dept_name2 from `dai_member_5.15`
    '''
    df = get_DataFrame_PD2(sql)
    return df


def get_depts():
    sql = '''
     select dept_name,channel,centel,department,gro from ymlj_dx.dai_dept
    '''
    df = get_DataFrame_PD2(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `dai_oid_orders_day` 
     (`order_id`,`order_state`,`order_pro`,`member_id`,
     `member_source`,`member_source_level2`,`member_identity`,`first_communicate_time`,`incoming_line_time`,
     `dept_name`,`nick_name`,`wechat_number`,`user_name`,`create_time`,
     `trade_time`,`complate_date`,`order_type`,`yx_type`,`is_fission`,
     `receiver_province`,`receiver_city`,`order_amount`,`refund_amount`,`jieyu_amount`,
     `on_member_rank_id`,`on_member_status`,`update_time`,`order_period`,
     `order_cycle`,`act_name`,`member_wzd`, `is_first_order`, `is_zhaoshang_first_order`,
     `is_follow_order`, `is_repurchase`, `channel`, `centel`,`department`, `gro`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
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
         `on_member_rank_id`=values(`on_member_rank_id`),`on_member_status`=values(`on_member_status`),
         `update_time`=values(`update_time`),`order_period`=values(`order_period`),`order_cycle`=values(`order_cycle`),
         `act_name`=values(`act_name`),`member_wzd`=values(`member_wzd`),`is_first_order`=values(`is_first_order`),
         `is_zhaoshang_first_order`=values(`is_zhaoshang_first_order`),`is_follow_order`=values(`is_follow_order`),`is_repurchase`=values(`is_repurchase`),
         `channel`=values(`channel`),`centel`=values(`centel`),`department`=values(`department`),
         `gro`=values(`gro`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table dai_oid_orders_day; -- 清空订单表
    '''
    executeSqlByConn(sql)


def dept_rep(table):
    if "四中心" in table['dept_name']:
        if pd.isna(table['dept_name2']):
            return table['dept_name']
        else:
            return table['dept_name2']
    else:
        return table['dept_name']


def main():
    df_order = get_orders()  # 订单表
    df_member = get_members()  # 客户表
    df_dept = get_depts()  # 部门表

    print('读取数据')
    # 剔除备注列，作用不大，文本占用内存
    df_order.drop(columns='memo', axis=0, inplace=True)

    # 填充缺失值
    df_order['is_fission'].fillna('非裂变', inplace=True)
    df_order['member_wzd'].fillna('其他', inplace=True)
    df_order['member_source_level2'].fillna('无二级渠道', inplace=True)
    df_order['on_member_status'].fillna('-1', inplace=True)
    print('处理缺失值1')
    #  因三四中心合并,需替换四中心的订单给三中心,用5月15日留存的客户表销售部门归属做替换
    # 四中心订单最近的是5月5号的 ，所以只需要对这之前的替换即可
    df_order1 = pd.merge(df_order, df_member, on='member_id', how='left')
    df_order1['dept_name'] = df_order1.apply(dept_rep, axis=1)
    print('处理四中心替换')
    # 删除原部门
    df_order1.drop(columns=['dept_name2', 'channel'], axis=1, inplace=True)
    # 匹配新中心部门
    df_order2 = pd.merge(df_order1, df_dept, on='dept_name', how='left')
    # 填充可能的缺失值
    df_order2['department'].fillna('无部门', inplace=True)
    df_order2['gro'].fillna('无小组', inplace=True)
    # 只取销售市场部
    df_order3 = df_order2.query('channel in ["销售部","市场部"]')
    print('处理部门小组缺失值')

    df_order3 = df_order3[
        ['order_id', 'order_state', 'order_pro', 'member_id', 'member_source',
         'member_source_level2', 'member_identity', 'first_communicate_time',
         'incoming_line_time', 'dept_name', 'nick_name', 'wechat_number',
         'user_name', 'create_time', 'trade_time', 'complate_date', 'order_type',
         'yx_type', 'is_fission', 'receiver_province', 'receiver_city',
         'order_amount', 'refund_amount', 'jieyu_amount', 'on_member_rank_id',
         'on_member_status', 'update_time', 'order_period', 'order_cycle',
         'act_name', 'member_wzd', 'is_first_order', 'is_zhaoshang_first_order',
         'is_follow_order', 'is_repurchase', 'channel', 'centel',
         'department', 'gro']]

    df_order3["complate_date"].replace("NaT", np.NaN, inplace=True)
    df_order3["incoming_line_time"].replace("NaT", np.NaN, inplace=True)
    df_order3 = df_order3.fillna(0)
    df_order3['update_time'] = df_order3['update_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_order3['complate_date'] = df_order3['complate_date'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_order3['incoming_line_time'] = df_order3['incoming_line_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    print('处理时间缺失值')
    df_order3 = df_order3[
        ['order_id', 'order_state', 'order_pro', 'member_id', 'member_source',
         'member_source_level2', 'member_identity', 'first_communicate_time',
         'incoming_line_time', 'dept_name', 'nick_name', 'wechat_number',
         'user_name', 'create_time', 'trade_time', 'complate_date', 'order_type',
         'yx_type', 'is_fission', 'receiver_province', 'receiver_city',
         'order_amount', 'refund_amount', 'jieyu_amount', 'on_member_rank_id',
         'on_member_status', 'update_time', 'order_period', 'order_cycle',
         'act_name', 'member_wzd', 'is_first_order', 'is_zhaoshang_first_order',
         'is_follow_order', 'is_repurchase', 'channel', 'centel',
         'department', 'gro']]
    del_sql()
    print('删除旧数据')
    save_sql(df_order3)
    print('插入数据完成')


if __name__ == '__main__':
    main()
