# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/13 9:03
# @Author  : diaozhiwei
# @FileName: hhx_order_campaign_51.py
# @description: 
# @update:
"""
from modules.mysql import jnmtMySQL
from modules.mysql import jnmtMySQL4
import pandas as pd
from datetime import datetime, timedelta
from modules.func import utils
from dateutil.relativedelta import relativedelta


def get_order_base():
    sql = '''
    SELECT
        *
    FROM
        t_orders_middle a
    # 状态
    where a.activity_name='2023年38女神节活动'
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
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
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


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
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    main()
