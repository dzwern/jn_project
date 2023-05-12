# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/11 13:45
# @Author  : diaozhiwei
# @FileName: hhx_campaign_day.py
# @description: 活动数据汇总
# @update:
"""
from datetime import datetime
from modules.mysql import jnmtMySQL
import pandas as pd


# 38女神节活动
def get_campaign_member(activity_name, log_name):
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.member_level,
        '2023' years,
        '{}' activity_name,
        count(DISTINCT a.member_id) activity_member
    FROM
        t_member_middle_log a
    where a.log_name='{}'
    GROUP BY a.dept_name,a.member_level
    '''.format(activity_name, log_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 活动销售
def get_campaign_order(activity_name, log_name):
    sql = '''
    SELECT 
        a.dept_name,
        b.member_level,
        '{}' activity_name,
        count(DISTINCT a.member_id) activity_develop_member,
        sum(a.order_amount) activity_order
    FROM 
        t_orders_middle a
    LEFT JOIN  t_member_middle_log b on a.member_id=b.member_id and b.log_name='{}'
    where a.order_state not in ('订单取消','订单驳回','拒收途中','待确认拦回')
    and a.clinch_type in ('复购日常成交','复购活动成交','后续首单日常成交','后续首单活动成交')
    and a.activity_name='{}'
    and a.order_amount>40
    GROUP BY a.dept_name,b.member_level
    '''.format(activity_name, log_name, activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
     INSERT INTO `t_campaign_day` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`years`,
     `activity_name`,`member_level`,`activity_member`,`activity_develop_member`,`activity_order`,
     `activity_rate`,`activity_price`,`activity_member_price`
     )
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`= VALUES(`dept_name`),
         `years`=VALUES(`years`),`activity_name`=VALUES(`activity_name`),`member_level`=values(`member_level`),
         `activity_member`=values(`activity_member`),`activity_develop_member`=values(`activity_develop_member`),
         `activity_order`=values(`activity_order`), `activity_rate`=values(`activity_rate`),
         `activity_price`=values(`activity_price`),`activity_member_price`=values(`activity_member_price`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 2023年38女神节活动，活动前客户
    df_campaign_member = get_campaign_member(activity_name1, log_name1)
    # 2023年51活动，活动前客户
    df_campaign_member2 = get_campaign_member(activity_name2, log_name2)
    # 2023年38女神节销售
    df_campaign_order = get_campaign_order(activity_name1, log_name1)
    # 2023年51活动
    df_campaign_order2 = get_campaign_order(activity_name2, log_name2)
    df_campaign = pd.concat([df_campaign_member, df_campaign_member2])
    df_order = pd.concat([df_campaign_order, df_campaign_order2])
    df_campaign = df_campaign.merge(df_order, on=['dept_name', 'member_level', 'activity_name'], how='left')
    df_campaign = df_campaign.fillna(0)
    # 活动转化率
    df_campaign['activity_rate'] = df_campaign['activity_develop_member'] / df_campaign['activity_member']
    # 活动客单价
    df_campaign['activity_price'] = df_campaign['activity_order'] / df_campaign['activity_develop_member']
    # 活动单产
    df_campaign['activity_member_price'] = df_campaign['activity_order'] / df_campaign['activity_member']
    df_campaign['id'] = df_campaign['dept_name'] + df_campaign['years'] + df_campaign['activity_name']
    df_campaign = df_campaign.fillna(0)
    df_campaign = df_campaign[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'years', 'activity_name', 'member_level', 'activity_member',
         'activity_develop_member', 'activity_order', 'activity_rate', 'activity_price', 'activity_member_price']]
    print(df_campaign)
    save_sql(df_campaign)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    log_name1 = '2023年38女神节活动前客户等级'
    log_name2 = '2023年51活动前客户等级'
    activity_name1 = '2023年38女神节活动'
    activity_name2 = '2023年五一活动'
    main()
