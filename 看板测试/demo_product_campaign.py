# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 14:59
# @Author  : diaozhiwei
# @FileName: demo_product_campaign.py
# @description: 活动期间商品监控
# @update:
"""
from datetime import datetime
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd


def get_product_order():
    sql = '''
    SELECT
        a.dept_name1,
        a.dept_name2,
        a.dept_name,
        a.product_name,
        sum(a.quantity) quantitys
    FROM
        t_order_item_middle a 
    WHERE a.activity_name='{}'
    GROUP BY a.dept_name,a.product_name
    '''.format(activity_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_dept(x):
    if x == '光辉部':
        return '1部门'
    elif x == '光华部':
        return '2部门'
    elif x == '光源部':
        return '3部门'
    elif x == '光芒部':
        return '4部门'
    else:
        return '1部门'


def get_dept2(x):
    if x == '光华部1组':
        return '小组1'
    elif x == '光华部二组':
        return '小组2'
    elif x == '光华部六组':
        return '小组3'
    elif x == '光华部五组':
        return '小组4'
    elif x == '光华部一组1':
        return '小组5'
    elif x == '光华部三组':
        return '小组6'
    elif x == '光华部七组':
        return '小组7'
    elif x == '光华部一组':
        return '小组8'
    elif x == '光辉部八组':
        return '小组1'
    elif x == '光辉部七组':
        return '小组2'
    elif x == '光辉部三组':
        return '小组3'
    elif x == '光辉部一组':
        return '小组4'
    elif x == '光辉部二组':
        return '小组5'
    elif x == '光辉部五组':
        return '小组6'
    elif x == '光辉部六组':
        return '小组7'
    elif x == '光辉组九组':
        return '小组8'
    elif x == '光芒部二组':
        return '小组1'
    elif x == '光芒部六组':
        return '小组2'
    elif x == '光芒部三组':
        return '小组3'
    elif x == '光芒部一组':
        return '小组4'
    elif x == '光源部蜂蜜八组':
        return '小组1'
    elif x == '光源部蜂蜜九组':
        return '小组2'
    elif x == '光源部蜂蜜四组':
        return '小组3'
    elif x == '光源部蜂蜜五组':
        return '小组4'
    elif x == '光源部海参七组':
        return '小组5'
    else:
        return '小组1'


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_product_campaign;
    '''
    hhx_sql3.executeSqlByConn(sql)


def save_sql(df):
    sql = '''
    INSERT INTO `t_product_campaign` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`product_name`,`quantitys`,`activity_name`) 
     VALUES (%s,%s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `product_name`=values(`product_name`),`quantitys`=values(`quantitys`),`activity_name`=values(`activity_name`)
         '''
    hhx_sql3.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 产品信息
    df_product_order = get_product_order()
    df_product_order['activity_name'] = activity_name
    df_product_order['id'] = df_product_order['dept_name'].astype(str) + df_product_order['product_name'].astype(str) + \
                             df_product_order['activity_name']
    df_product_order = df_product_order[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'product_name', 'quantitys', 'activity_name']]
    # del_sql()
    print(df_product_order)
    df_product_order['dept_name1'] = df_product_order.apply(lambda x: get_dept(x['dept_name1']), axis=1)
    df_product_order['dept_name'] = df_product_order.apply(lambda x: get_dept2(x['dept_name']), axis=1)
    df_product_order.sort_values(by=['quantitys'], ascending=True)
    df_product_order['rank'] = df_product_order['quantitys'].rank(ascending=False,method='min')
    df_product_order['product_name'] = '产品' + df_product_order['rank'].astype(str)
    df_product_order = df_product_order[
        ['id', 'dept_name1', 'dept_name2', 'dept_name', 'product_name', 'quantitys', 'activity_name']]
    del_sql()
    save_sql(df_product_order)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql3 = jnMysql('yanshiku_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 2023年五一活动，2023年38女神节活动，2023年618活动
    activity_name = '2023年618活动返场'
    main()
