# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/6/7 14:20
# @Author  : diaozhiwei
# @FileName: hhx_product_stock_day.py
# @description: 商品库存
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


def get_stock():
    sql = '''
    SELECT
        a.id,
        a.product_name,
        TRIM(BOTH '"' from JSON_EXTRACT(b.specification_values, '$[0].key')) types,
        TRIM(BOTH '"' from JSON_EXTRACT(b.specification_values, '$[0].value')) spec,
        b.stock,
        a.tenant_id
    FROM
        t_product a
    LEFT JOIN  t_sku b on a.id=b.product_id
    WHERE
        a.tenant_id in ('25','26','27','28')
    GROUP BY a.product_name,a.tenant_id
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def get_product_order(st, et):
    sql = '''
    SELECT
        a.dept_name2,
        a.product_name,
        sum(a.quantity) quantitys
    FROM
        t_order_item_middle a 
    where a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.dept_name,a.product_name
    '''.format(st, et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_index(x):
    if x == 25:
        return '蜜肤语项目'
    elif x == 26:
        return '蜜梓源项目'
    elif x == 27:
        return '蜂蜜项目'
    elif x == 28:
        return '海参项目'


def get_name(x):
    if x == '光辉部蜜肤语后端':
        return '蜜肤语项目'
    elif x == '光辉部蜜肤语前端':
        return '蜜肤语项目'
    elif x == '光华部蜜梓源面膜进粉前端':
        return '蜜梓源项目'
    elif x == '光华部蜜梓源面膜进粉后端':
        return '蜜梓源项目'
    elif x == '光芒部蜜梓源后端':
        return '蜜梓源项目'
    elif x == '光源部蜂蜜组':
        return '蜂蜜项目'
    elif x == '光源部海参组':
        return '海参项目'


# 保存数据
def save_sql(df):
    sql = '''
     INSERT INTO `t_product_stock_middle` 
     (
     `id`,`product_name`,`types`,`spec`,`stock`,`weekly_order`,`monthly_order`,`tenant_id`,`project`
     )
     VALUES (
     %s,%s,%s,%s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `product_name`= VALUES(`product_name`),`types`= VALUES(`types`),`spec`= VALUES(`spec`),
         `stock`= VALUES(`stock`),`weekly_order`= VALUES(`weekly_order`),`monthly_order`= VALUES(`monthly_order`),
         `tenant_id`= VALUES(`tenant_id`), `project`= VALUES(`project`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_product_stock_middle;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 库存数据
    df_stock = get_stock()
    df_stock['project'] = df_stock.apply(lambda x: get_index(x['tenant_id']), axis=1)
    df_stock = df_stock.fillna(0)
    # 销售数据，周销售
    df_product_order = get_product_order(st, st2)
    df_product_order['project'] = df_product_order.apply(lambda x: get_name(x['dept_name2']), axis=1)
    df_product_order = df_product_order.fillna(0)
    df_stock = df_stock.merge(df_product_order, on=['project', 'product_name'], how='left')
    # 销售数据，月销售
    df_product_order2 = get_product_order(st, et)
    df_product_order2['project'] = df_product_order2.apply(lambda x: get_name(x['dept_name2']), axis=1)
    df_product_order2 = df_product_order2.fillna(0)
    df_stock = df_stock.merge(df_product_order2, on=['project', 'product_name'], how='left')
    df_stock = df_stock.rename(columns={'quantitys_x': 'weekly_order', 'quantitys_y': 'monthly_order'})
    df_stock = df_stock[
        ['id', 'product_name', 'types', 'spec', 'stock', 'weekly_order', 'monthly_order', 'tenant_id', 'project']]
    df_stock = df_stock.fillna(0)
    del_sql()
    save_sql(df_stock)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    st = '2023-04-01'
    st2 = '2023-05-01'
    et = '2023-06-01'
    st = datetime.strptime(st, "%Y-%m-%d")
    st2 = datetime.strptime(st2, "%Y-%m-%d")
    et = datetime.strptime(et, "%Y-%m-%d")
    main()
