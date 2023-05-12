# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 14:59
# @Author  : diaozhiwei
# @FileName: hhx_product_campaign.py
# @description: 活动期间商品监控
# @update:
"""
from datetime import datetime
from modules.mysql import jnmtMySQL
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


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_product_campaign;
    '''
    hhx_sql2.executeSqlByConn(sql)


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
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 产品信息
    df_product_order=get_product_order()
    df_product_order['activity_name'] = activity_name
    df_product_order['id'] = df_product_order['dept_name'].astype(str) + df_product_order['product_name'].astype(str) + \
                             df_product_order['activity_name']
    df_product_order=df_product_order[['id','dept_name1','dept_name2','dept_name','product_name','quantitys','activity_name']]
    del_sql()
    save_sql(df_product_order)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    activity_name = '2023年五一活动'
    main()




