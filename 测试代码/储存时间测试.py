# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/17 13:46
# @Author  : diaozhiwei
# @FileName: 储存时间测试.py
# @description: 
# @update:
"""
import datetime
import pandas as pd
from jn_modules.mysql import jnmtMySQL5
from jn_modules.mysql import jnmtMySQL


# 订单基础信息
def get_hhx_orders():
    sql = '''
    SELECT
        a.order_sn,
        a.complate_date
    FROM
        t_orders a 
    WHERE
        a.tenant_id = 11
    and a.create_time>='{}'
    and a.create_time<'{}'
    '''.format(st1, et1)
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_time_tmp` 
     (`id`,`order_sn`,`complate_date`
     ) 
     VALUES (%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_sn`=values(`order_sn`), `complate_date`=values(`complate_date`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    df = get_hhx_orders()
    df['id'] = df['order_sn']
    df=df[['id','order_sn','complate_date']]
    print(df)
    save_sql(df)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    # 开始时间，结束时间
    # 时间转化
    st = '2023-04-01'
    et = '2023-04-13'
    st1 = datetime.datetime.strptime(st, "%Y-%m-%d")
    et1 = datetime.datetime.strptime(et, "%Y-%m-%d")
    main()
