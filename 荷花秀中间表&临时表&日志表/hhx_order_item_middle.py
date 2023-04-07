# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/6 14:28
# @Author  : diaozhiwei
# @FileName: hhx_order_item_middle.py
# @description: 产品信息
# @update:
"""
from modules.mysql import jnmtMySQL
from modules.mysql import jnmtMySQL4
import pandas as pd
import datetime


def get_order_product():
    sql='''
    SELECT
        a.order_sn,
        b.order_id,
        b.product_name,
        b.sku_price,
        b.real_price,
        b.quantity
    FROM
        t_orders a
        left join t_order_item b on a.id=b.order_id
    WHERE
        a.tenant_id = 11
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time>='{}'
    and a.create_time<'{}'
    '''.format(st,et)
    df=hhx_sql.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_order_item_middle` 
     (`id`,`order_sn`,`order_id`,`product_name`,`sku_price`,
     `real_price`,`quantity`
     ) 
     VALUES (%s,%s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_sn`= VALUES(`order_sn`),`order_id`= VALUES(`order_id`),`product_name`=VALUES(`product_name`),
         `sku_price`=values(`sku_price`),`real_price`=values(`real_price`),`quantity`=values(`quantity`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 产品信息数据
    df_order_product=get_order_product()
    df_order_product['id']=df_order_product['order_sn'].astype(str)+df_order_product['order_id'].astype(str)
    df_order_product=df_order_product.fillna(0)
    df_order_product=df_order_product[['id','order_sn','order_id','product_name','sku_price','real_price','quantity']]
    # 保存数据
    save_sql(df_order_product)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    time1 = datetime.datetime.now()
    st = time1 - relativedelta(days=1)
    et = time1 + relativedelta(days=1)
    st = date2str(st)
    et = date2str(et)
    print(st,et)
    main()







