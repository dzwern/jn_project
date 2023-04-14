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
from datetime import datetime, timedelta
from modules.func import utils
from dateutil.relativedelta import relativedelta


def get_order_product():
    sql = '''
    SELECT
        a.order_sn,
        a.create_time,
        d.dept_name,
        b.id,
        b.order_id,
        b.product_name,
        b.sku_price,
        b.real_price,
        b.quantity
    FROM
        t_orders a
        left join t_order_item b on a.id=b.order_id
        LEFT JOIN sys_user c ON a.sys_user_id=c.user_id
        LEFT JOIN sys_dept d on c.dept_id=d.dept_id
    WHERE
        a.tenant_id = 11
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time>='{}'
    and a.create_time<'{}'
    '''.format(st, et)
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组','光芒部一组',
           '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组','光华部1组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端',
           '光芒部蜜梓源后端','光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端',
           '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端','光华部蜜梓源面膜进粉后端','光华部蜜梓源面膜老粉前端','光华部蜜梓源面膜老粉后端','光华部蜜梓源面膜进粉后端',
           '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组','光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部',
           '光芒部', '光芒部', '光芒部', '光芒部',
           '光华部', '光华部', '光华部', '光华部', '光华部','光华部','光华部',
           '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name2': df2,
          'dept_name1': df3}
    data = pd.DataFrame(df)
    return data


def save_sql(df):
    sql = '''
    INSERT INTO `t_order_item_middle` 
     (`id`,`order_sn`,`create_time`,`order_id`,`dept_name1`,`dept_name2`,`dept_name`,
     `product_name`,`sku_price`,`real_price`,`quantity`
     ) 
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_sn`= VALUES(`order_sn`),`create_time`= VALUES(`create_time`),`order_id`= VALUES(`order_id`),
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`= VALUES(`dept_name`),
         `product_name`=VALUES(`product_name`),
         `sku_price`=values(`sku_price`),`real_price`=values(`real_price`),`quantity`=values(`quantity`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 产品信息数据
    df_order_product = get_order_product()
    # 订单所属
    df_order_dept = get_hhx_user()
    df_order_product = df_order_product.merge(df_order_dept, on=['dept_name'], how='left')
    df_order_product['id'] = df_order_product['order_sn'].astype(str) + df_order_product['id'].astype(str)
    df_order_product = df_order_product.fillna(0)
    df_order_product = df_order_product[
        ['id', 'order_sn', 'create_time',  'order_id', 'dept_name1', 'dept_name2', 'dept_name', 'product_name',
         'sku_price', 'real_price', 'quantity']]
    # 保存数据
    save_sql(df_order_product)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    time1 = datetime.now()
    st = time1 - relativedelta(days=3)
    et = time1 + relativedelta(days=0)
    st = utils.date2str(st)
    et = utils.date2str(et)
    print(st, et)
    main()
