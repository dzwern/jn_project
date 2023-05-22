# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/6 14:28
# @Author  : diaozhiwei
# @FileName: hhx_order_item_middle.py
# @description: 产品信息
# @update:
"""


from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd
import datetime
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
    and a.order_amount>40
    '''.format(st, et)
    df = hhx_sql1.get_DataFrame_PD(sql)
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


# 活动信息-光源组
def get_hhx_activity(x):
    if datetime.datetime.strptime('2023-04-18','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-28','%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01','%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光芒组
def get_hhx_activity2(x):
    if datetime.datetime.strptime('2023-04-18','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-29','%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01','%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光辉前端
def get_hhx_activity3(x):
    if datetime.datetime.strptime('2023-04-19','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-27','%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-20','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01','%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光辉后端
def get_hhx_activity4(x):
    if datetime.datetime.strptime('2023-04-17','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-29','%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01','%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光华组前端
def get_hhx_activity5(x):
    if datetime.datetime.strptime('2023-04-17','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-26','%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-02-23','%Y-%m-%d'):
        return '2023年38女神节活动'
    elif datetime.datetime.strptime('2023-03-05','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-09','%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光华组后端
def get_hhx_activity6(x):
    if datetime.datetime.strptime('2023-04-17','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-26','%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-02-23','%Y-%m-%d'):
        return '2023年38女神节活动'
    elif datetime.datetime.strptime('2023-03-03','%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-09','%Y-%m-%d'):
        return '2023年38女神节活动'


def save_sql(df):
    sql = '''
    INSERT INTO `t_order_item_middle` 
     (`id`,`order_sn`,`create_time`,`order_id`,`dept_name1`,
     `dept_name2`,`dept_name`,`product_name`,`sku_price`,`real_price`,
     `quantity`,`activity_name`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `order_sn`= VALUES(`order_sn`),`create_time`= VALUES(`create_time`),`order_id`= VALUES(`order_id`),
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`= VALUES(`dept_name`),
         `product_name`=VALUES(`product_name`),
         `sku_price`=values(`sku_price`),`real_price`=values(`real_price`),`quantity`=values(`quantity`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 产品信息数据
    df_order_product = get_order_product()
    # 订单所属
    df_order_dept = get_hhx_user()
    df_order_product = df_order_product.merge(df_order_dept, on=['dept_name'], how='left')
    df_order_product = df_order_product.fillna(0)
    # 活动信息
    # 光源部
    df1 = df_order_product[df_order_product['dept_name1'] == '光源部']
    df1['activity_name'] = df1.apply(lambda x: get_hhx_activity(x['create_time']), axis=1)
    # 光芒部
    df2 = df_order_product[df_order_product['dept_name1'] == '光芒部']
    df2['activity_name'] = df2.apply(lambda x: get_hhx_activity2(x['create_time']), axis=1)
    # 光辉部蜜肤语前端
    df3 = df_order_product[df_order_product['dept_name2'] == '光辉部蜜肤语前端']
    df3['activity_name'] = df3.apply(lambda x: get_hhx_activity3(x['create_time']), axis=1)
    # 光辉部蜜肤语后端
    df4 = df_order_product[df_order_product['dept_name2'] == '光辉部蜜肤语后端']
    df4['activity_name'] = df4.apply(lambda x: get_hhx_activity4(x['create_time']), axis=1)
    # 光华部蜜梓源面膜进粉前端
    df5 = df_order_product[df_order_product['dept_name2'] == '光华部蜜梓源面膜进粉前端']
    df5['activity_name'] = df5.apply(lambda x: get_hhx_activity5(x['create_time']), axis=1)
    # 光华部蜜梓源面膜进粉后端
    df6 = df_order_product[df_order_product['dept_name2'] == '光华部蜜梓源面膜进粉后端']
    df6['activity_name'] = df6.apply(lambda x: get_hhx_activity6(x['create_time']), axis=1)
    df_order_product2 = pd.concat([df1, df2, df3, df4, df5, df6])
    # 增加
    df_order_product2 = df_order_product2[['order_sn', 'id', 'activity_name']]
    df_order_product = df_order_product.merge(df_order_product2, on=['order_sn', 'id'], how='left')
    df_order_product=df_order_product.fillna(0)
    df_order_product['id'] = df_order_product['order_sn'].astype(str) + df_order_product['id'].astype(str)
    df_order_product = df_order_product[
        ['id', 'order_sn', 'create_time',  'order_id', 'dept_name1', 'dept_name2', 'dept_name', 'product_name',
         'sku_price', 'real_price', 'quantity' , 'activity_name']]
    # 保存数据
    save_sql(df_order_product)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # time1 = datetime.datetime.now()
    # st = time1 - relativedelta(days=5)
    # et = time1 + relativedelta(days=1)
    # st = utils.date2str(st)
    # et = utils.date2str(et)
    st = '2023-01-01'
    et = '2023-05-13'
    st = datetime.datetime.strptime(st, "%Y-%m-%d")
    et = datetime.datetime.strptime(et, "%Y-%m-%d")
    print(st, et)
    main()
