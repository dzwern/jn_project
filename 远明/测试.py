# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/16 17:38
# @Author  : diaozhiwei
# @FileName: hhx_dai_sku_orders.py
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


def get_sku_orders():
    sql = '''
    SELECT 
        所属部门,渠道,客户ID,订单编号,商品名称,商品简称,商品类型,规格,商品编号,数量,中心,部门,小组,
        销售价,实际支付单价,订单状态,下单日期,订单类型,地址,订单金额,退款金额,结余金额,营销类型,    
        unit_old AS '订单规格',
        unit_new AS '换算规格',
        rate AS '换算率',
        CASE WHEN rate IS NOT NULL THEN 数量/rate ELSE 0 END AS '销售数量'
        FROM (
        SELECT
      	t.dept_name AS '所属部门',
      	CASE WHEN t.dept_name LIKE '销售%%' THEN '销售部' ELSE '市场部' END AS '渠道',
        member_id AS '客户ID',
        order_sn AS '订单编号',
        t.product_name AS '商品名称',
        common_name AS '商品简称',
        is_wine AS '商品类型',
        TRIM( 
        BOTH '"'
        FROM JSON_EXTRACT(specification_values, '$[0].value')
    ) AS '规格',
        sku_sn AS '商品编号',
        quantity AS '数量',
        sku_price AS '销售价',
        real_price AS '实际支付单价',
        CASE
            WHEN order_state IN (0, 1, 2, 3, 4, 5) THEN '审核阶段'
            WHEN order_state = 6 THEN '订单取消'
            WHEN order_state = 7 THEN '已完结'
            WHEN order_state = 8 THEN '订单驳回'
            WHEN order_state BETWEEN 9 AND 11 THEN '拒收'
            WHEN order_state = 12 THEN '待支付'
            WHEN order_state BETWEEN 13 AND 16 THEN '待发货'
            WHEN order_state = 17 THEN '待签收'
            WHEN order_state BETWEEN 18 AND 21 THEN '订单拦回'
            WHEN order_state = 100 THEN '异常订单'
            ELSE '未归类'
        END AS '订单状态',
        create_time AS '下单日期',
        yx_type AS '订单类型',
        CONCAT(receiver_province, receiver_city) AS '地址',
        order_amount AS '订单金额',
        refund_amount AS '退款金额',
        order_amount - refund_amount AS '结余金额',
        CASE
            WHEN (no_performance_type = 1) THEN '补发订单'
            WHEN (no_performance_type = 2) THEN '客户品鉴'
            WHEN (no_performance_type = 3) THEN '客户激活'
            WHEN (no_performance_type = 4) THEN '客户维护'
            WHEN (no_performance_type = 5) THEN '私人定制'
            WHEN (no_performance_type = 6) THEN '正常销售'
            WHEN (no_performance_type = 7) THEN '裂变赠礼'
            WHEN (no_performance_type = 8) THEN '平台赠礼'
            WHEN (no_performance_type = 9) THEN '客户反加'
            WHEN (no_performance_type = 10) THEN '换货订单'
            WHEN (no_performance_type = 11) THEN '远币兑换'
            WHEN (no_performance_type = 12) THEN '积分兑换'
            WHEN (no_performance_type = 13) THEN '客户转移'
            WHEN (no_performance_type = 14) THEN '客户封坛'
        END AS '营销类型',
    dd.centel AS '中心',
    CASE
        WHEN dd.department IS NULL THEN '无部门'
        ELSE dd.department
    END '部门',
    CASE
        WHEN dd.gro IS NULL THEN '无小组'
        ELSE dd.gro
    END '小组'
    FROM
        (
        SELECT
            o.id AS order_id,
            d.dept_name,
            o.member_id,
            oi.id AS item_id,
            oi.product_name,
            oi.specification_values,
            oi.quantity,
            o.order_state,
            o.sys_user_id,
            m.incoming_line_time,
            m.member_source,
            m.member_source_level2,
            o.create_time,
            o.order_sn,
            o.order_type,
            o.receiver_name,
            o.receiver_detail_address,
            o.order_amount,
            o.refund_amount,
            u.nick_name,
            dd2.dict_label AS yx_type,
            m.member_type,
            o.receiver_province,
            o.receiver_city,
            o.receiver_region,
            ori.no_performance_type,
            u.user_name,
            sku.sn AS sku_sn,
            oi.sku_price,
            oi.real_price
        FROM
            crm_tm_ymlj.t_orders AS o
        LEFT JOIN crm_tm_ymlj.sys_user AS u ON 
            u.user_id = o.sys_user_id
        LEFT JOIN crm_tm_ymlj.sys_dept AS d ON
            d.dept_id = u.dept_id
        LEFT JOIN crm_tm_ymlj.t_order_item AS oi ON
            oi.order_id = o.id
        LEFT JOIN crm_tm_ymlj. t_sku AS sku ON
            sku.id = oi.sku_id
        LEFT JOIN crm_tm_ymlj.t_member AS m ON
            m.id = o.member_id
        INNER JOIN crm_tm_ymlj. t_order_rel_info AS ori ON
            ori.orders_id = o.id
            --      and ori.no_performance_type IN (3, 6) -- -- 只取正常销售、沉睡激活
        LEFT JOIN crm_tm_ymlj.t_wechat AS w ON
            w.id = o.wechat_id
        LEFT JOIN crm_tm_ymlj.sys_dict_data AS dd2 ON
            dd2.dict_type = 'order_type'
            AND dd2.dict_value = o.order_type
        WHERE
            1 = 1
            AND o.order_state IN (0, 1, 2, 3, 4, 5, 7, 12, 13, 14, 15, 16, 17)
            -- 筛选审核阶段、已完结、待发货、待签收
            AND (d.dept_name LIKE '销售%%' OR d.dept_name LIKE '运维部%%' 
                   OR d.dept_name LIKE '酒厂开发%%' OR d.dept_name LIKE '市场开发%%')
            AND o.create_time >= '2023-05-01 00:00:00'
            -- and o.create_time < '2022-07-01 00:00:00'
    ) t
    LEFT JOIN (
        SELECT
            product_name,
            common_name,
            is_wine
        FROM
            ymlj_dx.t_product_info
    ) a ON
        t.product_name = a.product_name
    LEFT JOIN ymlj_dx.dai_dept dd ON t.dept_name = dd.dept_name
)c LEFT JOIN (
SELECT unit_old,unit_new,rate
FROM ymlj_dx.t_product_unit_rate
)p ON c.规格 = p.unit_old
    '''
    df = get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `dai_sku_orders` 
     (`id`,`dept_name`,`qudao`,`member_id`,`order_sn`,`product_name`,
     `common_name`,`is_wine`,`specification_values`,`sku_sn`,`quantity`,
     `centel`,`department`,`gro`,`sku_price`,`real_price`,
     `order_state`,`create_time`,`yx_type`,`receiver_province`,`order_amount`,
     `refund_amount`,`jieyu_amount`,`no_performance_type`,`unit_old`,`unit_new`,
     `rate`,`quantitys`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name`= VALUES(`dept_name`),`qudao`= VALUES(`qudao`),`member_id`= VALUES(`member_id`),`order_sn`=VALUES(`order_sn`),
         `product_name`=values(`product_name`),`common_name`=values(`common_name`),`is_wine`=values(`is_wine`),
         `specification_values`=values(`specification_values`),`sku_sn`=values(`sku_sn`),`quantity`=values(`quantity`),
         `centel`=values(`centel`),`department`=values(`department`),`gro`=values(`gro`),
         `sku_price`=values(`sku_price`),`real_price`=values(`real_price`),`order_state`=values(`order_state`),
         `create_time`=values(`create_time`),`yx_type`=values(`yx_type`),`receiver_province`=values(`receiver_province`),
         `order_amount`=values(`order_amount`),`refund_amount`=values(`refund_amount`),`jieyu_amount`=values(`jieyu_amount`),
         `no_performance_type`=values(`no_performance_type`),`unit_old`=values(`unit_old`),`unit_new`=values(`unit_new`),
         `rate`=values(`rate`),`quantitys`=values(`quantitys`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table dai_sku_orders;
    '''
    executeSqlByConn(sql)


def main():
    df_sku_orders = get_sku_orders()
    df_sku_orders = df_sku_orders.fillna(0)
    df_sku_orders['id'] = df_sku_orders['订单编号']
    df_sku_orders = df_sku_orders[
        ['id', '所属部门', '渠道', '客户ID', '订单编号', '商品名称', '商品简称', '商品类型',
         '规格', '商品编号', '数量', '中心', '部门', '小组',
         '销售价', '实际支付单价', '订单状态', '下单日期', '订单类型', '地址', '订单金额',
         '退款金额', '结余金额', '营销类型', '订单规格', '换算规格', '换算率', '销售数量']]
    del_sql()
    save_sql(df_sku_orders)


if __name__ == '__main__':
    main()


