# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:23
# @Author  : diaozhiwei
# @FileName: demo_order_middle.py
# @description: 荷花秀订单基础信息表，主要内容有订单类型，订单时间，订单状态，订单客户等信息
# @update：增量更新，每小时更新前7天数据，保证状态变更
"""
import datetime
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import sys
from dateutil.relativedelta import relativedelta

userName = 'dzw'
password = 'dsf#4oHGd'
dbHost = 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com'
dbPort = 3306
URL = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/'
schema = 'crm_tm_jnmt'
schema2 = 'hhx_dx'
engine = create_engine(URL + schema + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)
engine2 = create_engine(URL + schema2 + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)


# 加载数据到df
def get_DataFrame_PD(sql='SELECT * FROM DUAL'):
    conn = engine.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 批量执行更新sql语句
def executeSqlManyByConn(sql, data):
    conn = engine2.connect()
    if len(data) > 0:
        with conn as connection:
            return connection.execute(sql, data)


def get_sql():
    sql = '''
    select * from sys_tenant
    '''
    df = get_DataFrame_PD(sql)
    return df


# 时间转化字符串
def date2str(parameter, format='%Y-%m-%d'):
    if isinstance(parameter, str):
        return parameter
    return parameter.strftime(format)


# 订单基础信息
def get_hhx_orders():
    sql = '''
    SELECT
        a.order_sn,
        a.original_order_sn,
        a.order_type,
        b.no_performance_type,
        a.sys_user_id,
        c.user_name,
        c.nick_name,
        a.wechat_id,
        e.wechat_name,
        e.wecaht_number,
        d.dept_name,
        a.member_id,
        a.create_time,
        a.trade_time,
        a.complate_date,
        a.order_amount,
        a.amount_paid,
        a.refund_amount,
        a.order_state,
        a.review_state,
        a.return_state,
        a.pay_type_name,
        a.project_category_id,
        a.receiver_province,
        a.receiver_city
    FROM
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    LEFT JOIN sys_user c on a.sys_user_id = c.user_id
    LEFT JOIN sys_dept d on c.dept_id=d.dept_id
    LEFT JOIN t_wechat e on a.wechat_id=e.id 
    WHERE
        a.tenant_id = 11
    and a.create_time>='{}'
    and a.create_time<'{}'
    '''.format(st, et)
    df = get_DataFrame_PD(sql)
    return df


# 订单类型
'''
0零售订单
4售后订单
5营销订单
'''


def get_order_type(x):
    if x == 0:
        return '零售订单'
    elif x == 4:
        return '售后订单'
    elif x == 5:
        return '营销订单'
    else:
        return '其他'


# 营销类型
'''
1补发订单
2客户品鉴
3客户激活
4客户维护
5换货补发
6正常销售
7裂变赠礼
8平台赠礼
9客户反加
10换货订单
12积分兑换
13客户转移
14公池客户反加
15招商品鉴
16平台赠礼
17大客户维护
'''


def get_no_performance_type(x):
    if x == 1:
        return '订单补发'
    elif x == 2:
        return '客户品鉴'
    elif x == 3:
        return '客户激活'
    elif x == 4:
        return '客户维护'
    elif x == 5:
        return '换货补发'
    elif x == 6:
        return '正常销售'
    elif x == 7:
        return '裂变赠礼'
    elif x == 8:
        return '平台赠礼'
    elif x == 9:
        return '客户反加'
    elif x == 10:
        return '换货订单'
    elif x == 12:
        return '积分总换'
    elif x == 13:
        return '客户转移'
    elif x == 14:
        return '公池客户反加'
    elif x == 15:
        return '招商品鉴'
    elif x == 16:
        return '平台赠礼'
    elif x == 17:
        return '大客户维护'
    else:
        return '其他'


# 成交类型
def get_clinch_type():
    sql = '''
    select * from
    (
    (
    SELECT
        a.order_sn,
        (case when b.clinch_type=0 then '当日首单日常成交'
                    when b.clinch_type=1 then '当日首单活动成交' 
                    when b.clinch_type=2 then '其他' END) clinch_type
    FROM 
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    WHERE a.tenant_id = 11 
    and a.create_time>='{}'
    and a.create_time<'{}'
    and is_first_order=1
    )
    UNION
    (
    SELECT
        a.order_sn,
        (case when b.clinch_type=0 then '后续首单日常成交'
                    when b.clinch_type=1 then '后续首单活动成交' 
                    when b.clinch_type=2 then '其他' END)types
    FROM 
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    WHERE a.tenant_id = 11
    and a.create_time>='{}'
    and a.create_time<'{}' 
    and b.is_follow_order=1
    )
    UNION
    (
    SELECT
        a.order_sn,
        (case when b.clinch_type=0 then '复购日常成交'
                    when b.clinch_type=1 then '复购活动成交' 
                    when b.clinch_type=2 then '其他' END)types
    FROM 
        t_orders a 
    LEFT JOIN t_order_rel_info b on a.id=b.orders_id
    WHERE a.tenant_id = 11
    and a.create_time>='{}'
    and a.create_time<'{}' 
    and b.is_repurchase=1
    )
    )t1
    '''.format(st, et, st, et, st, et)
    df = get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组',
           '光芒部二组', '光芒部六组', '光芒部三组', '光芒部一组',
           '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组', '光华部1组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端',
           '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端',
           '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端',
           '光华部蜜梓源面膜进粉后端', '光华部蜜梓源面膜老粉前端', '光华部蜜梓源面膜老粉后端',
           '光华部蜜梓源面膜进粉后端',
           '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组', '光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部',
           '光芒部', '光芒部', '光芒部', '光芒部',
           '光华部', '光华部', '光华部', '光华部', '光华部', '光华部', '光华部',
           '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name2': df2,
          'dept_name1': df3}
    data = pd.DataFrame(df)
    return data


# 客户来源
def get_member_source():
    sql = '''
    SELECT
        a.order_sn,
        b.member_source_level2 member_source
    FROM 
        t_orders a
    LEFT JOIN t_member b on a.member_id=b.id
    WHERE a.tenant_id = 11 
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.order_sn
    '''.format(st, et)
    df = get_DataFrame_PD(sql)
    return df


# 客户来源枚举
'''
0小程序登录
1电商平台获取
2投流加微获取
3投流线索获取
4表单获取
5线下获客
6客户咨询
'''


# 客户二级来源
def get_member_source2(x):
    if x == 0:
        return '平jie甄选'
    elif x == 1:
        return '线下小程序'
    elif x == 100:
        return '淘宝站内'
    elif x == 101:
        return '京东站内'
    elif x == 102:
        return '拼多多站内'
    elif x == 103:
        return '直购抖音小店'
    elif x == 104:
        return '直播抖音小店'
    elif x == 105:
        return '直播快手小店'
    elif x == 106:
        return '有赞小店'
    elif x == 107:
        return '京东站外'
    elif x == 108:
        return '淘宝站外'
    elif x == 200:
        return '主动裂变'
    elif x == 201:
        return '被动裂变'
    elif x == 202:
        return '线上投流加微'
    elif x == 300:
        return '百度'
    elif x == 301:
        return '快手'
    elif x == 302:
        return '腾讯'
    elif x == 303:
        return '抖音'
    elif x == 304:
        return '网易'
    elif x == 501:
        return '会展'
    elif x == 502:
        return '品鉴会'
    elif x == 503:
        return '门店'
    elif x == 504:
        return '厂家'
    elif x == 505:
        return '内部客户'
    elif x == 600:
        return '400电话咨询'
    elif x == 601:
        return '公众号咨询'
    else:
        return '其他'


# 客户信息,首次沟通时间
def get_hhx_member():
    sql = '''
    select 
        a.id member_id,
        IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time
    from 
     t_member a
    WHERE
        a.tenant_id = 11
    '''
    df = get_DataFrame_PD(sql)
    return df


# 商品信息

def get_product_name():
    sql = '''
    SELECT 
	    a.order_sn,
	    CONCAT('【', GROUP_CONCAT(CONCAT_WS('|', b.product_name, TRIM(BOTH '"' from JSON_EXTRACT(b.specification_values, '$[0].value')), b.quantity) separator '】【'), '】') product_name
    FROM 
        t_orders a
    left join t_order_item b on a.id = b.order_id
    WHERE a.tenant_id = 11 
    and a.create_time>='{}'
    and a.create_time<'{}'
    GROUP BY a.order_sn
    '''.format(st, et)
    df = get_DataFrame_PD(sql)
    return df


# 订单区间
'''
1k以下				
1k-2k				
2k-3k				
3k-4k				
4k-5k				
5k以上			
'''


def get_order_range(x):
    if x < 1000:
        return '1k以下'
    elif 1000 <= x < 2000:
        return '1k-2k'
    elif 2000 <= x < 3000:
        return '2k-3k'
    elif 3000 <= x < 4000:
        return '3k-4k'
    elif 4000 <= x < 5000:
        return '4k-5k'
    elif x >= 5000:
        return '5k以上'
    else:
        return '其他'


# 项目类别
'''
4蜜梓源面膜
5蜜梓源蜂蜜
6蜜肤语面膜
9参总管海参
'''


def get_project_category_id(x):
    if x == 4:
        return '蜜梓源面膜'
    elif x == 5:
        return '蜜梓源蜂蜜'
    elif x == 6:
        return '蜜肤语面膜'
    elif x == 9:
        return '参总管海参'
    else:
        return '其它'


# 订单状态
'''
0待稽查审核
1待售后审核
2待主管审核
3待产品部审核
4待运营部审核
5待市场部审核
6订单取消
7已完结
8订单驳回
9拒收途中
10拒收完结无异常
11拒收完结有异常
13待仓库审核
14待同步快递
17待签收
18待确认拦回
'''


def get_order_state(x):
    if x == 0:
        return '待稽查审核'
    elif x == 1:
        return '待售后审核'
    elif x == 2:
        return '待主管审核'
    elif x == 3:
        return '待产品部审核'
    elif x == 4:
        return '待运营部审核'
    elif x == 5:
        return '待市场部审核'
    elif x == 6:
        return '订单取消'
    elif x == 7:
        return '已完结'
    elif x == 8:
        return '订单驳回'
    elif x == 9:
        return '拒收途中'
    elif x == 10:
        return '拒收完结无异常'
    elif x == 11:
        return '拒收完结有异常'
    elif x == 13:
        return '待仓库审核'
    elif x == 14:
        return '待同步快递'
    elif x == 17:
        return '待签收'
    elif x == 18:
        return '待确认拦回'
    else:
        return '其它'


# 审核状态
def get_review_state(x):
    if x == 0:
        return '等通知'
    elif x == 1:
        return '待审核'
    elif x == 2:
        return '审核中'
    elif x == 3:
        return '审核通过'
    else:
        return '其他'


# 退款状态
'''
0无退款
1申请中
2申请拒绝
3部分退款
4全部退款
'''


def get_return_state(x):
    if x == 0:
        return '无退款'
    elif x == 1:
        return '申请中'
    elif x == 2:
        return '申请拒绝'
    elif x == 3:
        return '部分退款'
    elif x == 4:
        return '全部退款'
    else:
        return '其它'


# 活动信息-光源蜂蜜
def get_hhx_activity(x):
    if datetime.datetime.strptime('2023-04-18', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-30',
                                                                                               '%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息-光源海参
def get_hhx_activity1(x):
    if datetime.datetime.strptime('2023-04-18', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-30',
                                                                                               '%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-03-01', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-11',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光芒组
def get_hhx_activity2(x):
    if datetime.datetime.strptime('2023-04-18', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-05-01',
                                                                                               '%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光辉前端
def get_hhx_activity3(x):
    if datetime.datetime.strptime('2023-04-19', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-28',
                                                                                               '%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-20', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光辉后端
def get_hhx_activity4(x):
    if datetime.datetime.strptime('2023-04-17', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-05-01',
                                                                                               '%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-01',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光华组前端
def get_hhx_activity5(x):
    if datetime.datetime.strptime('2023-04-19', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-29',
                                                                                               '%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-02-23',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'
    elif datetime.datetime.strptime('2023-03-05', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-09',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'


# 活动信息2-光华组后端
def get_hhx_activity6(x):
    if datetime.datetime.strptime('2023-04-17', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-04-29',
                                                                                               '%Y-%m-%d'):
        return '2023年五一活动'
    elif datetime.datetime.strptime('2023-02-15', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-02-23',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'
    elif datetime.datetime.strptime('2023-03-03', '%Y-%m-%d') <= x <= datetime.datetime.strptime('2023-03-09',
                                                                                                 '%Y-%m-%d'):
        return '2023年38女神节活动'


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `t_orders_middle` 
     (`id`,`order_sn`,`original_order_sn`,`order_type`,`no_performance_type`,
     `clinch_type`,`dept_name1`,`dept_name2`,`dept_name`,`sys_user_id`,
     `user_name`,`nick_name`,`wechat_id`,`wechat_name`,`wechat_number`,
     `member_id`,`member_source`,`first_time`,`create_time`,`time_diff`,
     `receiver_province`,`receiver_city`,
     `product_name`,`order_amount`, `amount_paid`,`refund_amount`,
     `pay_type_name`,`order_interval`,`order_state`, `review_state`,`refund_state`,
     `trade_time`,`complate_date`,`project_category_id`,`is_activity`,`activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s
     )
     ON DUPLICATE KEY UPDATE
         `original_order_sn`= VALUES(`original_order_sn`),`order_type`=VALUES(`order_type`),
         `no_performance_type`=values(`no_performance_type`),`clinch_type`=values(`clinch_type`),`dept_name1`=values(`dept_name1`),
         `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),`sys_user_id`=values(`sys_user_id`),
         `user_name`=values(`user_name`),`nick_name`=values(`nick_name`),`wechat_id`=values(`wechat_id`),
         `wechat_name`=values(`wechat_name`),`wechat_number`=values(`wechat_number`),`member_id`=values(`member_id`),
         `member_source`=values(`member_source`),`first_time`=values(`first_time`),`create_time`=values(`create_time`),
         `time_diff`=values(`time_diff`),`receiver_province`=values(`receiver_province`),`receiver_city`=values(`receiver_city`),
         `product_name`=values(`product_name`),`order_amount`=values(`order_amount`),
         `amount_paid`=values(`amount_paid`),`refund_amount`=values(`refund_amount`),`pay_type_name`=values(`pay_type_name`),
         `order_interval`=values(`order_interval`),`order_state`=values(`order_state`),`review_state`=values(`review_state`),
         `refund_state`=values(`refund_state`),`trade_time`=values(`trade_time`),`complate_date`=values(`complate_date`),
         `project_category_id`=values(`project_category_id`),`is_activity`=values(`is_activity`),`activity_name`=values(`activity_name`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础数据表
    df_hhx_orders = get_hhx_orders()
    # 订单类型
    df_hhx_orders['order_type'] = df_hhx_orders.apply(lambda x: get_order_type(x['order_type']), axis=1)
    # 营销类型
    df_hhx_orders['no_performance_type'] = df_hhx_orders.apply(
        lambda x: get_no_performance_type(x['no_performance_type']), axis=1)
    # 成交类型
    df_clinch_type = get_clinch_type()
    df_hhx_orders = df_hhx_orders.merge(df_clinch_type, on=['order_sn'], how='left')
    # 员工信息
    df_hhx_user = get_hhx_user()
    df_hhx_orders = df_hhx_orders.merge(df_hhx_user, on=['dept_name'], how='left')
    # 客户信息
    df_hhx_member = get_hhx_member()
    df_hhx_orders = df_hhx_orders.merge(df_hhx_member, on=['member_id'], how='left')
    # 客户来源
    df_member_source = get_member_source()
    df_hhx_orders = df_hhx_orders.merge(df_member_source, on=['order_sn'], how='left')
    df_hhx_orders['member_source'] = df_hhx_orders.apply(lambda x: get_member_source2(x['member_source']), axis=1)
    # 沟通时间差
    df_hhx_orders['first_time'] = df_hhx_orders.apply(
        lambda x: "2000-01-01" if pd.to_datetime(x['first_time']) < datetime.datetime(2000,1,1) else x['first_time'], axis=1)
    df_hhx_orders['ct'] = df_hhx_orders['create_time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_hhx_orders['ft'] = df_hhx_orders['first_time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_hhx_orders['time_diff'] = ((pd.to_datetime(df_hhx_orders['ct']) - pd.to_datetime(df_hhx_orders['ft'])) / pd.Timedelta(1,'D')).fillna(0).astype(int)
    # 订单信息
    df_product_name = get_product_name()
    df_hhx_orders = df_hhx_orders.merge(df_product_name, on=['order_sn'], how='left')
    # 订单范围
    df_hhx_orders['order_interval'] = df_hhx_orders.apply(lambda x: get_order_range(x['order_amount']), axis=1)
    # 项目类型
    df_hhx_orders['project_category_id'] = df_hhx_orders.apply(
        lambda x: get_project_category_id(x['project_category_id']), axis=1)
    # 订单状态
    df_hhx_orders['order_state'] = df_hhx_orders.apply(lambda x: get_order_state(x['order_state']), axis=1)
    # 审核状态
    df_hhx_orders['review_state'] = df_hhx_orders.apply(lambda x: get_review_state(x['review_state']), axis=1)
    # 退货状态
    df_hhx_orders['refund_state'] = df_hhx_orders.apply(lambda x: get_return_state(x['return_state']), axis=1)
    # 活动信息
    # 光源部
    df0 = df_hhx_orders[df_hhx_orders['dept_name2'] == '光源部蜂蜜组']
    df0['activity_name'] = df0.apply(lambda x: get_hhx_activity(x['create_time']), axis=1)
    # 光源部海参组
    df1 = df_hhx_orders[df_hhx_orders['dept_name2'] == '光源部海参组']
    df1['activity_name'] = df1.apply(lambda x: get_hhx_activity1(x['create_time']), axis=1)
    # 光芒部
    df2 = df_hhx_orders[df_hhx_orders['dept_name1'] == '光芒部']
    df2['activity_name'] = df2.apply(lambda x: get_hhx_activity2(x['create_time']), axis=1)
    # 光辉部蜜肤语前端
    df3 = df_hhx_orders[df_hhx_orders['dept_name2'] == '光辉部蜜肤语前端']
    df3['activity_name'] = df3.apply(lambda x: get_hhx_activity3(x['create_time']), axis=1)
    # 光辉部蜜肤语后端
    df4 = df_hhx_orders[df_hhx_orders['dept_name2'] == '光辉部蜜肤语后端']
    df4['activity_name'] = df4.apply(lambda x: get_hhx_activity4(x['create_time']), axis=1)
    # 光华部蜜梓源面膜进粉前端
    df5 = df_hhx_orders[df_hhx_orders['dept_name2'] == '光华部蜜梓源面膜进粉前端']
    df5['activity_name'] = df5.apply(lambda x: get_hhx_activity5(x['create_time']), axis=1)
    # 光华部蜜梓源面膜进粉后端
    df6 = df_hhx_orders[df_hhx_orders['dept_name2'] == '光华部蜜梓源面膜进粉后端']
    df6['activity_name'] = df6.apply(lambda x: get_hhx_activity6(x['create_time']), axis=1)
    df_hhx_orders2 = pd.concat([df0, df1, df2, df3, df4, df5, df6])
    # 增加活动信息
    df_hhx_orders2 = df_hhx_orders2[['order_sn', 'member_id', 'activity_name']]
    df_hhx_orders = df_hhx_orders.merge(df_hhx_orders2, on=['order_sn', 'member_id'], how='left')
    # 是否活动
    df_hhx_orders['is_activity'] = df_hhx_orders.apply(lambda x: '否' if x['activity_name'] is None else '是', axis=1)
    df_hhx_orders['id'] = df_hhx_orders['order_sn'].astype(str) + df_hhx_orders['member_id'].astype(str)
    df_hhx_orders = df_hhx_orders.fillna(0)
    df_hhx_orders['complate_date'] = df_hhx_orders['complate_date'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_hhx_orders = df_hhx_orders[
        ['id', 'order_sn', 'original_order_sn', 'order_type', 'no_performance_type', 'clinch_type',
         'dept_name1', 'dept_name2', 'dept_name', 'sys_user_id', 'user_name', 'nick_name',
         'wechat_id', 'wechat_name', 'wecaht_number', 'member_id', 'member_source', 'first_time',
         'create_time', 'time_diff', 'receiver_province', 'receiver_city', 'product_name',
         'order_amount', 'amount_paid', 'refund_amount',
         'pay_type_name', 'order_interval', 'order_state', 'review_state', 'refund_state',
         'trade_time', 'complate_date', 'project_category_id', 'is_activity', 'activity_name']]
    print(df_hhx_orders)
    save_sql(df_hhx_orders)


if __name__ == '__main__':
    # 开始时间，结束时间
    time1 = datetime.datetime.now()
    st = time1 - relativedelta(days=5)
    et = time1 + relativedelta(days=1)
    st = date2str(st)
    et = date2str(et)
    print(st, et)
    main()



