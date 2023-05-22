# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/29 17:02
# @Author  : diaozhiwei
# @FileName: hhx_member_middle_log.py
# @description: 存储活动前，活动后客户的状态
# @update:
"""

from datetime import datetime
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd


# 客户基础信息
def get_member_base():
    sql='''
    SELECT
        a.id member_id,
        a.balance,
        a.point,
        a.phone,
        a.user_name user_name2,
        a.wechat_number wechat_number2,
        a.member_clue_id,
        a.member_identity,
        a.member_source,
        a.member_source_level2,
        a.incoming_line_time,
        a.add_wechat_time,
        a.wechat_id,
        b.wechat_name,
        b.wecaht_number wechat_number,
        a.sys_user_id,
        c.user_name,
        c.nick_name,
        d.dept_name
    FROM
        t_member a
    LEFT JOIN t_wechat b on a.wechat_id=b.id
    LEFT JOIN sys_user c on a.sys_user_id=c.user_id
    LEFT JOIN sys_dept d on c.dept_id=d.dept_id
    where a.tenant_id=11
    and a.add_wechat_time<'{}'
    '''.format(st)
    df=hhx_sql1.get_DataFrame_PD(sql)
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


# 客户首次沟通时间
def get_hhx_member():
    sql = '''
    select 
        a.id member_id,
        IF(a.add_wechat_time is NULL,a.incoming_line_time,a.add_wechat_time) first_time
    from 
     t_member a
    WHERE
        a.tenant_id = 11
    and a.add_wechat_time<'{}'
    '''.format(st)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 客户首次购买时间
def get_member_time():
    sql = '''
    SELECT
        a.member_id,
        min(a.create_time) create_time
    FROM t_orders a
    WHERE
    a.tenant_id = 11
    -- 订单状态
    and a.order_state NOT IN (6,8,10,11)
    # 退款状态
    and a.refund_state not in (4)
    and a.create_time<'{}'
    GROUP BY a.member_id
    '''.format(st)
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 客户销售情况
def get_member_order():
    sql = '''
    SELECT
        a.member_id,
        a.member_level,
        a.order_nums,
        a.order_amounts,
        a.order_nums_2023,
        a.order_amounts_2023,
        a.last_time 
    FROM
        t_member_level_middle_log a
    where a.log_name='{}'
    '''.format(log_name)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户身份
def get_member_identity(x):
    if x == 0:
        return '零售意向客户'
    elif x == 1:
        return '零售成交客户'
    elif x == 3:
        return '合伙人'
    elif x == 4:
        return '渠道商'
    elif x == 5:
        return '分销商'
    elif x == 6:
        return '开发商'
    elif x == 60:
        return '普通客户'
    else:
        return '其他'


# 客户来源
def get_member_source(x):
    if x == 0:
        return '小程序登录'
    elif x == 1:
        return '电商平台获取'
    elif x == 2:
        return '投流加微获取'
    elif x == 3:
        return '投流线索获取'
    elif x == 4:
        return '表单填报获取'
    elif x == 5:
        return '线下获客'
    elif x == 6:
        return '平台获取'
    else:
        return '其他'


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


def save_sql(df):
    sql = '''
    INSERT INTO `t_member_middle_log` 
     (`id`,`member_id`,`balance`,`point`,`phone`,
     `user_name2`,`wechat_number2`,`member_identity`,`member_source`,`member_source_level2`,
     `incoming_line_time`,`add_wechat_time`,`first_time`,`create_time`,`wechat_id`,
     `wechat_name`,`wechat_number`,`sys_user_id`,`user_name`,`nick_name`,
     `dept_name1`,`dept_name2`,`dept_name`,`member_level`,`order_nums`,
     `order_amounts`,`order_nums_2023`,`order_amounts_2023`,`last_time`,`last_time_diff`,`log_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `member_id`= VALUES(`member_id`),`balance`= VALUES(`balance`),`point`=VALUES(`point`),
         `phone`=values(`phone`),`user_name2`=values(`user_name2`),`wechat_number2`=values(`wechat_number2`),
         `member_identity`=values(`member_identity`),`member_source`=values(`member_source`),`member_source_level2`=values(`member_source_level2`),
         `incoming_line_time`=values(`incoming_line_time`),`add_wechat_time`=values(`add_wechat_time`),`first_time`=values(`first_time`),
         `create_time`=values(`create_time`),`wechat_id`=values(`wechat_id`),`wechat_name`=values(`wechat_name`),
         `wechat_number`=values(`wechat_number`),`sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),
         `nick_name`=values(`nick_name`),`dept_name1`=values(`dept_name1`),`dept_name2`=values(`dept_name2`),
         `dept_name`=values(`dept_name`),`member_level`=values(`member_level`),`order_nums`=values(`order_nums`),
         `order_amounts`=values(`order_amounts`),`order_nums_2023`=values(`order_nums_2023`),`order_amounts_2023`=values(`order_amounts_2023`),
         `last_time`=values(`last_time`),`last_time_diff`=values(`last_time_diff`),`log_name`=values(`log_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 基础客户数据
    df_member = get_member_base()
    # 客户所属
    df_member_user = get_hhx_user()
    df_member = df_member.merge(df_member_user, on=['dept_name'], how='left')
    # 客户首次沟通时间
    df_hhx_member = get_hhx_member()
    df_member = df_member.merge(df_hhx_member, on=['member_id'], how='left')
    # 客户首次购买时间
    df_member_time = get_member_time()
    df_member = df_member.merge(df_member_time, on=['member_id'], how='left')
    # 用户销售数据
    df_member_order = get_member_order()
    df_member_order['member_id'] = df_member_order['member_id'].astype(int)
    df_member = df_member.merge(df_member_order, on=['member_id'], how='left')
    # 客户身份
    df_member['member_identity']=df_member.apply(lambda x:get_member_identity(x['member_identity']),axis=1)
    # 客户来源
    df_member['member_source']=df_member.apply(lambda x:get_member_source(x['member_source']),axis=1)
    # 客户二级来源
    df_member['member_source_level2']=df_member.apply(lambda x:get_member_source2(x['member_source_level2']),axis=1)
    df_member['last_time'] = df_member['last_time'].fillna(0)
    df_member['last_time'] = df_member['last_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member['last_time2'] = df_member['last_time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_member['last_time_diff'] = ((pd.to_datetime(datetime.now()) - pd.to_datetime(df_member['last_time2'])) / pd.Timedelta(1,'D')).fillna(0).astype(int)
    # 记录时间
    df_member['date']=st
    df_member['id'] = df_member['member_id'].astype(str) + df_member['wechat_id'].astype(str)+df_member['date']
    df_member=df_member.fillna(0)
    df_member['incoming_line_time'] = df_member['incoming_line_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member['add_wechat_time'] = df_member['add_wechat_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member['first_time'] = df_member['first_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member['create_time'] = df_member['create_time'].apply(lambda x: '1900-01-01' if x == 0 else x)
    df_member['log_name'] = log_name
    df_member = df_member[
        ['id', 'member_id', 'balance', 'point', 'phone', 'user_name2', 'wechat_number2', 'member_identity',
         'member_source', 'member_source_level2', 'incoming_line_time', 'add_wechat_time', 'first_time',
         'create_time', 'wechat_id', 'wechat_name', 'wechat_number', 'sys_user_id', 'user_name', 'nick_name',
         'dept_name1', 'dept_name2', 'dept_name', 'member_level', 'order_nums', 'order_amounts',
         'order_nums_2023', 'order_amounts_2023', 'last_time', 'last_time_diff', 'log_name']]
    print(df_member)
    save_sql(df_member)


if __name__ == '__main__':
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    st = '2023-05-02'
    '''
    2023年1月初客户等级，2023年2月初客户等级，2023年3月初客户等级，2023年4月初客户等级，2023年5月初客户等级
    2023年38女神节活动前客户等级（2.15），2023年38女神节活动后客户等级（3.2），2023年51活动前客户等级（4.18），2023年51活动后客户等级（5.2）
    '''
    log_name='2023年51活动后客户等级'
    main()











