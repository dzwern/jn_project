# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/15 9:31
# @Author  : diaozhiwei
# @FileName: hhx_order_pred_campaign.py
# @description: 活动预估，之前数据
# @update：更新时间在，活动之前更新
"""

from modules.mysql import jnmtMySQL
import pandas as pd
import numpy as np


# 设备客户类型
def get_wechat_pred():
    sql = '''
    SELECT
        a.wechat_id,
        a.wechat_name,
        a.wecaht_number,
        a.sys_user_id,
        a.user_name,
        a.nick_name,
        a.dept_name1,
        a.dept_name2,
        a.dept_name 
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，活动总粉丝数
def get_member_category():
    sql = '''
    SELECT
        a.wechat_id,
        a.fans reality_fans
    FROM
        t_wechat_middle a 
    WHERE
        a.valid_state = '正常'
        '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户类型，新粉和活动期间新进粉丝
def get_wechat_fans(st, et):
    sql = '''
    SELECT 
        a.wechat_id,
        sum(a.credit) new_fans
    FROM 
        t_wechat_fans_log a
    LEFT JOIN t_wechat d on a.wechat_id=d.id
    WHERE a.tenant_id = 11 
    AND a.new_sprint_time >= '{}' 
    AND a.new_sprint_time < '{}'
    and d.valid_state=1
    GROUP BY a.wechat_id
    '''.format(st, et)
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 客户类型，客户分层人数
def get_member_level():
    sql = '''
    SELECT
        a.wechat_id,
        a.member_level,
        count(1) level_members
    FROM
        t_member_middle a
    GROUP BY a.wechat_id,a.member_level
    ORDER BY a.wechat_id
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户成交金额,成交数，粉丝转换，新进粉成交
def get_member_strike():
    sql='''
    SELECT 
        a.wechat_id,
        'add_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.first_time>='{}'
    and a.first_time<'{}'
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    GROUP BY a.wechat_id
    '''.format(st2,et,st2,et)
    df=hhx_sql2.get_DataFrame_PD(sql)
    return df


# 老粉成交
def get_member_strike2():
    sql='''
    SELECT 
        a.wechat_id,
        'old_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.first_time<'{}'
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    GROUP BY a.wechat_id
    '''.format(st2,et,st)
    df=hhx_sql2.get_DataFrame_PD(sql)
    return df


def get_member_strike3():
    sql='''
    SELECT 
        a.wechat_id,
        'new_fans' member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a 
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.first_time>='{}'
    and a.first_time<'{}'
    and a.clinch_type in ('当日首单日常成交','后续首单日常成交','后续首单活动成交','当日首单活动成交')
    GROUP BY a.wechat_id
    '''.format(st2,et,st,et)
    df=hhx_sql2.get_DataFrame_PD(sql)
    return df


# 客户转换
def get_member_struck():
    sql='''
    SELECT 
        a.wechat_id,
        b.member_level member_category,
        count(DISTINCT a.member_id) members_develop,
        sum(a.order_amount) members_amount
    FROM 
    t_orders_middle a
    LEFT JOIN  t_member_middle b on a.member_id=b.member_id
    WHERE a.create_time >= '{}' 
    AND a.create_time < '{}'
    and a.clinch_type in ('复购日常成交','复购活动成交')
    GROUP BY a.wechat_id,b.member_level
    ORDER BY a.wechat_id
    '''.format(st2,et)
    df=hhx_sql2.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_pred_campaign_tmp` 
     (`wechat_id`,`wechat_name`,`wechat_number`,`sys_user_id`,`user_name`,
     `nick_name`,`dept_name1`,`dept_name2`,`dept_name`,`member_category`,
     `members`,`members_develop`,`members_amount`,`member_rate`,`member_price`,
     `activity_name`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `wechat_id`= VALUES(`wechat_id`),`wechat_name`= VALUES(`wechat_name`),`wechat_number`=VALUES(`wechat_number`),
         `sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),
         `member_category`=values(`member_category`),`members`=values(`members`),`members_develop`=values(`members_develop`),
         `members_amount`=values(`members_amount`),`member_rate`=values(`member_rate`),`member_price`=values(`member_price`),
         `activity_name`=values(`activity_name`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_pred_campaign_tmp;
    '''
    hhx_sql2.executeSqlByConn(sql)


def main():
    # 基础数据，正常设备信息
    df_wechat_pred = get_wechat_pred()
    # 活动前客户数据，总粉，老粉，新粉，新进粉，V1客户，V2客户，V3客户，V4客户，V5客户
    # 总粉丝
    df_member_category = get_member_category()
    # 新粉，新进粉
    df_wechat_fans_new = get_wechat_fans(st, et)
    # 新进粉
    df_wechat_fans_new2 = get_wechat_fans(st2, et)
    # 客户分层
    df_member_level = get_member_level()
    # 汇总
    df_wechat_pred=df_wechat_pred.merge(df_member_category,on=['wechat_id'],how='left')
    df_wechat_pred['wechat_id'] = df_wechat_pred['wechat_id'].astype(int)
    df_wechat_pred=df_wechat_pred.merge(df_wechat_fans_new,on=['wechat_id'],how='left')
    df_wechat_pred=df_wechat_pred.merge(df_wechat_fans_new2,on=['wechat_id'],how='left')
    df_wechat_pred=df_wechat_pred.rename(columns={'new_fans_x':'new_fans','new_fans_y':'add_fans'})
    # 客户转换
    df_member_level = df_member_level.groupby(["wechat_id", "member_level",])['level_members'].sum().reset_index()
    df_member_level = df_member_level.set_index(["wechat_id", "member_level",])["level_members"]
    df_member_level = df_member_level.unstack().reset_index()
    df_member_level['wechat_id'] = df_member_level['wechat_id'].astype(int)
    df_wechat_pred = df_wechat_pred.merge(df_member_level,on=['wechat_id'],how='left')
    df_wechat_pred=df_wechat_pred.fillna(0)
    # 相差
    df_wechat_pred['old_fans']=df_wechat_pred['reality_fans']-df_wechat_pred['new_fans']-df_wechat_pred['add_fans']-\
                               df_wechat_pred['0']-df_wechat_pred['V1']-df_wechat_pred['V2']-df_wechat_pred['V3']-df_wechat_pred['V4']-df_wechat_pred['V5']
    df_wechat_pred=df_wechat_pred[['wechat_id','wechat_name','wecaht_number','sys_user_id','user_name','nick_name',
                                   'dept_name1','dept_name2','dept_name','old_fans','new_fans','add_fans','V1','V2','V3','V4','V5']]
    df_wechat_pred=pd.melt(df_wechat_pred,id_vars=['wechat_id','wechat_name','wecaht_number','sys_user_id','user_name','nick_name','dept_name1','dept_name2','dept_name'])
    df_wechat_pred=df_wechat_pred.rename(columns={'variable':'member_category','value':'members'})
    # 客户成交转换
    df_member_strike=get_member_strike()
    df_member_strike2=get_member_strike2()
    df_member_strike3=get_member_strike3()
    df_member_struck=get_member_struck()
    df_member_strike=pd.concat([df_member_strike,df_member_strike2,df_member_strike3,df_member_struck])
    df_member_strike['wechat_id'] = df_member_strike['wechat_id'].astype(int)
    df_wechat_pred=df_wechat_pred.merge(df_member_strike,on=['wechat_id','member_category'],how='left')
    df_wechat_pred=df_wechat_pred.fillna(0)
    # 转化率
    df_wechat_pred['member_rate']=df_wechat_pred['members_develop']/df_wechat_pred['members']
    # 客单价
    df_wechat_pred['member_price']=df_wechat_pred['members_amount']/df_wechat_pred['members_develop']
    df_wechat_pred['activity_name']='2023年女神节活动'
    df_wechat_pred = df_wechat_pred.replace([np.inf, -np.inf], np.nan)
    df_wechat_pred=df_wechat_pred.fillna(0)
    df_wechat_pred=df_wechat_pred[['wechat_id','wechat_name','wecaht_number','sys_user_id','user_name','nick_name',
                                   'dept_name1','dept_name2','dept_name','member_category','members','members_develop',
                                   'members_amount','member_rate','member_price','activity_name']]
    print(df_wechat_pred)
    # 删除数据
    del_sql()
    save_sql(df_wechat_pred)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    st = '2022-12-20'
    st2 = '2023-02-15'
    et = '2023-03-01'
    main()
