# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/31 14:06
# @Author  : diaozhiwei
# @FileName: hhx_wechat_middle.py
# @description: 微信设备信息，主要字段微信所属员工，所属部门，以及粉丝客户情况
# @update: 增量更新，每日更新
"""

from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd


# 微信基础设备
def get_wechat_base():
    sql = '''
    SELECT
        a.id wechat_id,
        a.create_time,
        a.version,
        a.wechat_name,
        a.wecaht_number,
        a.phone,
        a.phone_code,
        a.sys_user_id,
        b.nick_name,
        b.user_name,
        a.dept_id,
        c.dept_name,
        a.valid_state,
        a.fans,
        a.project_out_total_debit,
        a.own_fans,
        a.oneway_fans,
        a.tenant_id
    FROM
        t_wechat a 
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on b.dept_id=c.dept_id
    WHERE
       a.tenant_id  in ( '25', '26', '27', '28' ) 
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    sql = '''
    SELECT
        a.dept_name,
        a.dept_name1,
        a.dept_name2,
        a.tenant_id tenant_id2
    FROM
        t_dept_tmp a
    '''
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 系统客户数
def get_wechat_member():
    sql = '''
    SELECT
        a.wechat_id,
        count(DISTINCT a.id) member_trans
    FROM
        t_member a 
    WHERE
        a.tenant_id  in ( '25', '26', '27', '28' ) 
    GROUP BY a.wechat_id
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_wechat_middle` 
     (`wechat_id`,`create_time`,`version`,`wechat_name`,`wecaht_number`,
     `phone`,`phone_code`,`sys_user_id`,`user_name`,`nick_name`,
     `dept_id`,`dept_name1`,`dept_name2`,`dept_name`,`valid_state`,
     `fans`,`project_out_total_debit`,`own_fans`,`reality_fans`,`member_trans`,
     `oneway_fans`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s
     )
     ON DUPLICATE KEY UPDATE
         `wechat_id`= VALUES(`wechat_id`),`create_time`= VALUES(`create_time`),`version`=VALUES(`version`),
         `wechat_name`=values(`wechat_name`),`wecaht_number`=values(`wecaht_number`),`phone`=values(`phone`),
         `phone_code`=values(`phone_code`),`sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),
         `nick_name`=values(`nick_name`),`dept_id`=values(`dept_id`),`dept_name1`=values(`dept_name1`),
         `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),`valid_state`=values(`valid_state`),
         `fans`=values(`fans`),`project_out_total_debit`=values(`project_out_total_debit`),`own_fans`=values(`own_fans`),
         `reality_fans`=values(`reality_fans`),`member_trans`=values(`member_trans`),`oneway_fans`=values(`oneway_fans`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_wechat_middle;
    '''
    hhx_sql2.executeSqlByConn(sql)


# 主函数
def main():
    # 微信设备基础数据
    df_wechat = get_wechat_base()
    # 所属部门
    df_hhx_user = get_hhx_user()
    df_wechat = df_wechat.merge(df_hhx_user, on=['dept_name'], how='left')
    # 判断数据，旧系统更新到新系统ID变更
    df_wechat=df_wechat.fillna(0)
    df_wechat['fuzhu']=df_wechat['tenant_id2']-df_wechat['tenant_id']
    df_wechat=df_wechat.loc[df_wechat['fuzhu']==0,:]
    # 系统客户数
    df_wechat_number = get_wechat_member()
    df_wechat = df_wechat.merge(df_wechat_number, on=['wechat_id'], how='left')
    # 有效状态
    df_wechat['valid_state'] = df_wechat['valid_state'].apply(lambda x: '正常' if x == 1 else '维护')
    df_wechat['reality_fans'] = df_wechat['fans'] - df_wechat['project_out_total_debit'] - df_wechat['own_fans']
    df_wechat = df_wechat.fillna(0)
    df_wechat = df_wechat[['wechat_id', 'create_time', 'version', 'wechat_name', 'wecaht_number', 'phone', 'phone_code',
                           'sys_user_id', 'user_name', 'nick_name', 'dept_id', 'dept_name1', 'dept_name2', 'dept_name',
                           'valid_state', 'fans', 'project_out_total_debit', 'own_fans', 'reality_fans', 'member_trans',
                           'oneway_fans']]
    df_wechat = df_wechat.fillna(0)
    print(df_wechat)
    del_sql()
    save_sql(df_wechat)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()
