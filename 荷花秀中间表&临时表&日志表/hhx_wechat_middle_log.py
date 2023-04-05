# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/3 15:12
# @Author  : diaozhiwei
# @FileName: hhx_wechat_middle_log.py
# @description: 
# @update:
"""
import datetime

from modules.mysql import jnmtMySQL
from modules.mysql import jnmtMySQL4
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
        a.oneway_fans
    FROM
        t_wechat a 
    LEFT JOIN sys_user b on a.sys_user_id=b.user_id
    LEFT JOIN sys_dept c on a.dept_id=c.dept_id
    WHERE
        a.tenant_id = 11
    '''
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


# 员工信息
def get_hhx_user():
    df1 = ['光辉部三组', '光辉部一组', '光辉部八组', '光辉部七组', '光芒部二组', '光芒部六组', '光芒部三组',
           '光芒部一组', '光华部二组', '光华部五组', '光华部一组1', '光华部六组', '光华部三组', '光华部七组',
           '光源部蜂蜜九组', '光源部蜂蜜四组', '光源部蜂蜜五组', '光源部海参七组']
    df2 = ['光辉部蜜肤语前端', '光辉部蜜肤语前端', '光辉部蜜肤语后端', '光辉部蜜肤语后端', '光芒部蜜梓源后端',
           '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光芒部蜜梓源后端', '光华部蜜梓源面膜进粉前端',
           '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉前端', '光华部蜜梓源面膜进粉后端',
           '光华部蜜梓源面膜老粉前端', '光华部蜜梓源面膜老粉后端', '光源部蜂蜜组', '光源部蜂蜜组', '光源部蜂蜜组',
           '光源部海参组']
    df3 = ['光辉部', '光辉部', '光辉部', '光辉部', '光芒部', '光芒部', '光芒部', '光芒部', '光华部', '光华部', '光华部',
           '光华部', '光华部', '光华部', '光源部', '光源部', '光源部', '光源部']
    df = {"dept_name": df1,
          'dept_name1': df2,
          'dept_name2': df3}
    data = pd.DataFrame(df)
    return data


# 系统客户数
def get_wechat_member():
    sql = '''
    SELECT
        a.wechat_id,
        count(DISTINCT a.id) member_trans
    FROM
        t_member a 
    WHERE
        a.tenant_id = 11
    GROUP BY a.wechat_id
    '''
    df = hhx_sql.get_DataFrame_PD(sql)
    return df


def save_sql(df):
    sql = '''
    INSERT INTO `t_wechat_log` 
     (`wechat_id`,`create_time`,`version`,`wechat_name`,`wecaht_number`,
     `phone`,`phone_code`,`sys_user_id`,`user_name`,`nick_name`,
     `dept_id`,`dept_name1`,`dept_name2`,`dept_name`,`valid_state`,
     `fans`,`project_out_total_debit`,`own_fans`,`reality_fans`,`member_trans`,
     `oneway_fans`,`current_time`
     ) 
     VALUES (%s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s
     )
     ON DUPLICATE KEY UPDATE
         `wechat_id`= VALUES(`wechat_id`),`create_time`= VALUES(`create_time`),`version`=VALUES(`version`),
         `wechat_name`=values(`wechat_name`),`wecaht_number`=values(`wecaht_number`),`phone`=values(`phone`),
         `phone_code`=values(`phone_code`),`sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),
         `nick_name`=values(`nick_name`),`dept_id`=values(`dept_id`),`dept_name1`=values(`dept_name1`),
         `dept_name2`=values(`dept_name2`),`dept_name`=values(`dept_name`),`valid_state`=values(`valid_state`),
         `fans`=values(`fans`),`project_out_total_debit`=values(`project_out_total_debit`),`own_fans`=values(`own_fans`),
         `reality_fans`=values(`reality_fans`),`member_trans`=values(`member_trans`),`oneway_fans`=values(`oneway_fans`),
         `current_time`=values(`current_time`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 主函数
def main():
    # 微信设备基础数据
    df_wechat = get_wechat_base()
    # 所属部门
    df_hhx_user = get_hhx_user()
    df_wechat = df_wechat.merge(df_hhx_user, on=['dept_name'], how='left')
    # 系统客户数
    df_wechat_number = get_wechat_member()
    df_wechat = df_wechat.merge(df_wechat_number, on=['wechat_id'], how='left')
    # 有效状态
    df_wechat['valid_state']=df_wechat['valid_state'].apply(lambda x: '正常' if x==1 else '维护')
    df_wechat['reality_fans']=df_wechat['fans']-df_wechat['project_out_total_debit']-df_wechat['own_fans']
    df_wechat=df_wechat.fillna(0)
    df_wechat['current_time']=datetime.datetime.now()
    df_wechat['current_time']=df_wechat['current_time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_wechat=df_wechat
    print(df_wechat)
    df_wechat = df_wechat[['wechat_id', 'create_time', 'version', 'wechat_name', 'wecaht_number', 'phone', 'phone_code',
                           'sys_user_id', 'user_name', 'nick_name', 'dept_id', 'dept_name1', 'dept_name2', 'dept_name',
                           'valid_state', 'fans', 'project_out_total_debit', 'own_fans', 'reality_fans', 'member_trans',
                           'oneway_fans','current_time']]
    df_wechat=df_wechat.fillna(0)
    print(df_wechat)
    save_sql(df_wechat)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    # hhx_sql = jnmtMySQL4.QunaMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # hhx_sql2 = jnmtMySQL4.QunaMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 开始时间，结束时间
    # startTime = utils.get_time_args(sys.argv)
    # time1 = startTime
    # st = time1 - relativedelta(days=1)
    # et = time1 - relativedelta(days=0)
    # 时间转化
    st = '2023-02-01'
    et = '2023-03-01'
    main()











