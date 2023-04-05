# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/3 9:30
# @Author  : diaozhiwei
# @FileName: hhx_fans_develop_day.py
# @description: 进粉开发，基础数据为各个设备每日进粉数，以及在之后的订单中开发的客户数，计算粉丝开发率
# @update:
"""

from modules.mysql import jnmtMySQL
import pandas as pd
import datetime


# 基础数据，每日进粉数
def get_member_credit():
    sql = '''
    SELECT
        f.dept_name,
        e.nick_name,
        d.id wechat_id,
        d.wecaht_number wechat_number,
        left(a.new_sprint_time,10) date2,
        sum(a.credit) jinfen 
    FROM t_wechat_fans_log a
    LEFT JOIN t_wechat d on d.id=a.wechat_id
    LEFT JOIN sys_user e on e.user_id=d.sys_user_id
    LEFT JOIN sys_dept f on e.dept_id=f.dept_id
    where a.tenant_id=11
    and a.new_sprint_time>='{}'
    and a.new_sprint_time<'{}'
    and a.credit>0
    GROUP BY f.dept_name,e.nick_name,d.wecaht_number,left(a.new_sprint_time,10)
    '''.format(st,et)
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


def get_member_develop():
    sql = '''
    SELECT
        a.dept_name,
        a.nick_name,
        a.wechat_number,
        left(a.first_time,10) date2,
        min(a.time_diff) day,
        count(DISTINCT a.member_id) members
    FROM
        hhx_t_orders_tmp a
    where a.first_time>='{}'
    and a.first_time<'{}'
    GROUP BY a.dept_name,a.nick_name,a.wechat_number,left(a.first_time,10)
    '''.format(st,et)
    df = hhx_sql2.get_DataFrame_PD(sql)
    return df


# 当天	1-3天	4-7天	8-30天	31-90天	91-180天	181-360天	1年以上
def get_develop(x):
    if x == 0:
        return '当日'
    elif 0 < x <= 3:
        return '1-3天'
    elif 3 < x <= 7:
        return '4-7天'
    elif 7 < x <= 30:
        return '8-30天'
    elif 30 < x <= 90:
        return '31-90天'
    elif 90 < x <= 180:
        return '91-180天'
    elif 180 < x <= 360:
        return '181-360天'
    else:
        return '1年以上'


def save_sql(df):
    sql = '''
     INSERT INTO `t_fans_develop_day` 
     (`id`,`dept_name1`,`dept_name2`,`dept_name`,`nick_name`,
     `wechat_id`,`wechat_number`,`date2`,`jinfen`,`0_days`,
     `1_3_days`,`4_7_days`,`8_30_days`,`31_90_days`,`91_180_days`,
     `181_360_days`,`361_days`,`total`,`develop_rate`) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name1`= VALUES(`dept_name1`),`dept_name2`= VALUES(`dept_name2`),`dept_name`= VALUES(`dept_name`),
         `nick_name`=VALUES(`nick_name`),`wechat_id`=VALUES(`wechat_id`),`wechat_number`=values(`wechat_number`),
         `date2`=values(`date2`),`jinfen`=values(`jinfen`),`0_days`=values(`0_days`),
         `1_3_days`=values(`1_3_days`),`4_7_days`=values(`4_7_days`),`8_30_days`=values(`8_30_days`),
         `31_90_days`=values(`31_90_days`),`91_180_days`=values(`91_180_days`),`181_360_days`=values(`181_360_days`),
         `361_days`=values(`361_days`),`total`=values(`total`),`develop_rate`=values(`develop_rate`)
     '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 设备进粉数
    df_credit = get_member_credit()
    # 产出
    df_develop = get_member_develop()
    df_develop['day2'] = df_develop.apply(lambda x: get_develop(x['day']), axis=1)
    # 分组
    df_develop = df_develop.groupby(["dept_name", "nick_name", "wechat_number", "date2", "day2"])[
        'members'].sum().reset_index()
    df_develop = df_develop.set_index(["dept_name", "nick_name", "wechat_number", "date2", "day2"])[
        "members"]
    df_develop = df_develop.unstack().reset_index()
    df_develop = df_develop.fillna(0)
    df_hhx_develop = df_credit.merge(df_develop, on=["dept_name", "nick_name", "wechat_number", "date2"],
                                     how='left')
    df_hhx_develop['91-180天'] = 0
    df_hhx_develop['181-360天'] = 0
    df_hhx_develop['total'] = df_hhx_develop['当日'] + df_hhx_develop['1-3天'] + df_hhx_develop['4-7天'] + \
                              df_hhx_develop['8-30天'] + df_hhx_develop['31-90天'] + df_hhx_develop['91-180天'] + \
                              df_hhx_develop['181-360天'] + df_hhx_develop['1年以上']
    df_hhx_develop = df_hhx_develop.fillna(0)
    df_hhx_develop['develop_rate'] = (df_hhx_develop['total'] / df_hhx_develop['jinfen']).apply(lambda x: format(x,'.2%'))
    # 筛选开发率>100%的数据
    # df_hhx_develop['develop_rate']=df_hhx_develop[df_hhx_develop['develop_rate']]
    # df_hhx_develop['develop_rate'][df_hhx_develop[df_hhx_develop['total']>df_hhx_develop['jinfen']] > 1] = '100%'
    # 部门
    df_hhx_user = get_hhx_user()
    df_hhx_develop = df_hhx_develop.merge(df_hhx_user, on=['dept_name'], how='left')
    df_hhx_develop = df_hhx_develop.fillna(0)
    df_hhx_develop['id']=df_hhx_develop['wechat_id'].astype(str)+df_hhx_develop['date2'].astype(str)
    df_hhx_develop = df_hhx_develop[
        ["id","dept_name1", 'dept_name2', "dept_name", "nick_name", "wechat_id", "wechat_number", "date2", "jinfen",
         "当日","1-3天", "4-7天", "8-30天", "31-90天", "91-180天", "181-360天", "1年以上", "total", "develop_rate"]]
    df_hhx_develop=df_hhx_develop
    print(df_hhx_develop)
    save_sql(df_hhx_develop)


if __name__ == '__main__':
    hhx_sql = jnmtMySQL.QunaMysql('crm_tm_jnmt')
    hhx_sql2 = jnmtMySQL.QunaMysql('hhx_dx')
    # 时间转化
    st = '2023-01-01'
    et = '2023-04-01'
    st1 = datetime.datetime.strptime(st, "%Y-%m-%d")
    et1 = datetime.datetime.strptime(et, "%Y-%m-%d")
    main()
