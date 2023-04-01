# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/17 16:35
# @Author  : diaozhiwei
# @FileName: hhx_member_develop_day.py
# @description: 进粉开发数
"""

from modules.mysql import jnmtMySQL2, jnmtMySQL, jnmtMySQL3


def get_member_credit():
    sql = '''
    SELECT
        f.dept_id,
        f.dept_name,
        left(a.promotion_date,10) date2,
        e.nick_name,
        d.wecaht_number,
        sum(c.credit) jinfen
    FROM
        jnmt_t_promotion a
    LEFT JOIN jnmt_t_promotion_allocation b on a.promotion_id=b.promotion_id
    LEFT JOIN jnmt_t_wechat_fans_log c on b.we_chat_id=c.wechat_id and a.promotion_date=c.new_sprint_time
    LEFT JOIN jnmt_t_wechat d on d.id=b.we_chat_id
    LEFT JOIN jnmt_sys_user e on e.user_id=d.sys_user_id
    LEFT JOIN jnmt_sys_dept f on e.dept_id=f.dept_id
    where a.tenant_id=11
    and a.promotion_date>='2023-01-01'
    and a.promotion_date<'2023-03-20'
    and b.deleted=0
    GROUP BY f.dept_id,f.dept_name,e.nick_name,d.wecaht_number,left(a.promotion_date,10)
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


def get_member_develop():
    sql = '''
        select
        t.dept_id,
        t.dept_name,
        t.nick_name,
        t.wecaht_number,
        left(t.first_time2,10) date2,
        DATEDIFF(left(t.create_time,10),left(t.first_time2,10)) day2,
        count(DISTINCT t.member_id) members
    FROM
    (
    SELECT
        d.dept_id,
        d.dept_name,
        c.nick_name,
        e.wecaht_number,
        a.member_id,
        MAX(IFNULL(b.incoming_line_time, b.add_wechat_time)) first_time2,
        min(a.create_time) create_time,
        a.order_amount 
    FROM
        jnmt_t_orders a
    LEFT JOIN  jnmt_t_member b on a.member_id=b.id
    LEFT JOIN jnmt_sys_user c on a.sys_user_id=c.user_id
    LEFT JOIN jnmt_sys_dept d on c.dept_id=d.dept_id
    LEFT JOIN jnmt_t_wechat e on a.wechat_id=e.id
    WHERE
        a.tenant_id = 11
    -- 订单状态
    and a.order_state NOT IN (6,8,10,11)
    and a.refund_state not in (4)
    GROUP BY a.order_sn
    )t
    WHERE t.first_time2>='2023-01-01'
    and t.first_time2<'2023-03-20'
    GROUP BY t.dept_id,t.dept_name,t.nick_name,t.wecaht_number,left(t.create_time,10),DATEDIFF(left(t.create_time,10),left(t.first_time2,10))
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
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
     INSERT INTO `jnmt_member_develop` 
     (`dept_id`,`dept_name`,`nick_name`,`wecaht_number`,`date2`,`jianfen`,
     `0`,`1-3`,`4-7`,`8-30`,`31-90`,`91-180`,`181-360`,`361`,`total`) 
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
     ON DUPLICATE KEY UPDATE
         `dept_id`= VALUES(`dept_id`),`dept_name`= VALUES(`dept_name`),`nick_name`=VALUES(`nick_name`),
         `wecaht_number`=values(`wecaht_number`),`date2`=values(`date2`),`jianfen`=values(`jianfen`),
         `0`=values(`0`),`1-3`=values(`1-3`),`4-7`=values(`4-7`),
         `8-30`=values(`8-30`),`31-90`=values(`31-90`),`91-180`=values(`91-180`),
         `181-360`=values(`181-360`),`361`=values(`361`),`total`=values(`total`)
     '''
    jnmt_sql3.executeSqlManyByConn(sql, df.values.tolist())


def main():
    # 进粉数
    df_credit = get_member_credit()
    # 产出
    df_develop = get_member_develop()
    df_develop['day2'] = df_develop.apply(lambda x: get_develop(x['day2']), axis=1)
    df_credit = df_credit
    df_develop = df_develop
    # 分组
    df_develop = df_develop.groupby(["dept_id", "dept_name", "nick_name", "wecaht_number", "date2", "day2"])[
        'members'].sum().reset_index()
    df_develop = df_develop.set_index(["dept_id", "dept_name", "nick_name", "wecaht_number", "date2", "day2"])[
        "members"]
    df_develop = df_develop.unstack().reset_index()
    df_develop = df_develop.fillna(0)
    df = df_credit.merge(df_develop, on=["dept_id", "dept_name", "nick_name", "wecaht_number", "date2"], how='left')
    df['91-180天'] = 0
    df['181-360天'] = 0
    df['total'] = df['当日']+df['1-3天']+df['4-7天']+df['8-30天']+df['31-90天']+df['91-180天']+df['181-360天']+df['1年以上']
    df = df.fillna(0)
    print(df)
    df = df[
        ["dept_id", "dept_name", "nick_name", "wecaht_number", "date2", "jinfen", "当日", "1-3天", "4-7天", "8-30天",
         "31-90天", "91-180天", "181-360天", "1年以上", "total"]]
    save_sql(df)


if __name__ == '__main__':
    jnmt_sql = jnmtMySQL.QunaMysql('crm_bi')
    jnmt_sql2 = jnmtMySQL2.QunaMysql('jnmt_sql')
    jnmt_sql3 = jnmtMySQL3.QunaMysql('mydb1')
    main()



