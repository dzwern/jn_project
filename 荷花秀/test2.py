# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/18 14:24
# @Author  : diaozhiwei
# @FileName: test2.py
# @description:
"""

from modules.mysql import jnmtMySQL2


def get_member():
    sql='''
    SELECT
        t1.receiver_phone,
        left(t1.create_time,10),
        row_number() over(PARTITION by t1.receiver_phone ORDER BY left(t1.create_time,10) desc) rank2,
        sum(t1.order_amount)
    FROM
    (
    (
    SELECT
        * 
    FROM
        jnmt_t_orders3 a 
    WHERE
        a.order_amount> 0 
    )
    UNION
    (
    SELECT
        * 
    FROM
        jnmt_t_orders4 b 
    WHERE
        b.order_amount >0
    )
    )t1
    where t1.create_time>='2018-01-01'
    GROUP BY t1.receiver_phone,left(t1.create_time,10)
    '''
    df=jnmt_sql.get_DataFrame_PD(sql)
    return df


def main():
    df_member=get_member()
    df_member.to_excel('./客户数7.xlsx',index=False)


if __name__ == '__main__':
    jnmt_sql = jnmtMySQL2.QunaMysql('jnmt_sql')
    main()








