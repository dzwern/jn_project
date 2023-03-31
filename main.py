# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

#
# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
#
#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     print_hi('PyCharm')
#
# # See PyCharm help at https://www.jetbrains.com/help/pycharm/


# from modules import jnmtMySQL2
#
#
# # 客户数据源
# def get_member():
#     sql = '''
#     SELECT
#         a.member_id,
#         count( 1 ) cishu,
#         sum(a.order_amount) orders
#     FROM
#         jnmt_t_orders a
#     LEFT JOIN jnmt_sys_user b on a.sys_user_id=b.user_id
#     LEFT JOIN jnmt_sys_dept c on b.dept_id=c.dept_id
#     WHERE a.tenant_id=11
#     GROUP BY a.member_id
#     '''
#     df = jnmt_sql.get_DataFrame_PD(sql)
#     return df
#
#
# def main():
#     df_member=get_member()
#     print(df_member)
#
#
# if __name__ == '__main__':
#     jnmt_sql = jnmtMySQL2.QunaMysql('jnmt_sql')
#     main()


# import pandas as pd
#
#
# df = pd.DataFrame({"id":[1001,1002,1003,1004,1005,1006],
#  "date":pd.date_range('20130102', periods=6),
#   "city":['Beijing ', 'SH', ' guangzhou ', 'Shenzhen', 'shanghai', 'BEIJING '],
#  "age":[23,44,54,32,34,32],
#  "category":['100-A','100-B','110-A','110-C','210-A','130-F'],
#   "price":[1200,1,2133,5433,2,4432]},
#   columns =['id','date','city','category','age','price'])
#
# print(df)


import logging
import sys
import traceback
import decimal
import pandas as pd
from sqlalchemy import create_engine
from itertools import cycle
from urllib.parse import quote_plus as urlquote

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


def save_sql(df):
    sql = '''
     INSERT INTO `sys_tenant` 
     (`name`,`status`,`create_by`,`create_time`,`update_by`,`update_time`,`remark`,`version`,`order_num`) 
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
     ON DUPLICATE KEY UPDATE
         `name`= VALUES(`name`),`status`= VALUES(`status`),`create_by`=VALUES(`create_by`),
         `create_time`=values(`create_time`),`update_by`=values(`update_by`),`update_time`=values(`update_time`),
         `remark`=values(`remark`),`version`=values(`version`),`order_num`=values(`order_num`)
     '''
    data = df.values.tolist()
    executeSqlManyByConn(sql, data)


def main():
    df_sql = get_sql()
    df_sql = df_sql.iloc[:, 1:]
    print(df_sql)
    save_sql(df_sql)
    print('储存完成')


if __name__ == '__main__':
    main()
