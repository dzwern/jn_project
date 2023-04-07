# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 9:18
# @Author  : diaozhiwei
# @FileName: jnmtMySQL4.py
# @description: 
# @update:
"""

import logging
import sys
import traceback
import decimal
import pandas as pd
from sqlalchemy import create_engine
from itertools import cycle
from urllib.parse import quote_plus as urlquote

'''
九牛账号密码：mysql数据库地址: rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com 端口: 3306 只读账号 dzw  密码：dsf#4oHGd
('mysql+pymysql://数据库用户名:数据库密码@数据库地址/数据库名')
'''


class QunaMysql(object):
    # 初始化
    def __init__(self, schema='', userName='', password='', dbHost='',dbPort=3306):
        self._engin = create_engine(
            f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/' + schema + '?charset=utf8',
            pool_pre_ping=True,
            pool_recycle=3600 * 4)
        self.schema = schema
        self.userName = userName
        self.password = password
        self.dbHost = dbHost
        self.dbPort = 3306

    # 连接池状态
    def getPoolStatus(self):
        return self._engin.pool.status()

    # 获取连接
    def getConnection(self):
        conn = self._engin.connect()
        return conn

    # 释放连接
    def closeConnection(self, conn):
        if conn:
            conn.close()

    # 执行sql
    def executeSqlByConn(self, sql='SELECT * FROM DUAL', conn=None):
        conn = conn or self.getConnection()
        with conn as connection:
            return connection.execute(sql)

    # 批量执行更新sql语句
    def executeSqlManyByConn(self, sql='', data=[], conn=None):
        if len(data) > 0:
            conn = conn or self.getConnection()
            with conn as connection:
                return connection.execute(sql, data)

    # 加载数据到df
    def get_DataFrame_PD(self, sql='SELECT * FROM DUAL', conn=None):
        conn = conn or self.getConnection()
        with conn as connection:
            dataFrame = pd.read_sql(sql, connection)
            return dataFrame

    # 保存df到数据库
    def save_DataFrame_PD(self, pd, table, conn=None):
        conn = conn or self.getConnection()
        with conn as connection:
            pd.to_sql(table, connection, if_exists='append', index=False)

    def get_list(self, sql='SELECT * FROM DUAL'):
        '''返回结果为list'''
        result = self._engin.execute(sql)
        result_list = []
        for i in result:
            result_list.append(i[0])
        return result_list


if __name__ == '__main__':
    try:
        qunaMysql = QunaMysql()
    except:
        ex = traceback.format_exc()
        logging.error(ex)
    finally:
        pass