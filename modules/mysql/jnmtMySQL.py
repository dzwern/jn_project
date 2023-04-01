# -*-coding: utf-8-*-
"""
# @Time    : 2023/3/31 14:06
# @Author  : diaozhiwei
# @FileName: hhx_wechat_middle.py
# @description: 数据库连接
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
九牛账号密码：mysql数据库地址: rm-2ze366az6q84dcs4fyo.mysql.rds.aliyuncs.com 端口: 3306 只读账号 imr_bi  密码：dfpllj@#@0
('mysql+pymysql://数据库用户名:数据库密码@数据库地址/数据库名')
'''

# rm-2ze366az6q84dcs4fyo.mysql.rds.aliyuncs.com
# URL = 'mysql+pymysql://zsdatastat:ZS_quna1@192.168.1.219:3306/'   # zs_mongo
# URL = 'mysql+pymysql://root:021412@localhost:3306/'    # zs_test
# URL = 'mysql+pymysql://imr_bi:dfpllj@#@0@rm-2ze366az6q84dcs4fyo.mysql.rds.aliyuncs.com:3306/'
# userName = 'imr_bi'
# password = 'dfpllj@#@0'
# dbHost = 'rm-2ze366az6q84dcs4fyo.mysql.rds.aliyuncs.com'
# dbPort = 3306
# URL = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/'
userName = 'dzw'
password = 'dsf#4oHGd'
dbHost = 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com'
dbPort = 3306
URL = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/'


class QunaMysql(object):
    # 初始化
    def __init__(self, schema='crm_tm_jnmt'):
        self._engin = create_engine(URL + schema + '?charset=utf8',
                                    pool_pre_ping=True,
                                    pool_recycle=3600 * 4)
        self.schema = schema

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
    def executeSqlByEngine(self, sql='SELECT * FROM DUAL'):
        return self._engin.execute(sql)

    # 执行sql
    def executeSqlByConn(self, sql='SELECT * FROM DUAL', conn=None):
        conn = conn or self.getConnection()
        with conn as connection:
            return connection.execute(sql)

    def executeSqlWithParamByConn(self, sql='SELECT * FROM DUAL', data=None, conn=None):
        if data is None:
            return
        conn = conn or self.getConnection()
        with conn as connection:
            return connection.execute(sql, data)

    # 批量执行更新sql语句
    def executeSqlManyByConn(self, sql='', data=[], conn=None):
        if len(data) > 0:
            conn = conn or self.getConnection()
            with conn as connection:
                return connection.execute(sql, data)

    # 加载数据到df(自定义索引) 不推荐
    def load_DataFrame_Conn(self, sql='SELECT * FROM DUAL', conn=None):
        conn = conn or self.getConnection()
        with conn as connection:
            dataList = list(connection.execute(sql))
            dataFrame = pd.DataFrame(dataList, index=[(n + 1) for n in range(len(dataList))])
            return dataFrame

    # 加载数据到df 不推荐
    def get_DataFrame_Conn(self, sql='SELECT * FROM DUAL', conn=None):
        conn = conn or self.getConnection()
        with conn as connection:
            dataList = list(connection.execute(sql))
            dataFrame = pd.DataFrame(dataList)
            return dataFrame

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

    def get_dict(self, sql, key_names=None):
        '''返回结果为 dictionary in dictionary
        {"John":{"address":"3103 13th South Street","phone":"258-563-3654"}}

        sql 查询的第一个字段作为 最外层字典的 key
        其余字段另成为内层字典的 value
        :param key_names 指定内层字典 dict 的key，如未指定，命名为col_0，col_1...
        '''
        result = self._engin.execute(sql).fetchall()
        result_dict = {}

        for record in result:
            dict_key, *fields = record

            # 如果没有提供其余字段的键名
            # 则自动生成一个
            if key_names is None:
                key_names = []
                for num in range(len(fields)):
                    key_names.append("col_{}".format(num))

            elif len(key_names) != len(fields):
                raise ValueError("{} key names are privided,{} is required".format(len(key_names), len(fields)))
            # sqlalchemy 返回的小数 type 是decimal.Decimal
            # 需要转换
            for index, record in enumerate(fields):
                if isinstance(record, decimal.Decimal):
                    fields[index] = float(record)

            key_iter = cycle(key_names)
            data_dict = {}
            for each_field in fields:
                data_dict[next(key_iter)] = each_field

            result_dict[dict_key] = data_dict

        return result_dict


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        stream=sys.stdout,
                        filemode='a+')
    try:
        qunaMysql = QunaMysql()
    except:
        ex = traceback.format_exc()
        logging.error(ex)
    finally:
        pass
