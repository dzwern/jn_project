# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/17 16:37
# @Author  : diaozhiwei
# @FileName: demo1.py
# @description:
"""
import pandas as pd
from modules import jnmtMySQL3


def get_member():
    sql = '''
    SELECT * from orderinfo a
    '''
    df = jnmt_sql.get_DataFrame_PD(sql)
    return df


def main():
    df = get_member()
    print(df)


if __name__ == '__main__':
    jnmt_sql = jnmtMySQL3.QunaMysql('mydb1')
    main()

