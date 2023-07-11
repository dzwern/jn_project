# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/4/1 15:00
# @Author  : diaozhiwei
# @FileName: demo_campaign.py
# @description: 活动期间整体数据指标监控
# @update:
"""
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from dateutil.relativedelta import relativedelta


# 设备客户类型
def get_campaign():
    sql = '''
    SELECT
    *
    from 
    imr_tm_yhds.sys_user
    '''
    df = hhx_sql1.get_DataFrame_PD(sql)
    return df


# 中间表删除
def del_sql():
    sql = '''
    truncate table t_campaign;
    '''
    hhx_sql1.executeSqlByConn(sql)


def main():
    df = get_campaign()
    print(df)


if __name__ == '__main__':
    hhx_sql1 = jnMysql('', 'imr_tm_yhds', 'dfml#420', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')

    # hhx_sql2 = jnMysql('hhx_dx', 'dzw', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # hhx_sql2 = jnMysql('imr_tm_yhds', 'imr_tm_yhds', 'dfml#420', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    # 开始时间，结束时间
    # 2023年五一活动，2023年38女神节活动，2023年618活动
    main()


