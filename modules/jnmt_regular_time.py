# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/17 9:06
# @Author  : diaozhiwei
# @FileName: jnmt_regular_time.py
# @description: 脚本定时
"""

import schedule
import time


def hello_fun():
    print('+++++')


# 每秒运行一次
schedule.every().seconds.do(hello_fun)

while True:
    schedule.run_pending()









