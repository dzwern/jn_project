#!/usr/bin/env python
# coding=utf-8
"""
    Copyright (C) 2019 * Ltd. All rights reserved.
 
    Editor      : PyCharm
    File name   : date_tools.py
    Author      : Charles
    Created date: 2021/2/23 4:03 下午
    Description :
       时间类操作工具箱
"""
import datetime
import time


def str2date(parameter, format='%Y-%m-%d', type='datetime'):
    """
    将日期格式转为字符串， 默认为YYYT-MM-DD
    :param parameter:
    :param format:
    :param type: 返回的时间格式， 支持date和datetime
    :return:
    """
    if type == 'date':
        return datetime.datetime.strptime(parameter, format).date()
    return datetime.datetime.strptime(parameter, format)


def date2str(parameter, format='%Y-%m-%d'):
    """
    将日期格式转为字符串， 默认为YYYT-MM-DD
    :param parameter:
    :param format:
    :return:
    """
    if isinstance(parameter, str):
        return parameter
    return parameter.strftime(format)


def date2timestamp(parameter):
    """
    将日期格式转换为时间戳
    """
    return int(time.mktime(parameter.timetuple())) * 1000


def date2int(parameter, format='%Y-%m-%d'):
    """将日期转为数字格式， 返回字符串格式"""
    if not isinstance(parameter, str):
        parameter = date2str(parameter, format)
    return ''.join(parameter.split('-'))


def int2date(parameter):
    """将数字转为日期格式，返回日期格式"""
    if isinstance(parameter, str):
        return str2date('-'.join([parameter[:4], parameter[4:6], parameter[6:]]))
    elif isinstance(parameter, int):
        parameter = str(parameter)
        return str2date('-'.join([parameter[:4], parameter[4:6], parameter[6:]]))
    else:
        raise ValueError('请输入数字格式或字符串格式的数字！')

def df_add_date(data, st):
    """为df添加时间"""
    if not isinstance(st, str):
        st = date2str(st)
    data['date'] = st
    data['date_int'] = date2int(st)

    return data

if __name__ == '__main__':
    # 日期转字符
    s = date2str(datetime.datetime(2021, 2, 25))
    print(s, type(s))

    # 字符转日期
    s = str2date('2021-02-25')
    print(s, type(s))

    # 日期转时间戳
    s = date2timestamp(datetime.datetime(2021, 2, 25))
    print(s, type(s))

    # 日期转数字格式
    s = date2int(datetime.datetime(2021, 2, 25))
    print(int(s), type(s))

    q = int2date(int(s))
    print(q, type(q))
