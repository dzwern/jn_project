#!/usr/bin/env python
# coding=utf-8
"""
# @Time    : 2023/3/31 14:06
# @Author  : diaozhiwei
# @FileName: hhx_wechat_middle.py
# @description: 工具箱
# @update:
"""
import datetime
from functools import reduce, wraps
import json
import time
import pandas as pd


# 查看运行时间
def print_execute_time(func):
    '''定义一个计算执行时间的函数作装饰器，传入参数为装饰的函数或方法'''
    # 定义嵌套函数，用来打印出装饰的函数的执行时间
    @wraps(func)
    def call_func(*args, **kwargs):
        # 定义开始时间和结束时间，将func夹在中间执行，取得其返回值
        start = time.time()
        func_return = func(*args, **kwargs)
        cost = time.time() - start
        cost = round(cost, 3)
        # 打印方法名称和其执行时间
        print('方法{}的运行时间是：{}s'.format(func.__name__, cost))
        # 返回func的返回值
        return func_return
    # 返回嵌套的函数
    return call_func


# 时间转化字符串
def date2str(parameter, format='%Y-%m-%d'):
    '''
    将日期格式转为字符串， 默认为YYYT-MM-DD
    :param parameter:
    :param format:
    :return:
    '''
    if isinstance(parameter, str):
        return parameter
    return parameter.strftime(format)


# 字符串转化为时间
def str2date(parameter, format='%Y-%m-%d'):
    '''
    将日期格式转为字符串， 默认为YYYT-MM-DD
    :param parameter:
    :param format:
    :return:
    '''
    return datetime.datetime.strptime(parameter, format)


# 时间转换为数字
def date2int(parameter):
    '''将日期转为数字格式， 返回字符串格式'''
    if not isinstance(parameter, str):
        parameter = date2str(parameter)
    return ''.join(parameter.split('-'))


# 时间转换为时间戳
def date2timestamp(parameter):
    '''
    将日期格式转换为时间戳
    '''
    return int(time.mktime(parameter.timetuple())) * 1000


# 列表转换为字符串
def list2str(item_list):
    '''将list转为字符串格式'''
    item_list = [str(x) for x in item_list]
    quoted_list_str = "'" + "','".join(item_list) + "'"
    return quoted_list_str


# 读取文件，返回json格式
def read_file_return_json(path):
    '''读取文件，返回json格式'''
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data


# 数字转换为百分比
def float2percentage(data, columns):
    """将数字转成百分比"""
    # 如果是DataFrame， 将整列进行转换
    if isinstance(data, pd.DataFrame):
        for column in columns:
            data[column] = data[column].apply(lambda x: '%.2f%%' % (x * 100))
        return data
    # 将数字转为百分数
    if isinstance(data, float):
        return '%.2f%%' % (data * 100)


# 列表转换为数据库in
def quoted_list_func(item_list):
    '''当SQL语句中用 IN 的时候，把python list转为 IN 后面的内容'''
    item_list = [str(x) for x in item_list]
    quoted_list_str = "'" + "','".join(item_list) + "'"
    return quoted_list_str
