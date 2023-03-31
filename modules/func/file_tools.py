#!/usr/bin/env python
# coding=utf-8
"""
    Copyright (C) 2019 * Ltd. All rights reserved.
 
    Editor      : PyCharm
    File name   : file_tools.py
    Author      : Charles
    Created date: 2021/2/23 2:08 下午
    Description :
       文件操作工具库
"""
import time
import datetime

import os


def TimeStampToTime(timestamp):
    """把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12"""
    timeStruct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S', timeStruct)


def get_FileSize(filePath):
    """获取文件的大小,结果保留两位小数，单位为MB"""
    fsize = os.path.getsize(filePath)
    fsize = fsize / float(1024 * 1024)
    return round(fsize, 2)


def get_FileAccessTime(filePath):
    """获取文件的访问时间"""
    t = os.path.getatime(filePath)
    return TimeStampToTime(t)


def get_FileCreateTime(filePath):
    """获取文件的获取最后一次的改变时间"""
    t = os.path.getctime(filePath)
    return TimeStampToTime(t)


def get_FileModifyTime(filePath):
    """获取文件的修改时间"""
    t = os.path.getmtime(filePath)
    return TimeStampToTime(t)


if __name__ == '__main__':
    # 测试方法是否正确
    filePath = '/Users/chracles/OneDrive/逻辑梳理脑图/智慧零售/智慧物流/2021-02-22_全国智能补货仓库汇总数据.xlsx'

    # 获取文件大小
    fileSize = get_FileSize(filePath)
    print('文件大小为：{}M'.format(fileSize))

    # 获取文件访问时间
    file_access_time = get_FileAccessTime(filePath)
    print('文件访问时间为:{file_access_time}'.format(file_access_time=file_access_time))

    # 获取文件创建时间
    file_create_time = get_FileCreateTime(filePath)
    print('文件最后一次的改变时间{file_create_time}'.format(file_create_time=file_create_time))

    # 获取文件修改时间
    file_modify_time = get_FileModifyTime(filePath)
    print('文件修改时间:{file_modify_time}'.format(file_modify_time=file_modify_time))