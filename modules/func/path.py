#!/usr/bin/env python
# coding=utf-8
"""
    Copyright (C) 2019 * Ltd. All rights reserved.
 
    Editor      : PyCharm
    File name   : path.py
    Author      : Charles
    Created date: 2020/11/5 10:28 上午
    Description :
       路径相关的方法
"""

import os


def get_root_dir(file, project_name='zs_project'):
    '''获取项目的根目录'''
    curPath = os.path.abspath(os.path.dirname(file))
    root_dir = curPath[:curPath.find(project_name)]
    root_path = curPath[curPath.find(project_name):]
    return root_dir, root_path


def return_log_path(filePath, project_name='zs_project'):
    '''返回当前文件日志对应的文件夹路径'''
    root_dir, root_path = get_root_dir(filePath, project_name)
    logPath = os.path.join(root_dir, 'LOGS/', root_path)
    os.makedirs(logPath, exist_ok=True)

    return logPath


def make_logPath(file, fileType='.log', project_name='zs_project'):
    '''传入执行文件的绝对路径和文件名，返回拼接后的日志路径'''

    filePath = os.path.abspath(file)
    fileName = os.path.basename(file)

    logDir = return_log_path(filePath, project_name)  # 获取返回的log日志路径
    check_path(logDir)  # 检查文件夹是否存在，如果不存在就创建

    logPath = os.path.join(logDir, fileName.split('.')[0] + fileType)

    return logPath, logDir


def check_path(model_path):
    '''检测目录是否存在，如果不存在就创建'''
    # 如果是文件，就截取文件夹目录，然后判断是否存在，不存在就创建
    if not os.path.isdir(model_path):
        logFloder = os.path.dirname(model_path)
        if not os.path.exists(logFloder):
            try:
                os.makedirs(logFloder)
                print('Path created successfully：{}'.format(model_path))
            except:
                raise ValueError('创建指定目录失败，请检查！')
    else:
        # 不是文件，判断文件夹是否存在
        if not os.path.exists(model_path):
            try:
                os.makedirs(model_path, exist_ok=True)
                print('Path created successfully：{}'.format(model_path))
            except:
                raise ValueError('创建指定目录失败，请检查！')
