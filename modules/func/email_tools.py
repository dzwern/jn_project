#!/usr/bin/env python
# coding=utf-8
"""
    Copyright (C) 2019 * Ltd. All rights reserved.
 
    Editor      : PyCharm
    File name   : email_tools.py
    Author      : Charles
    Created date: 2021/2/22 10:21 上午
    Description :
       邮件再封装脚本，包含各种对邮件的复用,
       /home/datapy/LOGS/Smart_Retail_System/modules/func/邮箱消息管理设置.xlsx
"""
import os
import sys

import pandas as pd
from itertools import chain

from modules.email.ZsMail import ZsMail
from modules.func import utils, path
from modules.func.utils import save_dataframe_to_excel
from modules.logging.QunaLog import QunaLog


def get_receiver_list(mode='user', **kwargs):
    """
    # 邮箱接收人员的获取方法（mode={user,city,project}, *args, **kwargs）接收字典形式传参，不固定
    返回收件人和抄送人列表

    支持三种模式， 人员-user，城市-city，项目-project

    1、人员-user 模式
    当选择员工模式后， 收件人和抄送都需要输入员工的姓名即可， 以集合的形式进行保存，
    然后会自动读取该员工对应的邮箱， 若员工未填写邮箱或未匹配到邮箱，会弹出邮箱错误的提示。

    2、城市-city 模式
    当选择城市模式后，需要指定参数：项目字段，然后根据指定参数读取对应的收件人。
    若项目字段不存在，会弹出错误，若收件人为空，则忽略该城市。
    返回城市和对应的收件人。


    3、项目-project 模式
    当选择项目模式后，需要指定参数：项目组，然后根据指定参数去获取数据库信息，
    若获取信息为空，弹出信息为空的错误提示。
    当获取到项目信息后，判断是否存在多个项目组，检查每个项目组是否获取到对应数据，若未获取到数据，弹出错误。
    然后根据项目组匹配对应负责区域，若存在全国则单独创建一个全部区域变量，其他按照对应城市，获取对应负责人及成员，
    拼接成收件人和抄送人，直接发送邮件。
    """

    if mode == 'user':
        # user模式， 接收参数receiver和cc
        receiver = list(kwargs.get('receiver', []))
        cc = list(kwargs.get('cc', []))

        if len(receiver) == 0:
            raise ValueError('未指定收件人列表！')

        # 根据姓名找到对应的收件人列表
        user_info = pd.read_excel('/Users/chracles/OneDrive/逻辑梳理脑图/智慧零售/智慧物流/邮箱消息管理设置.xlsx', sheet_name='员工信息表')
        receiver_email_list = user_info.loc[user_info['员工姓名'].isin(receiver), '邮箱'].unique().tolist()

        # 如果获取列表为空，或者数量不匹配，提示错误信息
        if len(receiver_email_list) == 0 or len(receiver_email_list) != len(receiver):
            raise ValueError('员工未填写邮箱或未匹配到邮箱!')

        if len(cc) != 0:
            cc_email_list = user_info.loc[user_info['员工姓名'].isin(cc), '邮箱'].unique().tolist()
            if len(cc_email_list) == 0 or len(cc_email_list) != len(cc):
                raise ValueError('员工未填写邮箱或未匹配到邮箱!')
        else:
            cc_email_list = None

        return receiver_email_list, cc_email_list

    elif mode == 'city':
        # city模式
        project_filed = list(kwargs.get('project_filed', []))

        if len(project_filed) == 0:
            raise ValueError('指定项目字段为空!')

        # 根据传入城市，找到对应负责人
        city_info = pd.read_excel('/Users/chracles/OneDrive/逻辑梳理脑图/智慧零售/智慧物流/邮箱消息管理设置.xlsx', sheet_name='城市负责人表')
        try:
            city_info = city_info[['城市名称', project_filed[0]]]
        except:
            raise ValueError('未获取到指定项目字段对应的数据!')

        # 如果没有收件人，则删除该城市数据
        city_info = city_info.loc[~city_info[project_filed[0]].isin([None, '无'])]
        if len(city_info) == 0:
            raise ValueError('指定项目字段对应的数据未空!')
        city_info[project_filed[0]] = city_info[project_filed[0]].apply(lambda x: x.strip().split('，'))

        user_info = pd.read_excel('/Users/chracles/OneDrive/逻辑梳理脑图/智慧零售/智慧物流/邮箱消息管理设置.xlsx', sheet_name='员工信息表')
        user_email_dict = user_info[['员工姓名', '邮箱']].set_index('员工姓名')['邮箱'].to_dict()

        # 获取城市对应的收件人
        area_email_map = city_info.groupby('城市名称').apply(lambda x: list(chain(*x[project_filed[0]]))).to_dict()

        for area in area_email_map:
            try:
                area_email_map[area] = [user_email_dict[name] for name in set(area_email_map[area])]
            except:
                raise ValueError('项目组：{}下的成员邮箱存在错误或未获取到！'.format(area))

        return area_email_map

    elif mode == 'project':
        # project模式
        project_team = list(kwargs.get('project_team', []))

        if len(project_team) == 0:
            raise ValueError('指定项目不存在！')

        project_info = pd.read_excel('/Users/chracles/OneDrive/逻辑梳理脑图/智慧零售/智慧物流/邮箱消息管理设置.xlsx', sheet_name='项目成员管理')
        user_info = pd.read_excel('/Users/chracles/OneDrive/逻辑梳理脑图/智慧零售/智慧物流/邮箱消息管理设置.xlsx', sheet_name='员工信息表')
        user_email_dict = user_info[['员工姓名', '邮箱']].set_index('员工姓名')['邮箱'].to_dict()
        project_info['管理城市'] = project_info['管理城市'].apply(lambda x: x.strip().split('，'))
        project_info['负责人'] = project_info['负责人'].apply(lambda x: x.strip().split('，'))
        project_info['成员'] = project_info['成员'].apply(lambda x: x.strip().split('，'))
        team_info = project_info.loc[project_info['项目组'].isin(project_team)]

        # 将项目组进行拆分，分为管理城市和邮箱
        area_map = team_info.groupby('负责区域').apply(lambda x: list(chain(*x['管理城市']))).to_dict()

        email_map = team_info.groupby('负责区域', as_index=False).agg(
            {'负责人': lambda x: list(chain(*x)), '成员': lambda x: list(chain(*x))})
        email_map = email_map.groupby('负责区域').apply(
            lambda x: {'cc': list(chain(*x['负责人'])), 'receiver': list(chain(*x['成员']))}).to_dict()

        # 将项目成员名称转为邮箱，如果发生错误，则进行提示
        for area in email_map:
            try:
                email_map[area]['cc'] = [user_email_dict[name] for name in set(email_map[area]['cc'])]
                email_map[area]['receiver'] = [user_email_dict[name] for name in set(email_map[area]['receiver'])]
            except:
                raise ValueError('项目组：{}下的成员邮箱存在错误或未获取到！'.format(area))

        return area_map, email_map

    else:
        raise ValueError('指定mode模式错误，请检查后重新输入！')


def send_data_by_email(startTime, msg, subject, mode='user', file_type='xlsx', delete=True, **kwargs):
    """发送邮件
    startTime: 当前时间，必须字段
    msg: 要发送的信息， 必须字段
    subject: 邮件标题， 必须字段
    file_type: 保存文件格式，默认保存Excel。
    delete: 是否删除本地附件。默认删除。
    kwargs：参数字典。
    mode: 分为user, city, project三种模式。
        user模式： 支持带附件和无附件模式，指定收件人姓名和抄送人姓名，需要放在可迭代容器中，发送全量数据。
            # 有数据， user模式
            kwargs = {'datas': [data],
                      'root': logDir,
                      'file_name': '智能补货系统正式数据',
                      'receiver': {'张亚辉'},
                      'cc': {'张亚辉'}}

            # 无数据, 只支持user模式
            kwargs = {'receiver': {'张亚辉'},
                        'cc': ['张亚辉']}

        city模式： 仅支持附件模式，指定项目字段名称和区域划分字段，获取对应收件人，将数据按照城市进行发送。
            # 有数据， city模式
            kwargs = {'datas': [data],
                      'root': logDir,
                      'file_name': '智能补货系统正式数据',
                      'project_filed': {'数据开发'},
                      'area': '地区'}

        project模式： 仅支持附件模式， 指定项目组和区域划分字段，返回对应的区域划分字典和区域邮箱
            # 有数据，project模式
            kwargs = {'datas': [data],
                      'root': logDir,
                      'file_name': '智能补货系统正式数据',
                      'project_team': {'数据开发', '物流盘点', '运营', '物流配货'},
                      'area': '地区'}
    """
    zsMail = ZsMail()

    # 获取数据，判断是否存在数据
    datas = kwargs.get('datas', None)

    if datas is not None:
        # 存在数据则获取存储数据相关的参数
        root = kwargs.get('root', None)  # 根目录
        if root is None or not os.path.exists(root):
            raise ValueError('根目录不存在！')

        file_name = kwargs.get('file_name', None)  # 文件名
        if root is None or len(root.strip()) == 0:
            raise ValueError('文件名不存在！')

        # 如果存在数据，则判断mode模式
        if mode == 'user':
            # 保存数据
            save_path = '{root}/{startTime}_{file_name}.{file_type}'.format(root=root.rstrip('/'), startTime=startTime, file_name=file_name, file_type=file_type)
            sheets_name = kwargs.get('sheets_name', None)
            save_dataframe_to_excel(save_path, datas=datas, sheets_name=sheets_name)

            # 获取user模式对应的receiver,cc, 并发送邮件
            receiver, cc = get_receiver_list(mode, **kwargs)
            subject = '{startTime}_{subject}'.format(startTime=startTime, subject=subject)
            msg = '时间: {startTime}<br /><br />消息: {msg}<br />'.format(startTime=startTime, msg=msg)
            zsMail.sendMail(mail_msg=msg, subject=subject, sender='reporting@mx2.zzss.com', receiver=receiver, cc=cc,
                            attachFile=save_path)

            # 判断是否删除附件
            if delete and os.path.exists(save_path):
                os.remove(save_path)

        elif mode == 'city':
            # 获取划分的地区字典和邮件字典
            area_email_map = get_receiver_list(mode, **kwargs)
            key = kwargs.get('area', '城市')  # 划分地区的字段

            for area in area_email_map:
                # 分区域保存数据, 对于全国则给出全部数据
                if area == '全国':
                    area_data = datas
                else:
                    area_data = [data.loc[data[key].isin([area])] for data in datas]

                # 判断如果区域数据不存在，则不需要保存和发送邮件
                if any([len(data) != 0 for data in area_data]):
                    save_path = '{root}/{area}_{startTime}_{file_name}.{file_type}'.format(root=root.rstrip('/'), area=area, startTime=startTime, file_name=file_name, file_type=file_type)
                    sheets_name = kwargs.get('sheets_name', None)
                    save_dataframe_to_excel(save_path, datas=area_data, sheets_name=sheets_name)

                    # 发送邮件
                    area_subject = '{area}_{startTime}_{subject}'.format(area=area, startTime=startTime, subject=subject)
                    area_msg = '区域: {area}<br />时间: {startTime}<br /><br />消息: {msg}<br />'.format(area=area, startTime=startTime, msg=msg)
                    zsMail.sendMail(mail_msg=area_msg, subject=area_subject, sender='reporting@mx2.zzss.com',
                                    receiver=area_email_map[area], attachFile=save_path)

                # 判断是否删除附件
                if delete and os.path.exists(save_path):
                    os.remove(save_path)
        else:
            # 获取划分的地区字典和邮件字典
            area_map, email_map = get_receiver_list(mode, **kwargs)
            key = kwargs.get('area', '城市')  # 划分地区的字段

            for area in area_map:
                # 分区域保存数据, 对于全国则给出全部数据
                if area == '全国':
                    area_data = datas
                else:
                    area_data = [data.loc[data[key].isin(area_map[area])] for data in datas]

                # 判断如果区域数据不存在，则不需要保存和发送邮件
                if any([len(data) != 0 for data in area_data]):
                    save_path = '{root}/{area}_{startTime}_{file_name}.{file_type}'.format(root=root.rstrip('/'), area=area, startTime=startTime, file_name=file_name, file_type=file_type)
                    sheets_name = kwargs.get('sheets_name', None)
                    save_dataframe_to_excel(save_path, datas=area_data, sheets_name=sheets_name)

                    # 发送邮件
                    area_subject = '{area}_{startTime}_{subject}'.format(area=area, startTime=startTime, subject=subject)
                    area_msg = '区域: {area}<br />时间: {startTime}<br /><br />消息: {msg}<br />'.format(area=area, startTime=startTime, msg=msg)
                    zsMail.sendMail(mail_msg=area_msg, subject=area_subject, sender='reporting@mx2.zzss.com',
                                    receiver=email_map[area]['receiver'],
                                    cc=email_map[area]['cc'], attachFile=save_path)

            # 判断是否删除附件
            if delete and os.path.exists(save_path):
                os.remove(save_path)
    else:
        if mode == 'user':
            # 获取user模式对应的receiver,cc, 并发送邮件
            receiver, cc = get_receiver_list(mode, **kwargs)
            subject = '{startTime}_{subject}'.format(startTime=startTime, subject=subject)
            msg = '时间: {startTime}<br /><br />消息: {msg}<br />'.format(startTime=startTime, msg=msg)
            zsMail.sendMail(mail_msg=msg, subject=subject, sender='reporting@mx2.zzss.com', receiver=receiver, cc=cc)

        else:
            raise ValueError('当不存在数据时，仅支持user模式！')


if __name__ == '__main__':
    startTime = utils.get_time_args(sys.argv)
    logPath, logDir = path.make_logPath(__file__)
    logging = QunaLog(name=os.path.basename(__file__), fileName=logPath)
    logging.info('program start {}'.format(startTime))

    data = pd.read_excel('/Users/chracles/OneDrive/逻辑梳理脑图/智慧零售/智慧物流/2021-02-22_全国智能补货仓库汇总数据.xlsx')

    # 有数据，project模式
    kwargs = {'datas': [data],
              'root': logDir,
              'file_name': '智能补货系统正式数据',
              'project_team': {'数据开发', '物流盘点', '运营', '物流配货'},
              'area': '地区'}

    # 有数据， user模式
    # kwargs = {'datas': [data],
    #           'root': logDir,
    #           'file_name': '智能补货系统正式数据',
    #           'receiver': {'张亚辉'},
    #           'cc': {'张亚辉'}}

    # 有数据， city模式
    # kwargs = {'datas': [data],
    #           'root': logDir,
    #           'file_name': '智能补货系统正式数据',
    #           'project_filed': {'数据开发'},
    #           'area': '地区'}

    # 无数据, 只支持user模式
    # kwargs = {'receiver': {'张亚辉'},
    #           'cc': ['张亚辉']}

    msg = 'test'
    subject = '智能补货系统正式数据'
    send_data_by_email(utils.date2str(startTime), msg, subject, mode='user', **kwargs)
