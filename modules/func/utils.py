#!/usr/bin/env python
# coding=utf-8
"""
    Copyright (C) 2019 * Ltd. All rights reserved.

    Editor      : PyCharm
    File name   : utils.py
    Author      : zhang yahui
    Created date: 2020/6/2 14:10
    Description :
       工具箱
       1、保存数据到MySQL数据库
       2、计算函数运行时间
"""
import datetime
import functools
import traceback
from functools import reduce, wraps
import json
import os
import time
import subprocess

import pandas as pd

from base.aisle_schedule.multiProcess_version.database.zs_mongo.SRS_zs_juezhao_salesman_data import \
    ZsJuezhaoSalesmanData
from modules.email.ZsMail import ZsMail
from modules.dingtalk.DingTalk import DingTalk
from modules.func.file_tools import get_FileSize



def save_dataframe_to_mysql(connector, data, table, columns, logging, update_columns=None, primary_key='id', type_convert=True):
    '''
    保存DataFrame 数据到MySQL数据库
    :param connector: MySQL数据库连接器
    :param data: 要保存的数据 DataFrame格式
    :param table: 要保存的表名
    :param columns: list 要保存的列名， 需要包含主键
    :param logging: 日志文件
    :param update_columns: 要保存更新的列名， 不包含主键
    :param primary_key: 表的主键
    :param version: 版本控制，如果是None,直接调用链接，否则按照字典进行调用{'mongo': mongo_mysql, 'dw':dw_mysql,
                    'test':test_mysql, 'base':base_mysql}
    :return:
    '''

    # 判断columns是否是list，如果不是list，则引发错误
    if not isinstance(columns, list):
        raise ValueError('传入的columns{}不是list类型，请检查传入参数！'.format(columns))

    # 如果保存更新列为None，则默认除了主键外，其他列都更新
    if update_columns is None:
        update_columns = [item for item in columns if item != primary_key]

    update_columns_list = []
    for item in update_columns:
        update_columns_list.append("`{item}` = VALUES(`{item}`)".format(item=item))

    # 拼接SQL
    sql = '''INSERT INTO {table}({columns}) 
                VALUES ({placeholder}) 
                ON DUPLICATE KEY UPDATE {update_columns}'''.format(table=table,
                                                                   columns=','.join(columns),
                                                                   placeholder=','.join(['%s'] * len(columns)),
                                                                   update_columns=','.join(update_columns_list))

    # 对数据进行处理
    if type_convert:
        tmp = data.copy().astype(str)
    else:
        tmp = data
    # 替换空值
    tmp.replace({'nan': None, 'NaT': None, 'None': None}, inplace=True)

    connector.executeSqlManyByConn(sql, tmp[columns].values.tolist())
    logging.info(
        '数据库{}中的表{}已经插入更新列{}，共计{}行，时间戳{}'.format(connector.schema, table, update_columns, len(tmp),
                                                 date2str(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))
    del tmp


def save_df_to_mysql_after_delete(connector, data, table, columns, logging, update_columns=None, primary_key='id'):
    """先清空表中原本数据，然后再插入新数据，只能操作实时表，不要再流水表汇总使用"""
    try:
        del_sql = 'truncate table {}'.format(table)
        connector.executeSqlByConn(del_sql)
        logging.info('当前表：{}的数据已经清除！'.format(table))

        save_dataframe_to_mysql(connector, data, table, columns=columns, logging=logging,
                                update_columns=update_columns, primary_key=primary_key)

    except:
        connector.rollback()
        logging.error('当前表: {}的数据插入失败，进行数据回滚！')


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


def try_except(func):
    '''定义一个计算执行时间的函数作装饰器，传入参数为装饰的函数或方法'''

    # 定义嵌套函数，用来打印出装饰的函数的执行时间
    @wraps(func)
    def call_func(*args, **kwargs):
        try:
            func_return = func(*args, **kwargs)
            # 返回func的返回值
            return func_return
        except:
            ex = traceback.format_exc()
            script_error_alert(os.path.basename(__file__), ex)

    # 返回嵌套的函数
    return call_func


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


def date2timestamp(parameter):
    '''
    将日期格式转换为时间戳
    '''
    return int(time.mktime(parameter.timetuple())) * 1000


def str2date(parameter, format='%Y-%m-%d'):
    '''
    将日期格式转为字符串， 默认为YYYT-MM-DD
    :param parameter:
    :param format:
    :return:
    '''
    return datetime.datetime.strptime(parameter, format)


def str2date(parameter, format='%Y-%m-%d'):
    '''
    将日期格式转为字符串， 默认为YYYT-MM-DD
    :param parameter:
    :param format:
    :return:
    '''
    return datetime.datetime.strptime(parameter, format)


def date2int(parameter):
    '''将日期转为数字格式， 返回字符串格式'''
    if not isinstance(parameter, str):
        parameter = date2str(parameter)
    return ''.join(parameter.split('-'))

@print_execute_time
def save_dataframe_to_excel(outfile, datas=[], sheets_name=None, column_wide=18):
    '''
    保存dataframe 到Excel中， 可以保存为多个sheet
    :param outfile:
    :param datas:
    :param sheets_name:
    :return:
    '''
    # 传入的data和sheets_name必须是list格式
    if not isinstance(datas, list):
        raise ValueError('传入的data格式必须是list格式')

    if sheets_name is None:
        sheets_name = ['Sheet{}'.format(str(i+1)) for i in range(len(datas))]
    elif not isinstance(datas, list):
        raise ValueError('传入的sheets_name格式必须是list格式')

    # data和sheet_name的长度必须相等
    if len(datas) != len(sheets_name):
        raise ValueError('传入的data和sheets_name的长度必须相等')

    writer = pd.ExcelWriter(outfile,
                            engine='xlsxwriter',
                            datetime_format='YYYY-MM-DD HH:MM:SS',
                            date_format='YYYY-MM-DD')
    # 保存数据组中的所有DF
    for i in range(len(datas)):
        datas[i].to_excel(writer, sheet_name=sheets_name[i], index=False)

        # 设置列宽
        sheet_name = writer.sheets[sheets_name[i]]
        sheet_name.set_column('A:Z', column_wide)
    writer.save()

    # 输出文件保存地址，文件大小
    print('文件保存路径：{}\n文件大小：{}M'.format(outfile, get_FileSize(outfile)))


email_map = {'retail': ["zhangli@mx2.zzss.com", "wuhuiru@mx2.zzss.com", "zhouqiaoqiao@mx2.zzss.com"],
             'data': ['yanzehao@mx2.zzss.com', "zhangyahui@mx2.zzss.com"],
             'logistics': ['fanghuawu@mx2.zzss.com', 'xiepuqin@mx2.zzss.com',
                           'jiangfan@mx2.zzss.com', 'chenmengzhu@mx2.zzss.com', 'zhaoxianliang@zzss.com'],
             'logistics_warehouse': ["wangguojun@zzss.com", "sunzhe@zzss.com", "liuhuanhuan@mx2.zzss.com",
                                     "lvshiquan@mx2.zzss.com"],
             'logistics_boss': ['zhangqingsong@zzss.com'],
             'qj': ['qj@zzss.com'],
             'fanghuawu': ['fanghuawu@mx2.zzss.com'],
             'product': ['sunbin@mx2.zzss.com', 'yangrui@mx2.zzss.com'],
             'purchase': ['zhanghuirong@zzss.com', 'zhangchenli@mx2.zzss.com'],
             'boss': ['huangaihua@mx2.zzss.com'],
             '北京': ['liuhuanhuan@mx2.zzss.com'],
             '华南': ['lvshiquan@mx2.zzss.com'],
             '华中': ['wangguojun@zzss.com'],
             '华东': ['sunzhe@zzss.com'],
             '西南': ['zhangqingsong@zzss.com'],
             '上海': ['fanghuawu@mx2.zzss.com']}

area_map = {'北京': ['北京市'],
            '华南': ['深圳市', '厦门市', '广州市'],
            '华中': ['南京市', '武汉市', '合肥市', '长沙市'],
            '华东': ['杭州市', '无锡市', '苏州市', '昆山市', '郑州市'],
            '西南': ['重庆市', '成都市', '西安市'],
            '上海': ['上海市', '济南市', '宁波市', '青岛市', '南通市']
            }


def send_email(path, msg, subject, receiver, cc=[], logging=None, delete=True):
    '''发送邮件
    receiver: 可以通过集合指定组： c， data, logistics, product, boss
    '''
    zsMail = ZsMail()

    # 对receiver进行判断，如果是dict格式，则进行组别的拼接，如果是list形式，则直接使用
    if type(receiver) == set:
        res = []
        for key in receiver:
            res += email_map[key]
        receiver = res

    if type(cc) == set:
        res = []
        for key in cc:
            res += email_map[key]
        cc = res

    if path is None:
        zsMail.sendMail(mail_msg=msg,
                        subject=subject,
                        sender='reporting@mx2.zzss.com',
                        receiver=receiver,
                        cc=cc)
    else:
        zsMail.sendMail(mail_msg=msg,
                        subject=subject,
                        sender='reporting@mx2.zzss.com',
                        receiver=receiver,
                        cc=cc,
                        attachFile=path)

        if delete and os.path.exists(path):
            os.remove(path)

    if logging:
        logging.info('邮件发送成功...')

def list2str(item_list):
    '''将list转为字符串格式'''
    item_list = [str(x) for x in item_list]
    quoted_list_str = "'" + "','".join(item_list) + "'"
    return quoted_list_str


def subprocess_decorator(func):
    '''定义一个执行程序的装饰器，用于自动执行程序;传入参数1：表对应的脚本路径，参数2：表名称'''

    @functools.wraps(func)
    def warpper(*args, **kwargs):
        logging = args[2]
        a = func()
        if a == 0:
            logging.info('{}，Program running normally'.format(args[1]))
        else:
            logging.info('{}，The program is not working properly'.format(args[1]))
            out = subprocess.call(['/home/datapy/anaconda3/bin/ipython', args[0]])
            if out != 0:
                dinTalk = DingTalk('b46fd2eee9cc6707da38074262618a656cd6d2a8bf7ddc4e9debea5efb36a608')
                # dinTalk = DingTalk('995ed406df411d2173b2c34487684785cd693fb3f89d0d169731213d74776210')
                content = '''
                    【数据流水同步监控报警】
                    表名：{}
                    同步脚本: {}
                    该表前一日数据流水未同步，请及时同步！
                    监控时间：{}
                    '''.format(args[1], os.path.basename(args[0]), datetime.datetime.now())
                mobile_list = ['18271681654', '15565369015', '18979978602']
                dinTalk.send_DingTalk_text(content, mobile_list)
                logging.info('dindin info had send。。。')

    return warpper


def script_error_alert(filename, ex_info, receiver_key=None):
    '''
    将错误信息以邮件的形式发送
    :param ex_info:错误信息
    :param receiver: 收件人
    '''

    receiver_dict = {
        "DATA_GRP": ["yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com", 'wenyangfan@mx2.zzss.com'],
        "yanzehao": ["yanzehao@mx2.zzss.com"],
        "zhangyahui": ["zhangyahui@mx2.zzss.com"],
        "wenyangfan": ['wenyangfan@mx2.zzss.com'],
        "fanghaohui": ["yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com", "wenyangfan@mx2.zzss.com"],
        "haohuiwen": ["yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com", "wenyangfan@mx2.zzss.com"]}

    if receiver_key is None:
        receiver_key = "DATA_GRP"

    try:
        receiver = receiver_dict[receiver_key]
    except KeyError:
        raise KeyError("cannot find {}".format(receiver_key))

    ex_info = ex_info.replace("\n", "<br>")

    script_name = filename
    ex_body = '''
            <h2> Script Error: </h2>
            %s
            <h2> Time: </h2>
            %s
            <h2> Traceback: </h2>
            %s
            ''' % (script_name, str(datetime.datetime.now())[:19], ex_info)

    zsMail = ZsMail()
    zsMail.sendMail(mail_msg=ex_body,
                    subject="{}运行错误".format(script_name),
                    sender="reporting@mx2.zzss.com",
                    receiver=receiver)

    print("邮件发送成功。收件人:{}".format(receiver))

    mongo_mysql = QunaZsMySQL.QunaMysql('zs_mongo')

    sql = '''
    insert into zs_data_script_monitoring(`date`,`name`,`errorMsg`,`ct`)
    values (%s,%s,%s,%s)

    '''
    mongo_mysql.executeSqlManyByConn(sql,
                                     ((datetime.datetime.now().date(), filename, ex_info, datetime.datetime.now())))


# 读取文件，返回json格式
def read_file_return_json(path):
    '''读取文件，返回json格式'''
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data


def reduce_merge(reduce_data_list, on_keys, how_type='left'):
    '''利用reduce合并多个相同的DF, 第一个DF为保留结果'''
    all_data = reduce(lambda left, right: pd.merge(left, right, how=how_type, on=on_keys), reduce_data_list)
    return all_data


def get_time_args(args):
    '''获取程序传入参数，输出参数时间，手动输入参数格式： ‘2020-10-10 10:10:00’'''
    if len(args) > 1:
        time, = args[1:2]
        if '+' in time:
            # airflow传的参数
            if '.' in time:
                time = datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f+08:00').replace(microsecond=0)
            else:
                time = datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S+08:00').replace(microsecond=0)
        else:
            # 在linux上手动执行传入的参数
            time = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    else:
        time = datetime.datetime.now()

    return time


def float2percentage(data, columns=[]):
    """将数字转成百分比"""
    # 如果是DataFrame， 将整列进行转换
    if isinstance(data, pd.DataFrame):
        for column in columns:
            data[column] = data[column].apply(lambda x: '%.2f%%' % (x * 100))

        return data

    # 将数字转为百分数
    if isinstance(data, float):
        return '%.2f%%' % (data * 100)


def generate_main_city_data(data, connector, key='city_name', type='name'):
    """生成主城市的数据"""

    zs_juezhao_salesman_data = ZsJuezhaoSalesmanData(connector)

    # 获取城市依赖字典
    if type == 'name':
        city_dict = zs_juezhao_salesman_data.get_city_dependence_dict()
    else:
        city_dict = zs_juezhao_salesman_data.get_cityid_dependence_dict()

    for minor_city, main_city in city_dict.items():
        if minor_city != main_city:
            data.loc[data[key] == minor_city, key] = main_city

    return data