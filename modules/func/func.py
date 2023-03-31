# -*-coding:utf-8-*-
import csv
import os
import hashlib
import datetime
from datetime import datetime as dtt
from datetime import date as dt
from datetime import timedelta
from modules.email.ZsMail import ZsMail
from modules.mysql import QunaMysql, QunaZsMySQL


class CommonSignUtil(object):

    def __init__(self):
        self.DEFAULT_BITCNT = 6
        # 密钥
        self.SaltCode = "www.suteng.com/zzss/20-0201-2ddd-zdkfja0-=0-1ndiz9m/pc;lfap2io23nzna92nfl821a0c82djknqhg"

    def getMd5SignWithoutTime(self, data):
        class_name = self.__class__.__name__
        srcStr = class_name[1:self.DEFAULT_BITCNT + 1]
        securStr = class_name[0:self.DEFAULT_BITCNT]
        securArray = []
        for i in range(len(securStr)):
            securArray.append(securStr[len(securStr) - 1 - i])

        buf = ''
        for j in range(len(srcStr)):
            buf += str(ord(srcStr[j]) ^ ord(securArray[j % len(securArray)]))
        joint_data = data + self.SaltCode + buf
        m = hashlib.md5()
        m.update(joint_data.encode(encoding='utf-8'))
        hash_data = m.hexdigest()
        return hash_data


def logistics_area_email():
    '''物流区域邮件'''
    # 按区域划分手机号
    dict_area_mobile = {"上海区域": ["19357327916", "17847017780", "15890070973", "18616547996"],
                        "华东区域": ["13376226665", "15312457905", "13023603360", "15190567656", "15890070973"],
                        "西南区域": ["13339999841", "18229873348", "13739492112", "18092565156", "15923021796",
                                 "18970938318"],
                        "华南区域": ["18105900358", "18108284061", "13826017187", "13421317576", "13826017187",
                                 "13421317576"],
                        "华北区域": ["18911095920", "15053288080", "15554153377", "18005351698", "13642036836"]}

    # 各区域主管邮箱，结构 [['区域主管邮箱'], ['抄送人邮箱']]
    dict_area_email = {"上海区域": [["liangmeng@mx2.zzss.com"], []],
                       "华东区域": [["wangguojun@zzss.com"], []],
                       "西南区域": [["linpeng@mx2.zzss.com"], []],
                       "华南区域": [["lvshiquan@mx2.zzss.com"], []],
                       "华北区域": [["liuhuanhuan@mx2.zzss.com"], []],
                       "全国区域": [
                           ["zhangqingsong@zzss.com", "chenchun@zzss.com", "zhouxin@zzss.com", "fanghuawu@mx2.zzss.com",
                            "xiepuqin@mx2.zzss.com", "yanzehao@mx2.zzss.com"], []]}

    # 按区域划分城市
    dict_area = {
        "上海区域": ["上海市", "苏州市", "昆山市", "杭州市", "郑州市"],
        "华东区域": ["无锡市", "芜湖市", "南京市", "合肥市", "宁波市", "常州市", "南通市"],
        "西南区域": ["重庆市", "成都市", "西安市", "南昌市", "武汉市", "长沙市"],
        "华南区域": ["厦门市", "广州市", "深圳市", "佛山市", "福州市", "东莞市"],
        "华北区域": ["北京市", "烟台市", "天津市", "青岛市", "济南市"],
        "全国区域": ["全国区域"],
    }

    # 各城市负责人邮箱
    dict_city_email = {
        "linpeng@mx2.zzss.com": ["成都市"],
        "zhanggaopeng@zzss.com": ["西安市"],
        "yangchaopei@mx2.zzss.com": ["重庆市"],
        "dongdongdong@mx2.zzss.com": ["青岛市"],
        "ludonghai@mx2.zzss.com": ["济南市"],
        "chenzhifeng@mx2.zzss.com": ["宁波市"],
        "zhangzhishang@mx2.zzss.com": ["上海市"],
        "yuanpengfei@mx2.zzss.com": ["合肥市"],
        "yubaocheng@mx2.zzss.com": ["南京市"],
        "liuguisi@mx2.zzss.com": ["武汉市"],
        "lishu@mx2.zzss.com": ["长沙市"],
        "chenguobing@mx2.zzss.com": ["常州市", "无锡市"],
        "wangtaohz@mx2.zzss.com": ["杭州市"],
        "liuzhigang@mx2.zzss.com": ["昆山市", "苏州市"],
        "liangmeng@mx2.zzss.com": ["郑州市"],
        "liuhuanhuan@mx2.zzss.com": ["北京市"],
        "liwencan@zzss.com": ["佛山市", "广州市"],
        "qiumin@zzss.com": ["深圳市", "东莞市"],
        "lvshiquan@mx2.zzss.com": ["厦门市"],
        "zhongshaohua@mx2.zzss.com": ["烟台市"],
        "zhangning@mx2.zzss.com": ["天津市"],
        "sunwei@mx2.zzss.com": ["芜湖市"],
        "dantangxin@mx2.zzss.com": ["南通市"],
        "maoweichen@mx2.zzss.com": ["福州市"],
        "zouweimin@mx2.zzss.com": ["南昌市"],
    }

    return dict_area_mobile, dict_area_email, dict_area, dict_city_email


def logistics_area_dindin():
    '''
    物流钉钉邮件发送人信息
    :return:
    '''
    dict_mobile = {"shanghai": ["19357327916", "17847017780", "15890070973", "18616547996"],
                   "huadong": ["15061468928", "15255336813", "13813807584", "13856923707", "15156248750", "13601615669",
                               "15155130053"],
                   "xinan": ["13339999841", "18229873348", "13739492112", "18092565156", "15923021796", "18970938318"],
                   "huanan": ["18105900358", "18108284061", "13826017187", "13421317576", "13826017187", "13421317576"],
                   "beijing": ["18911095920", "15053288080", "15554153377", "18005351698", "13642036836"]}

    dict_area = {
        "shanghai": ["上海市", "苏州市", "昆山市", "杭州市", "郑州市"],
        "huadong": ["无锡市", "常州市", "芜湖市", "南京市", "合肥市", "宁波市", "南通市"],
        "xinan": ["重庆市", "成都市", "西安市", "长沙市", "武汉市", "南昌市"],
        "huanan": ["厦门市", "广州市", "深圳市", "佛山市", "福州市", "东莞市"],
        "beijing": ["北京市", "青岛市", "济南市", "烟台市", "天津市"]
    }

    dict_token = {
        "shanghai": "db28bf9eebd8c438e831b09c709588fc8f4773d111c25fe476332cddcd5fa71e",
        "huadong": "6e504822c171db3d0956bc394b24f79424d776865c3e0a9ccea5434ac764df65",
        "xinan": "b3e87876fe745d132b9b0a9579ed45883c30982b726d8139095657a3c6c0678b",
        "huanan": "fd4545829e27033a795563f78ff15a101ad78e0007d0857852addbbeed2e4137",
        "beijing": "a8dba8875c09f1fd0e3907b0e367972d5e16682e3ce669f75650787e172822f7"
    }

    return dict_mobile, dict_area, dict_token


def script_error_alert(filename, ex_info, receiver_key=None):
    '''
    将错误信息以邮件的形式发送
    :param ex_info:错误信息
    :param receiver: 收件人
    '''

    receiver_dict = {
        "DATA_GRP": ["lile@mx2.zzss.com", "wangqinyue@mx2.zzss.com"],
        "yanzehao": ["yanzehao@mx2.zzss.com"],
        "zhangyahui": ["zhangyahui@mx2.zzss.com"],
        "wenyangfan": ['wenyangfan@mx2.zzss.com'],
        "fanghaohui": ["yanzehao@mx2.zzss.com", "chenhanzhen@mx2.zzss.com", "wangqinyue@mx2.zzss.com"],
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
            ''' % (script_name, str(dtt.now())[:19], ex_info)

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


def script_error_alert_test(filename, ex_info, receiver_key=None):
    '''
    将错误信息以邮件的形式发送
    :param ex_info:错误信息
    :param receiver: 收件人
    '''

    receiver_dict = {
        "DATA_GRP": ["lile@mx2.zzss.com", 'linan@mx2.zzss.com'],
        "yanzehao": ["yanzehao@mx2.zzss.com"],
        "zhangyahui": ["zhangyahui@mx2.zzss.com"],
        "wenyangfan": ['wenyangfan@mx2.zzss.com'],
        "fanghaohui": ["fangpeng@mx2.zzss.com", "yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com",
                       "wenyangfan@mx2.zzss.com"],
        "haohuiwen": ["yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com", "wenyangfan@mx2.zzss.com"]}

    if receiver_key is None:
        receiver_key = "DATA_GRP"

    try:
        receiver = receiver_dict["DATA_GRP"]
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
            ''' % (script_name, str(dtt.now())[:19], ex_info)

    zsMail = ZsMail()
    zsMail.sendMail(mail_msg=ex_body,
                    subject="测试{}运行错误".format(script_name),
                    sender="reporting@mx2.zzss.com",
                    receiver=receiver)

    print("邮件发送成功。收件人:{}".format(receiver))


def script_warning_alert(filename, ex_info, receiver_key=None):
    '''
    将警告信息以邮件的形式发送
    :param ex_info:警告信息
    :param receiver: 收件人
    '''

    receiver_dict = {
        "DATA_GRP": ["yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com", "wenyangfan@mx2.zzss.com"],
        "yanzehao": ["yanzehao@mx2.zzss.com"],
        "zhangyahui": ["zhangyahui@mx2.zzss.com"],
        "wenyangfan": ["wenyangfan@mx2.zzss.com"],
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
            <h2> Script Warning: </h2>
            %s
            <h2> Time: </h2>
            %s
            <h2> Traceback: </h2>
            %s
            ''' % (script_name, str(dtt.now())[:19], ex_info)

    zsMail = ZsMail()
    zsMail.sendMail(mail_msg=ex_body,
                    subject="{}运行警告".format(script_name),
                    sender="reporting@mx2.zzss.com",
                    receiver=receiver)

    print("邮件发送成功。收件人:{}".format(receiver))


def notification_email(filename, message, receiver_key=None, subject=None):
    '''
    将错误信息以邮件的形式发送
    :param message:错误信息
    :param receiver: 收件人
    '''

    receiver_dict = {
        "DATA_GRP": ["yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com", 'wenyangfan@mx2.zzss.com'],
        "yanzehao": ["yanzehao@mx2.zzss.com"],
        "zhangyahui": ["zhangyahui@mx2.zzss.com"],
        "wenyangfan": ['wenyangfan@mx2.zzss.com'],
        "haohuiwen": ["yanzehao@mx2.zzss.com", "zhangyahui@mx2.zzss.com", "wenyangfan@mx2.zzss.com"]}

    if receiver_key is None:
        receiver_key = "DATA_GRP"

    try:
        receiver = receiver_dict[receiver_key]
    except KeyError:
        raise KeyError("cannot find {}".format(receiver_key))

    message = message.replace("\n", "<br>")

    script_name = filename
    ex_body = '''
            <h2> Script Name: </h2>
            %s
            <h2> Time: </h2>
            %s
            <h2> Message: </h2>
            %s
            ''' % (script_name, str(dtt.now())[:19], message)

    zsMail = ZsMail()

    if subject is None:
        subject = "{}通知消息".format(script_name)

    zsMail.sendMail(mail_msg=ex_body,
                    subject=subject,
                    sender="reporting@mx2.zzss.com",
                    receiver=receiver)

    print("邮件发送成功。收件人:{}".format(receiver))


def quoted_list_func(item_list):
    '''当SQL语句中用 IN 的时候，把python list转为 IN 后面的内容'''
    item_list = [str(x) for x in item_list]
    quoted_list_str = "'" + "','".join(item_list) + "'"
    return quoted_list_str


def get_date(end_date=None, end_day_offset=0, type="string", days_interval=1):
    '''
    返回指定条件的开始日期和结束日期
    :param end_date: 结束日期，如果未指定则为今天
    :param end_day_offset: 结束日期偏移, 例如:想指定结束日期前一天，offset = -1
    :param type: 返回类型, string or date
    :param days_interval: 开始日期和结束日期的天数间隔
    :return:
    '''

    if not isinstance(end_day_offset, int):
        raise ValueError("end day offset should be integers")

    if end_date is None:
        end_date = dtt.now().date() + timedelta(days=end_day_offset)

    elif isinstance(end_date, dtt) or isinstance(end_date, dt):
        end_date = end_date

    elif isinstance(end_date, str):
        try:
            end_date = dt(year=int(end_date[:4]), month=int(end_date[5:7]), day=int(end_date[8:10]))
        except:
            raise ValueError("If end_date is string,it should be like '2018-01-01'")

    else:
        raise ValueError("end_date should be either string or datetime object")

    start_date = end_date - timedelta(days=days_interval)

    if type == "string":
        return sorted((str(start_date), str(end_date)))
    elif type == "date":
        return sorted((start_date, end_date))
    else:
        raise ValueError("type should be either 'string' or 'date'")


def get_datetime(end_datetime=None, type="string", **kwargs):
    # todo 待完善
    '''
    返回指定条件的开始日期时间和结束日期时间
    :param end_datetime: 结束日期，如果未指定则为今天
    :param end_day_offset: 结束日期偏移, 例如:想指定结束日期前一天，offset = -1
    :param type: 返回类型, string or date
    :param days_interval: 开始日期和结束日期的天数间隔
    :return:
    '''

    if end_datetime is None:
        end_datetime = dtt.now().replace(microsecond=0)

    elif isinstance(end_datetime, dtt) or isinstance(end_datetime, dt):
        end_datetime = end_datetime

    elif isinstance(end_datetime, str):
        try:
            end_datetime = dtt.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
        except:
            raise ValueError("Cannot parse end_datetime to datetime object.It should be like '2018-01-01 10:00:00'")

    else:
        raise ValueError("end_datetime should be either string or datetime object")

    if kwargs == {}:
        start_datetime = end_datetime - timedelta(minutes=5)
    else:
        start_datetime = end_datetime - timedelta(**kwargs)

    if type == "string":
        return sorted((str(start_datetime), str(end_datetime)))
    elif type == "datetime":
        return sorted((start_datetime, end_datetime))
    else:
        raise ValueError("type should be either 'string' or 'datetime'")


def list_to_csv_fun(list, fileName, nested_list=False, mode="w", encoding="UTF-8"):
    '''
    导出list 到 csv
    :param list:
    :param fileName: 导出路径和名字
    :param nested_list: 是否是 lists in list
    :param mode:
    :param encoding:
    :return:
    '''
    if nested_list is False:
        with open(fileName, mode, encoding=encoding) as output:
            writer = csv.writer(output, lineterminator='\n')
            for record in list:
                writer.writerow([record])
    else:
        with open(fileName, mode, encoding=encoding) as output:
            writer = csv.writer(output, lineterminator='\n')
            for record in list:
                writer.writerows([record])


def try_remove(filepath):
    '''
    删除某个文件
    :param filepath:文件路径
    '''
    try:
        os.remove(filepath)
        print("existing file removed:{}".format(filepath))
    except OSError:
        pass


def check_df_dtype(df):
    """列出df中每列第一行的数据类型"""
    for col in df.columns:
        print(
            "{} : {}".format(col, type(df.loc[0, col]))
        )


def convert_mongo_time(mongo_time):
    if mongo_time:
        return (mongo_time + timedelta(hours=8))
    else:
        return None


def date2str(parameter):
    '''
    将datetime时间格式转为字符串
    :param parameter: 传入的datetime时间
    :return: 字符串
    '''
    if type(parameter) == dtt:
        parameter = parameter.strftime("%Y-%m-%d")
    return parameter


def str2date(parameter):
    '''字符串时间转成datetime'''
    if type(parameter) == str:
        parameter = dtt.strptime(parameter, '%Y-%m-%d')
    return parameter


def send_email(msg, subject, receiver, cc, path, logging):
    '''发送邮件'''
    zsMail = ZsMail()
    zsMail.sendMail(mail_msg=msg,
                    subject=subject,
                    sender='reporting@mx2.zzss.com',
                    receiver=receiver,
                    cc=cc,
                    attachFile=path)
    logging.info('邮件发送成功...')

    if os.path.exists(path):
        os.remove(path)
        logging.info('邮件删除成功...')


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
