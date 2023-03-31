#!/usr/bin/env python
# coding=utf-8
"""
    Copyright (C) 2019 * Ltd. All rights reserved.
 
    Editor      : PyCharm
    File name   : workdays.py
    Author      : zhang yahui
    Created date: 2019/11/5 17:07
    Description :
       计算指定时间内有多少工作日和周末

"""
import datetime


# 计算两个日期之间的工作日数,非天数.
class workDays():
    def __init__(self, days_off=None):
        """days_off:休息日,默认周六日, 以0(星期一)开始,到6(星期天)结束, 传入tupple
        没有包含法定节假日,
        """
        self.days_off = days_off
        if days_off is None:
            self.days_off = 5, 6
        # 每周工作日列表
        self.days_work = [x for x in range(7) if x not in self.days_off]

    def timeFormat(self, start_date, end_date):
        '''实现日期格式的规范， 统一转成datetime格式'''
        if type(start_date) == str:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        if type(end_date) == str:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        if type(start_date) == datetime.date:
            start_date = datetime.datetime.strptime(str(start_date), '%Y-%m-%d')
        if type(end_date) == datetime.date:
            end_date = datetime.datetime.strptime(str(end_date), '%Y-%m-%d')
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        return start_date, end_date

    def date2str(self, parameter):
        '''
        将datetime时间格式转为字符串
        :param parameter: 传入的datetime时间
        :return: 字符串
        '''
        if type(parameter) == datetime.datetime:
            parameter = parameter.strftime("%Y-%m-%d")
        return parameter

    def Days(self, start_date, end_date, type='str'):
        """实现日期的 iter, 从start_date 到 end_date ,yield 日期
        """
        start_date, end_date = self.timeFormat(start_date, end_date)
        # 还没排除法定节假日
        tag_date = start_date
        while True:
            if tag_date > end_date:
                break
            if type == 'str':
                yield self.date2str(tag_date)
            else:
                yield tag_date

            tag_date += datetime.timedelta(days=1)

    def workDays(self, start_date, end_date, type='str'):
        """实现工作日的 iter, 从start_date 到 end_date , 如果在工作日内,yield 日期
        """
        start_date, end_date = self.timeFormat(start_date, end_date)
        # 还没排除法定节假日
        tag_date = start_date
        while True:
            if tag_date > end_date:
                break
            if tag_date.weekday() in self.days_work:
                if type == 'str':
                    yield self.date2str(tag_date)
                else:
                    yield tag_date

            tag_date += datetime.timedelta(days=1)



    def timeInterval(self, start_date, end_date, type='day'):
        '''返回两个日期的时间间隔'''
        start_date, end_date = self.timeFormat(start_date, end_date)
        interval = end_date - start_date
        if type == 'day':
            interval = interval.days + 1
        elif type == 'hour':
            interval = interval.days * 24 + interval.seconds//3600 + 1
        elif type == 'second':
            interval = interval.seconds

        return interval

    def workdaysCount(self, start_date, end_date):
        """工作日统计,返回数字"""
        start_date, end_date = self.timeFormat(start_date, end_date)
        return len(list(self.workDays(start_date, end_date)))

    def weekendsCount(self, start_date, end_date):
        '''周末统计， 返回数字'''
        start_date, end_date = self.timeFormat(start_date, end_date)
        return self.timeInterval(start_date, end_date) - self.workdaysCount(start_date, end_date)

    def weeksCount(self, start_date, end_date, day_start=0):
        """统计所有跨越的周数,返回数字
        默认周从星期一开始计算
        """
        start_date, end_date = self.timeFormat(start_date, end_date)
        day_nextweek = start_date
        while True:
            if day_nextweek.weekday() == day_start:
                break
            day_nextweek += datetime.timedelta(days=1)
        # 区间在一周内
        if day_nextweek > end_date:
            return 1
        weeks = ((end_date - day_nextweek).days + 1) / 7
        weeks = int(weeks)
        if ((end_date - day_nextweek).days + 1) % 7:
            weeks += 1
        if start_date < day_nextweek:
            weeks += 1
        return weeks

if __name__ == '__main__':
    startDate = datetime.datetime(2019, 11, 4)
    endDate = datetime.datetime(2019, 11, 10)
    workDay = workDays()
    print(workDay.workdaysCount(startDate, endDate))
    print(workDay.timeInterval(startDate, endDate))
    print(workDay.weekendsCount(startDate, endDate))
    print(workDay.weeksCount(startDate, endDate))
    print(list(workDay.Days(startDate, endDate, type='str')))

