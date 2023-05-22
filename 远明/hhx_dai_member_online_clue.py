# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/15 16:50
# @Author  : diaozhiwei
# @FileName: hhx_dai_member_online_clue.py
# @description: 
# @update:
"""

import pandas as pd
from datetime import datetime, timedelta
import sys
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import numpy as np

userName = 'dzw'
password = 'dsf#4oHGd'
dbHost = 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com'
dbPort = 3306
URL = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/'
schema = 'crm_tm_jnmt'
schema2 = 'ymlj_dx'
engine = create_engine(URL + schema + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)
engine2 = create_engine(URL + schema2 + '?charset=utf8', pool_pre_ping=True, pool_recycle=3600 * 4)


# 加载数据到df
def get_DataFrame_PD(sql='SELECT * FROM DUAL'):
    conn = engine.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 加载数据到df
def get_DataFrame_PD2(sql='SELECT * FROM DUAL'):
    conn = engine2.connect()
    with conn as connection:
        dataFrame = pd.read_sql(sql, connection)
        return dataFrame


# 批量执行更新sql语句
def executeSqlManyByConn(sql, data):
    conn = engine2.connect()
    if len(data) > 0:
        with conn as connection:
            return connection.execute(sql, data)


# 时间转化字符串
def date2str(parameter, format='%Y-%m-%d'):
    if isinstance(parameter, str):
        return parameter
    return parameter.strftime(format)


# 执行sql
def executeSqlByConn(sql='SELECT * FROM DUAL', conn=None):
    conn = engine2.connect()
    with conn as connection:
        return connection.execute(sql)


def get_member_online_clue():
    sql = '''
    SELECT 
        channel, centel, department, gro, 
        来源 as source, 获客年份 as in_year, 获客月份 in_month, 获客日期 in_date, member_source_level2, 
    -- 	cycle_name, order_period, act_name, 
        COUNT(1) in_number
            , sum(if(conv_int='即时转化',1,0)) 即时转化数
            , sum(if(conv_int='一周内转化',1,0)) 一周内转化数
            , sum(if(conv_int='一月内转化',1,0)) 一月内转化数
            , sum(if(conv_int='其他',1,0)) 其他转化数
            , sum(当日产出金额) 当日产出金额
            , sum(一周内产出金额) 一周内产出金额
            , sum(一月内产出金额) 一月内产出金额
            , sum(其他产出金额) 其他产出金额
        
    FROM (
        SELECT 
            m.dept_name, channel , centel, department , gro,
            case 
                WHEN is_fission<>-1 THEN '主动&被动裂变'
                WHEN member_source_level2 like '%%扫码裂变%%' THEN '盖内码裂变'
                WHEN member_source = '投流线索获取' THEN '推广投流'
                WHEN member_source = '电商平台获取' THEN '电商平台'
                WHEN member_source = '直播平台获取' AND LEAST(first_communicate_time , incoming_line_time)>'2023-02-01' THEN '陈坛6直播'
                ELSE '其他'
            end as 来源
            , member_source_level2
            , year(LEAST(first_communicate_time , incoming_line_time)) 获客年份
            , month(LEAST(first_communicate_time , incoming_line_time)) 获客月份
            , left(LEAST(first_communicate_time , incoming_line_time),10) 获客日期
    -- 		, cy.order_period , cy.cycle_name, cy.act_name
    -- 		, m.member_id
            , ou.*
        FROM ymlj_dx.dai_member_new m
        INNER JOIN ymlj_dx.dai_dept dep on m.dept_name = dep.dept_name
    -- 	LEFT JOIN t_act_schedule as cy on LEAST(first_communicate_time , incoming_line_time) BETWEEN cy.start_time AND cy.end_time
        LEFT JOIN (
            SELECT member_id, conv_int
            ,sum(if(out_int='当日产出',jieyu_amount,0)) 当日产出金额
            ,sum(if(out_int='一周内产出',jieyu_amount,0)) 一周内产出金额
            ,sum(if(out_int='一月内产出',jieyu_amount,0)) 一月内产出金额
            ,sum(if(out_int='其他',jieyu_amount,0)) 其他产出金额
        FROM (
            -- 23年投流线索 是否首单判断 转化间隔判断 产出间隔判断
            SELECT ord.member_id, conv.conv_int,jieyu_amount, 
                CASE 
                WHEN DATEDIFF(ord.create_time, ord.incoming_line_time) = 0 THEN '当日产出'
                WHEN DATEDIFF(ord.create_time, ord.incoming_line_time) < 7 THEN '一周内产出'
                WHEN DATEDIFF(ord.create_time, ord.incoming_line_time) < 30 THEN '一月内产出'
                ELSE '其他'
            END as out_int
            FROM `ymlj_dx`.`dai_orders_new` ord 
            LEFT JOIN (
            -- 23年投流线索，转化间隔
                SELECT member_id, 
                    CASE 
                    WHEN DATEDIFF(MIN(create_time),MIN(incoming_line_time)) = 0 THEN '即时转化'
                    WHEN DATEDIFF(MIN(create_time),MIN(incoming_line_time)) < 7 THEN '一周内转化'
                    WHEN DATEDIFF(MIN(create_time),MIN(incoming_line_time)) < 30 THEN '一月内转化'
                    ELSE '其他'
                END as conv_int
    
                FROM `ymlj_dx`.`dai_orders_new` 
                WHERE year(LEAST(first_communicate_time , incoming_line_time))>=2023 AND create_time>='2023-01-01'
                GROUP BY member_id
            ) conv on ord.member_id = conv.member_id
            WHERE ord.create_time>='2023-01-01' AND year(LEAST(ord.first_communicate_time , ord.incoming_line_time))>=2023
        ) out_put
        GROUP BY member_id, conv_int
        ) ou on ou.member_id = m.member_id
        WHERE 1=1
            AND year(LEAST(first_communicate_time , incoming_line_time))>=2023
    -- 		AND m.member_id < 10000
            AND channel = '销售部'
    ) a
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    '''
    df = get_DataFrame_PD(sql)
    return df


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `dai_member_online_clue` 
     (`id`,`channel`,`centel`,`department`,`gro`,`source`,
     `member_source_level2`,`years`,`monthly`,`incoming_line_time`,`fans`,
     `js_members`,`weekly_members`,`monthly_members`,`other_members`,`js_amounts`,
     `weekly_amounts`,`monthly_amounts`,`other_amounts`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `channel`= VALUES(`channel`),`centel`=VALUES(`centel`),`department`=values(`department`),`gro`=values(`gro`),
         `source`=values(`source`),`member_source_level2`=values(`member_source_level2`),`years`=values(`years`),
         `monthly`=values(`monthly`),`incoming_line_time`=values(`incoming_line_time`),`fans`=values(`fans`),
         `js_members`=values(`js_members`),`weekly_members`=values(`weekly_members`),`monthly_members`=values(`monthly_members`),
         `other_members`=values(`other_members`),`js_amounts`=values(`js_amounts`),`weekly_amounts`=values(`weekly_amounts`),
         `monthly_amounts`=values(`monthly_amounts`),`other_amounts`=values(`other_amounts`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


# 中间表删除
def del_sql():
    sql = '''
    truncate table dai_member_online_clue;
    '''
    executeSqlByConn(sql)


def main():
    df_member_online_clue = get_member_online_clue()
    df_member_online_clue = df_member_online_clue.fillna(0)
    df_member_online_clue['id'] = df_member_online_clue['centel'] + df_member_online_clue['department'].astype(str) + \
                                  df_member_online_clue['gro'].astype(str) + df_member_online_clue['member_source_level2'] + \
                                  df_member_online_clue['in_date']
    df_member_online_clue = df_member_online_clue[
        ['id', 'channel', 'centel', 'department', 'gro', 'source', 'member_source_level2', 'in_year', 'in_month',
         'in_date', 'in_number', '即时转化数', '一周内转化数', '一月内转化数', '其他转化数', '当日产出金额',
         '一周内产出金额',
         '一月内产出金额', '其他产出金额']]
    del_sql()
    save_sql(df_member_online_clue)


if __name__ == '__main__':
    main()
