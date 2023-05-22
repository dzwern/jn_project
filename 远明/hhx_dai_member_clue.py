# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/5/15 16:12
# @Author  : diaozhiwei
# @FileName: hhx_dai_member_clue.py
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


def get_member_clue():
    sql = '''
    SELECT a.dept_name, channel, centel, department,gro,left(incoming_line_time,10) incoming_line_time, CAST(MONTH(incoming_line_time) as char) 进线月份, clue_source, clue_source_level2
        , COUNT(1) 进线数, COUNT(1)-sum(clue_state) 非重复进线数, sum(add_wechat_state) 加微数, sum(if(follow_state=1,1,0)) 正常跟进数, SUM(promotion_budget) 推广预算
          ,sum(if(conv_int='即时转化',1,0)) 即时转化数
            ,sum(if(conv_int='一周内转化',1,0)) 一周内转化数
            ,sum(if(conv_int='一月内转化',1,0)) 一月内转化数
            ,sum(if(conv_int='其他',1,0)) 其他转化数
        , sum(当日产出金额) 当日产出金额
        , sum(一周内产出金额) 一周内产出金额
        , sum(一月内产出金额) 一月内产出金额
        , sum(其他产出金额) 其他产出金额
    FROM (
        -- 线索表：所属部门 推广日期 推广id 员工id 员工名称 加微状态 线索是否重复 跟进状态 线索id 进线时间
        SELECT sd.dept_id, sd.dept_name, p.promotion_date, p.promotion_id, 
        case
                when mc.clue_source = 0 then '小程序登录'
                when mc.clue_source = 1 then '投流线索获取'
                when mc.clue_source = 2 then '表单填报获取'
        end as clue_source,
        case
                when mc.clue_source_level2 = 100 then '百度'
                when mc.clue_source_level2 = 101 then '快手'
                when mc.clue_source_level2 = 102 then '腾讯'
                when mc.clue_source_level2 = 103 then '抖音'
                when mc.clue_source_level2 = 104 then '网易'
                when mc.clue_source_level2 = 200 then '公众号-远明酒业'
                when mc.clue_source_level2 = 201 then '公众号-远明之家'
                when mc.clue_source_level2 = 202 then '公众号-远明酱酒'
                when mc.clue_source_level2 = 203 then '公众号-远明老酒'
                when mc.clue_source_level2 = 204 then '官网-产品咨询	'
                when mc.clue_source_level2 = 205 then '官网-产品代理'
        end as clue_source_level2
            , mc.sys_user_id
            , su.nick_name
            , IFNULL(add_wechat_state, 0) add_wechat_state
            , IFNULL(mc.clue_state, 0)  clue_state 
            , IFNULL(follow_state, 0) follow_state 
            , mc.id clue_id
            , mc.incoming_line_time as incoming_line_time
        -- 	, DATE_ADD(mc.incoming_line_time, INTERVAL 6 HOUR) as '后移进线日期'        
        FROM crm_tm_ymlj.t_promotion AS p
        INNER JOIN crm_tm_ymlj.t_member_clue mc ON mc.promotion_id = p.promotion_id
        LEFT JOIN crm_tm_ymlj.sys_user AS su ON su.user_id = mc.sys_user_id
        LEFT JOIN crm_tm_ymlj.sys_dept AS sd ON sd.dept_id = su.dept_id
        WHERE 1 = 1
            AND p.deleted = 0 AND p.status = 0 -- 推广计划状态正常
            and p.promotion_date >= '2023-01-01' AND p.promotion_date < '2024-01-01' -- 只取23年推广计划
    ) a
    LEFT JOIN (
        -- 线价表：推广id 进线数 推广预算 线价
        SELECT p.promotion_id
        -- 	, count(1) 进线数, promotion_budget '推广预算' -- 非必要
            , promotion_budget/count(1) promotion_budget
        FROM crm_tm_ymlj.t_promotion AS p
        INNER JOIN crm_tm_ymlj.t_member_clue mc ON mc.promotion_id = p.promotion_id
        LEFT JOIN crm_tm_ymlj.sys_user AS su ON su.user_id = mc.sys_user_id
        LEFT JOIN crm_tm_ymlj.sys_dept AS sd ON sd.dept_id = su.dept_id
        WHERE 1 = 1
            and p.promotion_date >= '2023-01-01' and p.promotion_date < '2024-01-01'
            AND p.deleted = 0 AND p.status = 0
        GROUP BY p.promotion_id
    ) b on a.promotion_id = b.promotion_id
    left JOIN (
        SELECT member_clue_id, conv_int
            ,sum(if(out_int='当日产出',jieyu_amount,0)) 当日产出金额
            ,sum(if(out_int='一周内产出',jieyu_amount,0)) 一周内产出金额
            ,sum(if(out_int='一月内产出',jieyu_amount,0)) 一月内产出金额
            ,sum(if(out_int='其他',jieyu_amount,0)) 其他产出金额
        FROM (
            -- 23年投流线索 是否首单判断 转化间隔判断 产出间隔判断
            SELECT ord.*, tm.member_clue_id, conv.conv_int,
                CASE 
                WHEN DATEDIFF(ord.create_time, ord.incoming_line_time) = 0 THEN '当日产出'
                WHEN DATEDIFF(ord.create_time, ord.incoming_line_time) < 7 THEN '一周内产出'
                WHEN DATEDIFF(ord.create_time, ord.incoming_line_time) < 30 THEN '一月内产出'
                ELSE '其他'
            END as out_int
    
            FROM `ymlj_dx`.`dai_orders_new` ord 
            INNER JOIN `crm_tm_ymlj`.`t_member` tm on ord.member_id = tm.id
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
                WHERE incoming_line_time>='2023-01-01' AND member_source = '投流线索获取'
                GROUP BY member_id
            ) conv on ord.member_id = conv.member_id
            WHERE ord.create_time>='2023-01-01' AND ord.incoming_line_time>='2023-01-01' AND ord.member_source = '投流线索获取'
        ) out_put
        GROUP BY member_clue_id, conv_int
    ) ou on a.clue_id = ou.member_clue_id
    LEFT JOIN ymlj_dx.dai_dept dep on a.dept_name=dep.dept_name
    WHERE 1=1
    -- 	AND channel is not null
    GROUP BY dept_name, channel, centel, department, gro, left(incoming_line_time,10), clue_source, clue_source_level2
        '''
    df = get_DataFrame_PD(sql)
    return df


# 中间表删除
def del_sql():
    sql = '''
    truncate table dai_member_clue;
    '''
    executeSqlByConn(sql)


# 保存数据
def save_sql(df):
    sql = '''
    INSERT INTO `dai_member_clue` 
     (`id`,`dept_name`,`channel`,`centel`,`department`,
     `gro`,`incoming_line_time`,`monthly`,`clue_source`,`clue_source_level2`,
     `fans`,`no_fans`,`add_wechats`,`follow_fans`,`promotion_budget`,
     `js_members`,`weekly_members`,`monthly_members`,`other_members`,`js_amounts`,
     `weekly_amounts`,`monthly_amounts`,`other_amounts`
     ) 
     VALUES (
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s,%s,%s,
     %s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `dept_name`= VALUES(`dept_name`),`channel`= VALUES(`channel`),`centel`=VALUES(`centel`),
         `department`=values(`department`),`gro`=values(`gro`),`incoming_line_time`=values(`incoming_line_time`),
         `monthly`=values(`monthly`),`clue_source`=values(`clue_source`),`clue_source_level2`=values(`clue_source_level2`),
         `fans`=values(`fans`),`no_fans`=values(`no_fans`),`add_wechats`=values(`add_wechats`),
         `follow_fans`=values(`follow_fans`),`promotion_budget`=values(`promotion_budget`),`js_members`=values(`js_members`),
         `weekly_members`=values(`weekly_members`),`monthly_members`=values(`monthly_members`),`other_members`=values(`other_members`),
         `js_amounts`=values(`js_amounts`),`weekly_amounts`=values(`weekly_amounts`),`monthly_amounts`=values(`monthly_amounts`),
         `other_amounts`=values(`other_amounts`)
     '''
    executeSqlManyByConn(sql, df.values.tolist())


def main():
    df_member_clue = get_member_clue()
    df_member_clue = df_member_clue.fillna(0)
    df_member_clue['id'] = df_member_clue['dept_name'] + df_member_clue['clue_source_level2'] + df_member_clue[
        'incoming_line_time']
    df_member_clue = df_member_clue[
        ['id', 'dept_name', 'channel', 'centel', 'department', 'gro', 'incoming_line_time', '进线月份',
         'clue_source', 'clue_source_level2', '进线数', '非重复进线数', '加微数', '正常跟进数',
         '推广预算', '即时转化数', '一周内转化数', '一月内转化数', '其他转化数',
         '当日产出金额', '一周内产出金额', '一月内产出金额', '其他产出金额']]
    del_sql()
    save_sql(df_member_clue)


if __name__ == '__main__':
    main()
