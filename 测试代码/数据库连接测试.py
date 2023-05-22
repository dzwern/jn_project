# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/31 15:31
# @Author  : diaozhiwei
# @description: 数据库测试
# @update:
"""
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils


# 执行SQL
@utils.print_execute_time
def get_orders():
    sql='''
    select * from t_wechat a
    where a.tenant_id=11
    '''
    df=hhx_sql1.get_DataFrame_PD(sql)
    return df


# 保存SQL
def save_sql(df):
    sql = '''
    INSERT INTO `t_user_campaign` 
     (`id`,`sys_user_id`,`user_name`,`nick_name`,`dept_name1`
     ) 
     VALUES (%s,%s,%s,%s,%s
     )
     ON DUPLICATE KEY UPDATE
         `sys_user_id`=values(`sys_user_id`),`user_name`=values(`user_name`),`nick_name`=values(`nick_name`),
         `dept_name1`=values(`dept_name1`), `dept_name2`=values(`dept_name2`)
         '''
    hhx_sql2.executeSqlManyByConn(sql, df.values.tolist())


# 发送钉钉
def send_dingTalk(access_token,mobile_list):
    '''发送钉钉消息'''
    dingTalk = DingTalk(access_token)
    # 发送钉钉消息

    context = '''
    ----------------------------
    测试数据：数据有误
    ---------------------------
    '''
    dingTalk.send_DingTalk_text(context,mobile_list)


def main():
    df_orders=get_orders()
    print(df_orders)
    # 报警发送钉钉群
    send_dingTalk(access_token,[''])


if __name__ == '__main__':
    # 钉钉open
    access_token = '0555344754fdbdabb56ca53eb347e8fc150a2979c22a2c91e4860443bb449fa1'
    hhx_sql1=jnMysql('crm_tm_jnmt','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2=jnMysql('hhx_dx','dzw','dsf#4oHGd','rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()







