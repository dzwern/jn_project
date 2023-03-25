# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/4 18:07
# @Author  : diaozhiwei
# @FileName: jnmt_member_divide.py
# @description: 各个部门的客户分级模型
"""

import pandas as pd

'''
<1000
1000-2000
2000-5000
5000-20000
>20000
'''


# 光辉部
def member_divide1(x):
    if x < 1000:
        return 'V1'
    elif 1000 <= x < 2000:
        return 'V2'
    elif 2000 <= x < 5000:
        return 'V3'
    elif 5000 <= x < 20000:
        return 'V4'
    else:
        return 'V5'


'''
v1	<500
v2	>500,(>1000,<4)	
v3	(>1000,>=4),>2000
v4	>5000,<7	
v5	>5000,>=7	

'''


# 光芒部
def member_divide2(x, y):
    if x < 500:
        return 'V1'
    elif 1000 > x >= 500:
        return 'V2'
    elif 2000 > x >= 1000 and y < 4:
        return 'V2'
    elif 2000 > x >= 1000 and y >= 4:
        return 'V3'
    elif 5000 > x >= 2000:
        return 'V3'
    elif x >= 5000 and y < 7:
        return 'V4'
    elif x >= 5000 and y >= 7:
        return 'V5'


'''
v1	<500
v2	500-2000
v3	2000-5000
v4	5000-20000
v5	>20000
'''


# 光华部
def member_divide3(x):
    if x < 500:
        return 'V1'
    elif 500 <= x < 2000:
        return 'V2'
    elif 2000 <= x < 5000:
        return 'V3'
    elif 5000 <= x < 20000:
        return 'V4'
    else:
        return 'V5'


'''
v1	<500,(500-1000,<5)
v2	(500-1000,>=5),>1000,(2000-5000,<3)
v3	2000-5000,>=3
v4	>5000,<6
v5	>5000,>=6
'''


# 光源部蜂蜜
def member_divide4(x, y):
    if x < 500:
        return 'V1'
    elif 500 <= x < 1000 and y < 5:
        return 'V1'
    elif 500 <= x < 1000 and y >= 5:
        return 'V2'
    elif 1000 <= x < 2000:
        return 'V2'
    elif 2000 <= x < 5000 and y < 3:
        return 'V2'
    elif 2000 <= x < 5000 and y >= 3:
        return 'V3'
    elif 5000 <= x and y < 6:
        return 'V4'
    elif 5000 <= x and y >= 6:
        return 'V5'


'''
(购买金额<2000)或者(2000<=购买金额<5000且购买次数<2)
(2000<=购买金额<5000且购买次数>=2)或者(5000<=购买金额<10000)
10000<=购买金额<50000且购买次数<11
(10000<=购买金额<50000且购买次数>10)或者(50000<=购买金额<100000)
100000<=购买金额
'''


def member_divide5(x, y):
    if x < 2000:
        return 'V1'
    elif 2000 <= x < 5000 and y < 2:
        return 'V1'
    elif 2000 <= x < 5000 and y >= 2:
        return 'V2'
    elif 5000 <= x < 10000:
        return 'V2'
    elif 10000 <= x < 50000 and y < 11:
        return 'V3'
    elif 10000 <= x < 50000 and y > 10:
        return 'V4'
    elif 50000 <= x < 100000:
        return 'V4'
    elif 100000 <= x:
        return 'V5'


def member_divide(df):
    # 光辉部
    df1 = df[df['所属部门'] == '光辉部']
    df1['客户等级'] = df1['历史购买金额'].apply(lambda x: member_divide1(x))
    # print(df1)
    # 光芒部
    df2 = df[df['所属部门'] == '光芒部']
    df2['客户等级'] = df2.apply(lambda x: member_divide2(x['历史购买金额'], x['历史购买次数']), axis=1)
    # print(df2)
    # 光华部
    df3 = df[df['所属部门'] == '光华部']
    df3['客户等级'] = df3['历史购买金额'].apply(lambda x: member_divide3(x))
    # print(df3)
    # 光源部蜂蜜
    df4 = df[df['所属部门'] == '光源部蜂蜜']
    df4['客户等级'] = df4.apply(lambda x: member_divide4(x['历史购买金额'], x['历史购买次数']), axis=1)
    # print(df4)
    # 光源部海参
    df5 = df[df['所属部门'] == '光源部海参']
    df5['客户等级'] = df5.apply(lambda x: member_divide5(x['历史购买金额'], x['历史购买次数']), axis=1)
    print(df5)
    df_member_divide = pd.concat([df1, df2, df3, df4, df5])
    print(df_member_divide)
    df_member_divide.to_excel('./客户分层2.xlsx', index=False)


if __name__ == '__main__':
    df_member = pd.read_excel('./客户分层.xlsx')
    member_divide(df_member)
