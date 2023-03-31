# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/31 15:31
# @Author  : diaozhiwei
# @FileName: 钉钉测试.py
# @description: 
# @update:
"""


# -*-coding:utf-8 -*-
"""
File name : zs_user_sales_dingding.py
Author : diaozhiwei
Created date: 2022/11/7 17:17
Description :
"""

from modules.dingtalk.DingTalk import DingTalk


def send_dingTalk(access_token,mobile_list):
    '''发送钉钉消息'''
    dingTalk = DingTalk(access_token)
    # 发送钉钉消息

    context = '''
    ----------------------------
    测试数据：+++++++++++
    ---------------------------
    '''
    dingTalk.send_DingTalk_text(context,mobile_list)


def main():
    # 报警发送钉钉群
    send_dingTalk(access_token,[''])


if __name__ == '__main__':
    access_token = '0555344754fdbdabb56ca53eb347e8fc150a2979c22a2c91e4860443bb449fa1'
    main()



