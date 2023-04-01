"""
# @Time    : 2023/3/31 14:06
# @Author  : diaozhiwei
# @FileName: hhx_wechat_middle.py
# @description: 钉钉调动
# @update:
"""

import requests
import json
import logging
import pandas as pd

# https://oapi.dingtalk.com/robot/send?access_token=0555344754fdbdabb56ca53eb347e8fc150a2979c22a2c91e4860443bb449fa1

class DingTalk(object):
    def __init__(self, access_token='281e0e4342e38f6f54ae9f708c85d64da370dc500ee144caf0b8b0a11d80f2b4'):
        self.access_token = access_token
        self.headers = {"Content-Type": "application/json"}
        self.url = 'https://oapi.dingtalk.com/robot/send?access_token=%s' % access_token

    def send_DingTalk_text(self, content, mobile_list, isAtAll=False):
        '''
        发送文本信息
        :param content: 发送内容
        :param mobile_list: @人员的手机号，list格式
        :param isAtAll: True为@所有人，默认为False
        :return:
        '''
        data = {
            "msgtype": "text",
            "text": {
                "content": content
            },
            "at": {
                "atMobiles": mobile_list,
                "isAtAll": isAtAll
            }
        }
        response = requests.post(url=self.url, headers=self.headers, data=json.dumps(data), verify=False)
        msg = json.loads(response.text)
        if msg.get('errmsg') == 'ok' and msg.get('errcode') == 0:
            logging.info("信息发送成功....")
        else:
            logging.info("信息发送失败....")

    def send_DingTalk_link(self, text, title, picUrl=None, messageUrl=None):
        '''
        发送链接信息
        :param text: 消息内容
        :param title: 消息标题
        :param picUrl: 图片URL
        :param messageUrl: 点击消息跳转的URL
        :return:
        '''
        data = {
            "msgtype": "link",
            "link": {
                "text": text,
                "title": title,
                "picUrl": picUrl,
                "messageUrl": messageUrl
            }
        }
        response = requests.post(url=self.url, headers=self.headers, data=json.dumps(data), verify=False)
        msg = json.loads(response.text)
        if msg.get('errmsg') == 'ok' and msg.get('errcode') == 0:
            logging.info("信息发送成功....")
        else:
            logging.info("信息发送失败....")

    def send_DingTalk_markdown(self, text, title, mobile_list, isAtAll=False):
        '''
        发送markdown信息
        :param text: markdown格式的消息
        :param title:首屏会话透出的展示内容
        :param mobile_list:被@人的手机号(在text内容里要有@手机号)
        :param isAtAll:@所有人时：true，否则为：false
        :return:
        '''
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": text
            },
            "at": {
                "atMobiles": mobile_list,
                "isAtAll": isAtAll
            }
        }
        response = requests.post(url=self.url, headers=self.headers, data=json.dumps(data), verify=False)
        msg = json.loads(response.text)
        if msg.get('errmsg') == 'ok' and msg.get('errcode') == 0:
            logging.info("信息发送成功....")
        else:
            logging.info("信息发送失败....")
