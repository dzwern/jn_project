

'''
远程连接1.186服务器，读取相关日志信息，用于日志解析

'''

import os
import paramiko

#服务器信息，主机名（IP地址）、端口号、用户名及密码
# hostname = "192.168.1.4"
# port = 22
# username = "datapy"
# password = "datapy123"
hostname = "192.168.1.211"
port = 22
username = "yanzehao"
password = "QUNAzzss01"


class Paramiko_shh(object):
    def __init__(self):
        self.host = hostname
        self.port = port
        self.username = username
        self.password = password
        self.client = None

    def pk_connect(self):
        self.client = paramiko.SSHClient()  # 创建ssh对象
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 允许连接不在know_host中的主机
        self.client.connect(hostname=self.host, port=self.port, username=self.username, password=self.password, compress=True)  # 连接服务器
        try:
            return self.client.open_sftp()
        except Exception as e:
            print('Connect error:', e)
            exit()

    def paramiko_put(self, local_dir, remote_dir):
        '''远程上传文件'''
        sftp = self.pk_connect()
        files = os.listdir(local_dir)
        cnt = 0
        for file in files:
            sftp.put(os.path.join(local_dir, file), os.path.join(remote_dir, file))
        cnt += 1
        if cnt == len(files):
            print(str(cnt) + ' files put successful')
        else:
            print('put failure')

    def paramiko_get(self, local_dir, remote_dir):
        '''远程下载文件'''
        sftp = self.pk_connect()
        files = sftp.listdir(remote_dir)
        cnt = 0
        for file in files:
            sftp.get(os.path.join(remote_dir, file), os.path.join(local_dir, file))
        cnt += 1
        if cnt == len(files):
            print(str(cnt) + ' files get successful')
        else:
            print('get failure')

    def __del__(self):
        self.client.close()

if __name__ == '__main__':
    paramik = Paramiko_shh()
    sftp_client = paramik.pk_connect()
    dir_list = sftp_client.listdir('/home/data/collect-server')  # 读取文件夹
    print(dir_list)
    # 写文件信息
    with sftp_client.open('/home/datapy/zs_project/logs/base/stat/zs_srp_order_detail.log', 'r') as f:
        for line in f.readlines():
            print(line)