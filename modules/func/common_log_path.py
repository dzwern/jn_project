#-*-coding:utf-8-*-


import os


def commonPath(server='local'):
    '''统一公共日志路径'''
    if server == 'local':
        path = 'D:/logs/'
    elif server == 'base':
        path = '/home/datapy/'
    elif server == 'tensorflow':
        path = '/home/dluser/'
    elif server == 'aliyun':
        path = '/home/datacheck/'
    elif server == 'yun':
        path = '/Users/chracles/PycharmProjects/'
    else:
        raise KeyError('Invalid parameter ...')
    return path

def return_file_path(filePath):
    '''返回当前脚本对应的文件夹路径'''
    logPath = os.path.join(commonPath(), '/'.join(filePath.split('\\')[1:-1]), '/')
    return logPath

def return_log_path(filePath):
    '''返回当前文件日志对应的文件夹路径'''
    # 判断服务器和本地, 如果
    if '\\' in filePath:
        logPath = os.path.join(commonPath(), 'zs_project/logs/', '/'.join(filePath.split('\\')[1:-1]))
    else:
        logPath = os.path.join(commonPath(), 'zs_project/logs/', '/'.join(filePath.split('/')[3:-1]))

    return logPath


def make_logPath(filePath, fileName, fileType='.log'):
    '''传入执行文件的绝对路径和文件名，返回拼接后的日志路径'''
    # 判断服务器和本地, 如果
    if '\\' in filePath:
        logPath = os.path.join(commonPath(), 'zs_project/logs/', '/'.join(filePath.split('\\')[1:-1]))
        logName = os.path.join(logPath+'/', fileName.split('.')[0] + fileType)
        print('本地运行,创建日志文件:{}'.format(logName))
    else:
        logPath = os.path.join(commonPath(), 'zs_project/logs/', '/'.join(filePath.split('/')[3:-1]))
        logName = os.path.join(logPath+'/', fileName.split('.')[0] + fileType)
        print('服务器运行，创建日志文件：{}'.format(logName))

    logFloder = os.path.dirname(logName)

    if not os.path.exists(logFloder):
        try:
            os.makedirs(logFloder)
        except:
            print('未找到E盘，已经在D盘上创建指定目录，请检查！')

            logFloder = os.path.join('D:/', '/'.join(logFloder.split('/')[1:]))
            if not os.path.exists(logFloder):
                os.makedirs(logFloder)
                print('文件创建目录:{}'.format(logFloder))
            else:
                print('文件目录：{}已经创建'.format(logFloder))

            logName = os.path.join(logFloder, fileName.split('.')[0] + fileType)
    return logName

def check_path(model_path):
    '''检测目录是否存在，如果不存在就创建'''
    # 如果是文件，就截取文件夹目录，然后判断是否存在，不存在就创建
    if not os.path.isdir(model_path):
        logFloder = os.path.dirname(model_path)
        if not os.path.exists(logFloder):
            try:
                os.makedirs(logFloder)
            except:
                print('创建指定目录失败，请检查！')
    else:
        # 不是文件，判断文件夹是否存在
        if not os.path.exists(model_path):
            try:
                os.makedirs(model_path)
            except:
                print('创建指定目录失败，请检查！')