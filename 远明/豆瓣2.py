
# 模块安装
import requests
from lxml import etree
import csv
import time
import random
'''

'''
ip = ['183.56.105.58:32589',
      '114.104.139.57:27936',
      '115.202.140.134:23752',
      '220.179.102.189:32593',
      '36.57.87.3:35902',
      '116.22.51.229:30883',
      '27.152.91.39:39738',
      '114.99.196.3:31651',
      '183.7.213.87:24778',
      '114.226.162.39:40157',
      '113.74.55.10:23786',
      '115.230.79.206:46715',
      '182.46.248.129:22145',
      '116.209.129.45:39105',
      '124.112.214.121:33853',
      '119.120.248.138:22542',
      '182.46.103.64:47024',
      '121.230.191.40:47916',
      '113.128.121.148:44561',
      '60.168.245.200:21470',
      '223.198.25.136:43853']

headers = {
    # 'cookie': 'll="108296"; bid=WQ8mtUXwuLc; douban-fav-remind=1; __utma=30149280.1088133017.1596610454.1596764881.1604567359.3; __utmz=30149280.1604567359.3.3.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmc=30149280; __utmt=1; dbcl2="160643101:hS5jyV5M8HU"; ck=xQAv; ap_v=0,6.0; push_doumail_num=0; push_noty_num=0; __utmv=30149280.16064; gr_user_id=bfbac667-62ea-44fc-90bf-8c6972f653e6; gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03=729e11bf-3849-437d-a632-f9398c69fa99; gr_cs1_729e11bf-3849-437d-a632-f9398c69fa99=user_id%3A1; __utmt_douban=1; gr_session_id_22c937bbd8ebd703f2d8e9445f7dfd03_729e11bf-3849-437d-a632-f9398c69fa99=true; _vwo_uuid_v2=DE7DA0D7DB16EA249493A5BD42949E213|428b29b57bd5dec81e5cde90a434de92; __gads=ID=037919e70f9000f9-2297054f92c400ef:T=1604567379:RT=1604567379:S=ALNI_MYOEu_uRmM3gPwIG3HwDaOFjS_mKQ; __utmb=30149280.8.10.1604567359',
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'}
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
}

# 书籍页面获取
def get_content(leixing,response):
    # try:
    D=[]
    items = etree.HTML(response)
    item = items.xpath('//*[@id="subject_list"]/ul/li')
    # print(item)
    for its in item:
        # 封面
        fengmian = its.xpath('./div[1]/a/img/@src')[0]
        # print(fengmian)
        # 书名
        shuming = its.xpath('./div[2]/h2/a/text()')[0]
        # 删除不需要的单元格
        shuming1 = shuming.replace('\n', '').replace('\t', '').strip()
        # 保存图片
        # save1(shuming1,fengmian)
        # print(shuming1)
        # 辅助列
        fuzhu = its.xpath('./div[2]/div[1]/text()')[0]
        fuzhu = fuzhu.replace('\n', '').replace('\t', '').strip().split('/')
        # print(fuzhu)
        # print(len(fuzhu))
        if len(fuzhu) == 5:
            guojia=fuzhu[0].split(']')[0].strip('[')
            # print(guojia)
            # 作者
            zuozhe = fuzhu[0]
            # 翻译人
            fanyi = fuzhu[1].strip()
            # 出版社
            chuban = fuzhu[2].strip()
            # 出版日期
            riqi = fuzhu[3].strip()
            # 价格
            jiage = fuzhu[4].strip()
            # print(zuozhe)
            # print(fanyi)
            # print(chuban)
            # print(riqi)
            # print(jiage)
        elif len(fuzhu) == 4:
            # 国家
            guojia='中'
            # 作者
            zuozhe = fuzhu[0]
            fanyi = ''
            # 出版社
            chuban = fuzhu[1].strip()
            # 出版社日期
            riqi = fuzhu[2].strip()
            # 价格
            jiage = fuzhu[3].strip()
        else:
            guojia=''
            zuozhe=''
            fanyi=''
            chuban=''
            riqi=''
            jiage=''
        guojia=guojia
        zuozhe=zuozhe
        fanyi=fanyi
        chuban=chuban
        riqi=riqi
        jiage=jiage
        # print(zuozhe)
        # print(fanyi)
        # print(chuban)
        # print(riqi)
        # print(jiage)
        # 评分
        try:
            pingfen = its.xpath('./div[2]/div[2]/span[2]/text()')[0]
        except:
            pingfen=''
        # print(pingfen)
        # 评价人数
        try:
            pingjiarenshu = its.xpath('./div[2]/div[2]/span[3]/text()')[0]
            pingjiarenshu=pingjiarenshu.replace('\n', '').replace('\t','').strip().strip('(').strip(')')
        except:
            pingjiarenshu=''
        # print(pingjiarenshu)
        # 书籍简介
        try:
            jianjie = its.xpath('./div[2]/p/text()')[0].replace('\n', '').replace('\t','')
        except:
            jianjie=''
        # print(jianjie)
        # try:
        data=[leixing,fengmian,shuming1,guojia,zuozhe,fanyi,chuban,riqi,jiage,pingfen,pingjiarenshu,jianjie]
        print(data)
        D.append(data)
    save(D)
    # except:
    #     pass


# 保存总数据
def save(data):
    with open('./书籍.csv', 'a', newline='', encoding='gb18030')as file:
        write = csv.writer(file)
        write.writerows(data)


def save1(shuming,data):
    response=requests.get(url=data,headers=headers)
    with open('./书籍图片/'+shuming+'.jpg','wb')as f:
        f.write(response.content)


def main():
    leixing='外国名著'
    li=['类型','封面','书名','国家','作者','翻译人','出版社','出版日期','价格','评分','评价人数','简介']
    with open('./书籍.csv', 'a', newline='', encoding='gb18030')as file:
        write = csv.writer(file)
        write.writerow(li)
    for i in range(0, 50):
        # time.sleep(1)
        # 链接
        # https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4?start=40&type=T
        lianjie1 = 'https://book.douban.com/tag/' + leixing + '?start={}&type=T'.format(i * 20)
        print(lianjie1)
        a = random.randint(1, 19)
        response = requests.get(url=lianjie1,proxies={"http":ip[a]}, headers=headers).text
        # print(response)
        get_content(leixing,response)
        time.sleep(2)


if __name__ == '__main__':
    main()