import traceback
import asyncio
import aiofiles
import re
import os
import time
import json
from aio_request import AioRequest
from lxml import etree
from setting import *
from urllib.parse import unquote

class Spider(AioRequest):
    def __init__(self, task_type, max_page):
        super().__init__()
        self.task_type = task_type
        self.max_page = max_page
        self.succeed = 0
        self.failed = []
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,und;q=0.7',
            'Connection': 'keep-alive',
            'Host': 'www.c-whale.com',
            'Referer': 'http://www.c-whale.com/index',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'
        }
        self.init_file_dir()

    def init_file_dir(self):
        self.dir_name = TASK_TYPE_NAME[self.task_type]
        if not os.path.exists(self.dir_name):
            os.mkdir(self.dir_name)

    def get_list_url(self):
        '''
        获取列表页url
        :return:
        '''
        url_list = []
        for page in range(1, self.max_page+1):
            row = page * 10
            aid = TASK_TYPE_ID[self.task_type]
            url = f'http://www.c-whale.com/jsp/list?aid={aid}&bid=&row={row}'
            url_list.append(url)

        return url_list

    def get_detail_url(self, source):
        url_list = []
        tree = etree.HTML(source)
        data_list = tree.xpath('//div[@class="newlist"]')
        for data in data_list:
            url = data.xpath('./a[@class="conn"]/@href')
            if url:
                url = f'http://www.c-whale.com/jsp{url[0].lstrip(".")}'
                url = unquote(url)

                r = re.search('tit=(.*)',url)
                title = unquote(r.group(1)) if r else ''

                url_list.append(
                    {
                        'url':url,
                        'title':title
                    }
                )

        return url_list

    def get_pdf_url(self, source):
        tree = etree.HTML(source)
        url = tree.xpath('//iframe/@src')
        if url:
            r = re.search('tit=(.*?).pdf',url[0])
            if r:
                url =f'http://www.c-whale.com/db/{r.group(1)}.pdf'
            else:
                url = 'None'
        else:
            url = 'None'

        return url

    def save_failed(self, code, msg, url, type):
        failed_info = {
            'code': code,
            'msg': msg,
            'url': url,
            'type': type
        }
        self.failed.append(failed_info)

    async def crawl_list(self, url):
        try:
            resp = await self.bound_get(url, _kwargs={'headers': self.headers})
            status, source = resp
            if status == 200:
                detail_url_list = self.get_detail_url(source)
                tasks = [asyncio.ensure_future(self.crawl_detail(url)) for url in detail_url_list]
                await asyncio.gather(*tasks)
            else:
                self.save_failed(status,'wrong status',url,'list_1')
        except:
            self.save_failed('',str(traceback.format_exc()),url,'list_2')

    async def crawl_detail(self, url_info):
        url = url_info['url']
        title = url_info['title']
        try:
            resp = await self.bound_get(url, _kwargs={'headers': self.headers})
            status, source = resp
            if status == 200:
                pdf_url = self.get_pdf_url(source)
                await self.crawl_pdf(pdf_url, title)
            else:
                self.save_failed(status, 'wrong status', url, 'detail_1')
        except:
            self.save_failed('', str(traceback.format_exc()), url, 'detail_2')

    async def crawl_pdf(self, url, title):
        try:
            resp = await self.bound_get(url, _kwargs={'headers': self.headers}, source_type='buff')
            status, source = resp
            if status == 200:
                await self.aio_save_pdf(title, source)
                self.succeed += 1
                print(f'下载成功({self.succeed}):{title}')
            else:
                self.save_failed(status, 'wrong status', url, 'pdf_1')
        except:
            self.save_failed('', str(traceback.format_exc()), url, 'pdf_2')

    async def aio_save_pdf(self, title, buff):
        file_name = f'{title}.pdf'
        file_name = re.sub(r'[\\/:*?"<>|]', '-', file_name)
        file_name = f'{self.dir_name}/{file_name}'
        async with aiofiles.open(file_name, 'wb') as f:
            await f.write(buff)

    def save_pdf(self, title, buff):
        file_name = f'{title}.pdf'
        file_name = re.sub(r'[\\/:*?"<>|]', '-', file_name)
        file_name = f'{self.dir_name}/{file_name}'
        with open(file_name, 'wb') as f:
            f.write(buff)

    def save_json(self):
        dir_name = 'log'
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)

        file_name =f'{dir_name}/{int(time.time()*1000)}.json'
        with open(file_name,'w') as f:
            json.dump(self.failed,f)

    async def run(self, url_list):
        '''
        运行
        '''
        try:
            tasks = [asyncio.ensure_future(self.crawl_list(url)) for url in url_list]
            await asyncio.gather(*tasks)

        except:
            print('运行错误:')
            print(traceback.format_exc())

        finally:
            await self.close()

    def start(self):
        '''
        开始
        '''
        print('开始'.center(100,'-'))
        start = int(time.time()*1000)

        url_list = self.get_list_url()
        total_num = len(url_list*10)
        print(f'待下载文件数:{total_num}')

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run(url_list))


        end = int(time.time()*1000)
        total_time = int((end - start) / 1000)

        print(f'\n下载完成, 成功数:{self.succeed} 失败数:{total_num-self.succeed} 总耗时:{total_time} 秒')
        print('结束'.center(100, '-'))

        self.save_json()





if __name__ == '__main__':
    print('任务类型\n0:申报通知\n1:立项公告\n2:管理文件\n3:表格模板')
    print('\n输入exit退出程序...')

    while 1:
        task_type = input('\n请选择任务类型:')
        if task_type == 'exit':
            break

        if task_type not in TASK_TYPE_NAME.keys():
            print('任务类型不存在,请重新选择')
            continue

        max_page = input('请选择最大页数:')
        try:
            max_page = int(max_page)
        except:
            print('最大页数必须为整数,请重新选择')
            continue

        s = Spider(task_type,max_page)
        s.start()


