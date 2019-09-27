# -*- coding: utf-8 -*-
# @Time : 2019/4/1
# @Author : zxy
import asyncio
import aiohttp
import async_timeout
from collections import namedtuple
from async_retrying import retry
import marshal
import random

# 并发数
CONCURRENCY_NUM = 5
# 重试数
MAX_RETRY_TIMES = 3
# 是否使用代理
USE_PROXY = False
# 请求超时
TIME_OUT = 10

# 响应格式
Response = namedtuple("Response", ["status", "source"])

try:
    # uvloop增加事件循环效率, 仅支持Linux环境
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

sem = asyncio.Semaphore(CONCURRENCY_NUM)

class AioRequest():
    def __init__(self):
        self.init_session()

    async def bound_get(self, url, _kwargs: dict = {}, source_type="text") -> Response:
        '''
        以限制并发数的方式请求
        :param url: 请求URL
        :param _kwargs: 支持所有requests的get参数
        :param source_type: 响应结果的资源类型 text/buff
        :param status_code: 响应结果的状态码,默认200
        :return:
        '''
        async with sem:
            res = await self.get(url, _kwargs, source_type)
            await asyncio.sleep(random.random())

            return res

    @retry(attempts=MAX_RETRY_TIMES)
    async def get(self, url, _kwargs: dict = {}, source_type="text") -> Response:
        '''
        异步请求
        :param url: 请求URL
        :param _kwargs: 支持所有requests的get参数
        :param source_type: 响应结果的资源类型 text/buff
        :param status_code: 响应结果的状态码,默认200
        :return:
        '''
        # 使用marshal复制kwargs，避免重试时kwargs无效
        kwargs = marshal.loads(marshal.dumps(_kwargs))

        if USE_PROXY:
            await self.get_proxy(kwargs)
        method = kwargs.pop("method", "get")
        timeout = kwargs.pop("timeout", TIME_OUT)
        with async_timeout.timeout(timeout):
            async with getattr(self.session, method)(url, **kwargs) as req:
                status = req.status
                if source_type == "text":
                    source = await req.text()
                elif source_type == "buff":
                    source = await req.read()

        res = Response(status=status, source=source)

        return res

    async def get_proxy(self, kwargs):
        '''
        获取代理
        '''
        pass

    def init_session(self):
        '''
        创建Tcpconnector，包括ssl和连接数的限制
        创建一个全局session。
        :return:
        '''
        self.tc = aiohttp.connector.TCPConnector(limit=300, force_close=True,
                                                 enable_cleanup_closed=True,
                                                 ssl=False)
        self.session = aiohttp.ClientSession(connector=self.tc)

    async def close(self):
        await self.tc.close()
        await self.session.close()
