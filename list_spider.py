import requests
import threading
from openpyxl import Workbook
from setting import *

class ListSpider():
    def __init__(self, tasks):
        self.tasks = tasks

    def crawl(self, url):
        pass

    def clean(self, source):
        pass

    def save_rows(self, rows_data, ws):
        pass

    def save(self, wb):
        pass

    def run(self, task):
        wb = Workbook()
        ws = wb.active

        aid = TASK_TYPE_ID[task]
        page = 1
        while 1:
            url = f'http://www.c-whale.com/jsp/list?aid={aid}&bid=&row={page*10}'
            source = self.crawl(url)
            if source is None:
                break

            rows_data = self.clean(source)

            self.save_rows(rows_data, ws)

        self.save(wb)



    def start(self):
        pass





