import os
from openpyxl import Workbook
import time
import traceback


BASE_DATA_DIR = r'C:\Users\vigour\Desktop\gen_data_clean\source\Series GSE97693\data'


def file_to_dict(filepath):
    _, tempfilename = os.path.split(filepath)
    filename, _ = os.path.splitext(tempfilename)
    ret = {'name':filename}
    data = {}

    with open(filepath,'r') as f:
        for line in f.readlines():
            k,v = line.split()[0].strip(),line.split()[1].strip()
            try:
                if int(float(v)) == 0:
                    v = '0'
            except:
                pass

            data[k] = v

    ret['data'] = data

    return ret


def clean(file_dir):
    wb = Workbook()
    ws = wb.active

    file_list = os.listdir(f'{BASE_DATA_DIR}/{file_dir}')
    all_data = [file_to_dict(f'{BASE_DATA_DIR}/{file_dir}/{file}') for file in file_list]
    columns = [data['name'] for data in all_data]
    columns.insert(0,'')
    ws.append(columns)

    row_tags = all_data[0]['data'].keys()
    for tag in row_tags:
        row = [data['data'][tag] for data in all_data]
        row.insert(0,tag)
        ws.append(row)

    wb.save(f"{file_dir}.xlsx")


def clean_multi():
    source_dirs = os.listdir(BASE_DATA_DIR)
    print(f'开始清洗,文件夹数量:{len(source_dirs)}')

    for file_dir in source_dirs:
        print(f'正在清洗：{file_dir}')
        start = time.time()

        clean(file_dir)

        end = time.time()

        total = round(end - start,2)

        print(f'{file_dir} 清洗完成，耗时：{total} 秒')











if __name__ == '__main__':
    clean_multi()
    # clean('CRC_02_PT')
