import os
import multiprocessing
import numpy as np

str_encode = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
max_size = 500 * 1024 * 1024 

# 递归读取文件夹下所有文件
def get_all_files(dir_path, legal_file_type=['.txt'], return_file_type='queue'):
    if return_file_type == 'queue':
        return get_all_files_queue(dir_path, legal_file_type)
    elif return_file_type == 'list':
        return get_all_files_list(dir_path, legal_file_type)


def get_all_files_queue(dir_path, legal_file_type=['.txt']):
    file_nums = 0
    file_path_queue = multiprocessing.Queue()
    for root, _, files in os.walk(dir_path):
        for file in files:
            if os.path.splitext(file)[-1] not in legal_file_type:
                continue
            file_path = os.path.join(root, file)
            file_path_queue.put(file_path)
            file_nums += 1
    return file_path_queue, file_nums


def get_all_files_list(dir_path, legal_file_type=['.txt']):
    file_path_list = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            if os.path.splitext(file)[-1] not in legal_file_type:
                continue
            file_path = os.path.join(root, file)
            file_path_list.append(file_path)
    return file_path_list, len(file_path_list)


def number_to_string(hashvalues, str_encode=str_encode, length_each_number=6):
    '''
    一个把数字列表转换成字符串的函数。
    根据RFC-3629，UTF-8编码的字符最多占用4个字节，所以每个数字最多占用6个字符。
    通常情况下，不需要考虑其他编码的情况。

    '''
    encoded_str = ''
    for hashvalue in hashvalues:
        for i in range(length_each_number):
            num = hashvalue % len(str_encode)
            hashvalue = hashvalue // len(str_encode)
            encoded_str += str(str_encode[int(num)])

    return encoded_str



def string_to_number(encoded_str, str_encode=str_encode, length_each_number=6):
    '''
    一个把字符串转换成numpy列表的函数。
    根据RFC-3629，UTF-8编码的字符最多占用4个字节，所以每个数字最多占用6个字符。
    通常情况下，不需要考虑其他编码的情况。

    '''
    hashvalues = np.empty(len(encoded_str)/length_each_number, dtype=np.uint64)
    for i in range(len(hashvalues)):
        for j in range(length_each_number):
            hashvalues[i] += str_encode.index(encoded_str[i*length_each_number+j]) * (len(str_encode) ** j)

    return hashvalues