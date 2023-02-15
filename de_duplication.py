from utils import str_encode, max_size, get_all_files, string_to_number
import numpy as np
import os
import jsonlines
from multiprocessing import Process, Queue
import multiprocessing
import tempfile
import shutil
from datasketch import MinHash, MinHashLSH


# 检查文件是否有重复，windows_size是窗口大小，对滑窗内所有hash求和，如果有重复，就将滑窗内的所有行都标记为重复
# 认为连续的重复行是需要去重的，因此需要滑窗
def check(file_json, windows_size):
    # 若文件中的hash值重复度不小于10%，则进行去重
    if file_json['hash_length']/(len(file_json['content'])) < 0.9:
        # 滑窗内所有hash求和
        hashs_sum_each_windows_size = []
        for i in range(len(file_json['content'])-windows_size):
            hashs_sum_each_windows_size.append(
                sum([file_json['content'][j]['hash'] for j in range(i, i+windows_size)]))
        # 若滑窗内所有hash求和的重复度不小于10%，则进行去重
        sets = set(hashs_sum_each_windows_size)
        if len(hashs_sum_each_windows_size) > 0 and len(sets)/len(hashs_sum_each_windows_size) < 0.9:
            for tmpset in sets:
                if hashs_sum_each_windows_size.count(tmpset) > 1:
                    indices = [i for i, x in enumerate(
                        hashs_sum_each_windows_size) if x == tmpset][1:]
                    for index in indices:
                        for i in range(index, index+windows_size):
                            # 标记为软删除
                            file_json['content'][i]['is_redundant'] = True
                            # file_json['content'][i]['redundancy_in_this_file'] += 1
    return file_json


def run_process(file_path_queue, windows_size):
    while not file_path_queue.empty():
        file_path = file_path_queue.get()

        with jsonlines.open(file_path, 'r') as reader:
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
                for file_json in reader:

                    check(file_json, windows_size=windows_size)

                    jsonlines.Writer(temp).write(file_json)
        print('finish', file_path)
        # 将临时文件覆盖原始文件
        shutil.move(temp.name, file_path)


# 去重函数，src_dir是输入文件夹，里面应该是jsonl格式文件，n_process是进程数，通常建议设置为cpu核数-1
def remove_duplicates(src_dir, n_process=4, windows_size=5):
    assert os.path.exists(src_dir)
    file_path_queue = get_all_files(src_dir, legal_file_type=['.jsonl'])

    processes = []
    for _ in range(n_process):
        p = Process(target=run_process, args=(file_path_queue, windows_size))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
