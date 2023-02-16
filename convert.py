import os
from multiprocessing import Process
import multiprocessing
import argparse
import tqdm
# from datasketch import MinHash
from utils import str_encode, max_size, get_all_files, number_to_string
import numpy as np
import jsonlines
import hashlib
# from charset_mnbvc import api


# # 计算该行的minhash值
# def get_minhash_values(line, num_perm=128):
#     m = MinHash(num_perm=num_perm)
#     cuts = set(line)
#     m.update_batch([cut.encode('utf8') for cut in cuts])

#     return m


# 计算一个长为32的minhash值
def from_txt_to_json(file_path, threshold, num_perm=32):
    hashs = set()
    f = open(file_path, 'r', encoding='utf-8', errors='ignore')
    file_json = {'file_name': file_path,
                 'simhash':'',
                 'suspected': False,
                 'hash_length': 0,
                 'content': []}
    while True:
        line = f.readline()
        if not line:
            break

        line = line.strip()
        if len(line) == 0:
            continue
        h = hashlib.md5(line.encode()).hexdigest()
        hashs.add(h)
        file_json['content'].append({'line_number': len(file_json['content']) + 1,
                                     'line_content': line,
                                     'is_redundant': False,
                                     'hash': h,
                                    #  'minhash': number_to_string(get_minhash_values(line, num_perm).hashvalues),
                                     'redundancy_in_this_file': 0,
                                     'redundancy_in_this_book': 0,
                                     'redundancy_in_global': 0
                                     })

    file_json['hash_length'] = len(hashs)
    if len(hashs)/len(file_json['content']) < threshold:
        file_json['suspected'] = True

    return file_json


def run_process(file_path_queue, json_to_write_queue, threshold):

    while not file_path_queue.empty():
        file_path = file_path_queue.get()
        one_json = from_txt_to_json(file_path, threshold)
        json_to_write_queue.put(one_json)


def write_jsonl(json_to_write_queue, file_nums, dst_dir):
    count = 0
    last_file = jsonlines.open(os.path.join(dst_dir, str(count) + '.jsonl'), mode='w')
    problem_count = 0
    last_problem_file = jsonlines.open(os.path.join(dst_dir, 'problem_'+str(problem_count) + '.jsonl'), mode='w')
    
    for _ in tqdm.tqdm(range(file_nums)):
        one_json = json_to_write_queue.get()
        if one_json['suspected']:
            last_problem_file.write(one_json)
            if last_problem_file._fp.tell() > max_size:
                last_problem_file.close()
                problem_count += 1
                last_problem_file = jsonlines.open(os.path.join(dst_dir, 'problem_'+str(problem_count) + '.jsonl'), mode='w')
        else:
            last_file.write(one_json)
            if last_file._fp.tell() > max_size:
                last_file.close()
                count += 1
                last_file = jsonlines.open(os.path.join(dst_dir, str(count) + '.jsonl'), mode='w')

    last_file.close()

def convert(src_dir, src='txt', dst='jsonl', dst_dir='converted/', n_process = 4, threshold = 0.95):
    assert os.path.exists(src_dir)
    assert src in ['txt', 'jsonl']
    assert dst in ['txt', 'jsonl']
    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)

    if src != 'txt' or dst != 'jsonl':
        raise NotImplementedError('Only support converting from txt to jsonl now.')

    file_path_queue, file_nums = get_all_files(src_dir)
    json_to_write_queue = multiprocessing.Queue(200)

    processes = []

    for _ in range(n_process):
        p = Process(target = run_process, args = (file_path_queue, json_to_write_queue, threshold))
        p.start()
        processes.append(p)
    
    write_jsonl(json_to_write_queue, file_nums, dst_dir)
    for p in processes:
        p.join()




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_dir', type=str, default='data/')
    parser.add_argument('--src', type=str, default='txt')
    parser.add_argument('--dst', type=str, default='jsonl')
    parser.add_argument('--dst_dir', type=str, default='converted/')
    parser.add_argument('--n_process', type=int, default=4)
    parser.add_argument('--threshold', type=float, default=0.95)
    args = parser.parse_args()
    convert(args.src_dir, args.src, args.dst, args.dst_dir, args.n_process, args.threshold)