import os
from multiprocessing import Process
import multiprocessing
from datasketch import MinHash
from utils import str_encode, max_size, get_all_files, number_to_string
import numpy as np
import jsonlines



# 计算该行的minhash值
def get_minhash_values(line, num_perm=128):
    m = MinHash(num_perm=num_perm)
    cuts = set(line)
    m.update_batch([cut.encode('utf8') for cut in cuts])

    return m







# 计算一个长为32的minhash值
def from_txt_to_json(file_path, num_perm=32):
    hashs = set()
    f = open(file_path, 'r')
    file_json = {'file_name': os.path.basename(file_path)[:-4],'hash_length': 0,'content': []}
    while True:
        line = f.readline()
        if not line:
            break

        line = line.strip()
        if len(line) == 0:
            continue
        h = hash(line)
        hashs.add(h)
        file_json['content'].append({'line_number': len(file_json['content']) + 1,
                                     'line_content': line,
                                     'is_redundant': False,
                                     'hash': h,
                                     'minhash': number_to_string(get_minhash_values(line, num_perm).hashvalues),
                                     'redundancy_in_this_file': 0,
                                     'redundancy_in_this_book': 0,
                                     'redundancy_in_global': 0
                                     })

    file_json['hash_length'] = len(hashs)

    return file_json


def run_process(file_path_queue, count, lock, dst_dir):
    lock.acquire()
    last_file_path = os.path.join(os.getcwd(), dst_dir, str(count.value) +'.jsonl')
    count.value += 1
    print(count.value)
    lock.release()

    file_to_write = jsonlines.open(last_file_path, mode='w')

    while not file_path_queue.empty():
        file_path = file_path_queue.get()

        one_json = from_txt_to_json(file_path)

        file_to_write.write(one_json)
        
        if os.path.getsize(last_file_path) >= max_size:
            file_to_write.close()
            lock.acquire()
            last_file_path = os.path.join(os.getcwd(), dst_dir, str(count.value) +'.jsonl')
            count.value += 1
            lock.release()
            file_to_write = jsonlines.open(last_file_path, mode='w')

    file_to_write.close()




def convert(src_dir, src='txt', dst='jsonl', dst_dir='converted/', n_process=4):
    assert os.path.exists(src_dir)
    assert src in ['txt', 'jsonl']
    assert dst in ['txt', 'jsonl']
    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)

    if src != 'txt' or dst != 'jsonl':
        raise NotImplementedError('Only support converting from txt to jsonl now.')

    file_path_queue = get_all_files(src_dir)

    lock = multiprocessing.Lock()
    count = multiprocessing.Value('i', 0)

    processes = []

    for _ in range(n_process):
        p = Process(target = run_process, args = (file_path_queue, count, lock, dst_dir))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()


if __name__ == '__main__':
    convert('/home/xiang/文档/20230124', n_process=multiprocessing.cpu_count())


