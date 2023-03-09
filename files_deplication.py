import simhash
import jsonlines
import argparse
import os, tqdm
from utils import get_all_files
import pickle
from multiprocessing import Process, Queue
import collections

def run_process(file_path_queue, get_index_bucket_queue, threshold):
    index = simhash.SimhashIndex([], k=threshold)
    path_simhashs = []
    while not file_path_queue.empty():
        file_path = file_path_queue.get()
        with jsonlines.open(file_path) as reader:
            for one_json in reader:
                # 获取simhash值
                one_simhash = simhash.Simhash(one_json['simhash'])
                path_simhashs.append((file_path+one_json['文件名'], one_simhash))
                index.add(file_path+one_json['文件名'], one_simhash)

    get_index_bucket_queue.put((index.bucket, path_simhashs))




def files_deplication(src_dir, threshold, n_process, load_simhash_dict, save_simhash_dict):
    # 认为src_dir是一个文件夹，里面存放着待检测的jsonl文件
    # 如果不load_simhash_dict，就从头开始计算simhash值，计算src文件内部两两之间的相似度
    # 如果load_simhash_dict，就从load_simhash_dict中读取simhash值，计算src文件内部两两之间的相似度和src文件与load_simhash_dict中的文件之间的相似度

    # 检查输入参数是否合理
    assert os.path.exists(src_dir)
    
    index = simhash.SimhashIndex([], k=threshold)

    get_index_bucket_queue = Queue()
    path_simhashs = []

    if load_simhash_dict != '':
        with open(load_simhash_dict, 'rb') as f:
            index.bucket = pickle.load(f)


    file_path_queue, file_nums = get_all_files(src_dir, legal_file_type=['.jsonl'])



    processes = []
    for _ in range(n_process):
        p = Process(target=run_process, args=(file_path_queue, get_index_bucket_queue, threshold))
        p.start()
        processes.append(p)

    for _ in tqdm.tqdm(range(n_process), desc='读取jsonl文件'):
        return_value = get_index_bucket_queue.get()
        for key, value in return_value[0].items():
            index.bucket[key] |= value
        
        path_simhashs.extend(return_value[1])


    for p in processes:
        p.join()

    if save_simhash_dict != '':
        with open(save_simhash_dict, 'wb') as f:
            pickle.dump(index.bucket, f)


    poped_keys = []
    for key, one_simhash in path_simhashs:
        if key in poped_keys:
            continue
        # 获取相似文件
        similar_files = index.get_near_dups(one_simhash)

        # 如果相似文件有在load的字典中的，就对src赶尽杀绝，全标记为重复文件
        # 如果相似文件都不在load的字典中的，就留下第一个，其他的都标记为重复文件

        if len(similar_files) == 1:
            continue

        similar_json = {
            '来源': key.split(".jsonl")[0]+'.jsonl',
            '文件名': key.split(".jsonl")[1],
            '是否重复文件': False,
            '相似文件': []
        }

        similar_files.remove(key)


        for similar_file in similar_files:
            split_keys = similar_file.split(".jsonl")
            if len([path_simhash for path_simhash in path_simhashs if path_simhash[0] == similar_file])==1:
                similar_json['相似文件'].append({
                    '来源': split_keys[0]+'.jsonl',
                    '文件名': split_keys[1],
                    '是否重复文件': True
                })
                poped_keys.append(similar_file)
            else:
                # 如果有一个相似文件在load的字典中，就不保留当前的文件
                similar_json['是否重复文件'] = True

        with jsonlines.open('result.jsonl', mode='a') as file:
            file.write(similar_json)


    # for file_path in file_path_list:
    #     if file_path in result_dict:
    #         with jsonlines.open(file_path) as reader:
    #             with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
    #                 for one_json in reader:
    #                     if one_json['文件名'] in result_dict[file_path].keys():
    #                         one_json['是否重复文件'] = True
    #                     jsonlines.Writer(temp).write(one_json)
    #         shutil.move(temp.name, file_path)





if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="源文件夹路径")
    # 添加可选参数，指定进程数，默认为4
    # parser.add_argument('--n_process', type=int, default=4, help="指定进程数，默认为4")
    parser.add_argument('--load_simhash_dict', type=str, default='', help="是否加载simhash值，默认为不加载")
    parser.add_argument('--save_simhash_dict', type=str, default='', help="是否保存simhash值，默认为不保存")

    # 添加可选参数，指定进程数，默认为4
    parser.add_argument('--n_process', type=int, default=4, help="指定进程数，默认为4")

    # 添加可选参数，指定去重阈值，默认为3
    parser.add_argument('--threshold', type=int, default=5, help="指定去重阈值，默认为5，也就是simhash值相差3以内算相似")
    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    files_deplication(args.src_dir, args.threshold, args.n_process, args.load_simhash_dict, args.save_simhash_dict)
