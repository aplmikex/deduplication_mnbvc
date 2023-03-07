import simhash, shutil, tempfile
import jsonlines
import argparse
import os, tqdm
from utils import get_all_files
import pickle
# import multiprocessing
import collections

def files_deplication(src_dir, threshold, load_simhash_dict, save_simhash_dict):
    # 认为src_dir是一个文件夹，里面存放着待检测的jsonl文件
    # 如果不load_simhash_dict，就从头开始计算simhash值，计算src文件内部两两之间的相似度
    # 如果load_simhash_dict，就从load_simhash_dict中读取simhash值，计算src文件内部两两之间的相似度和src文件与load_simhash_dict中的文件之间的相似度

    # 检查输入参数是否合理
    assert os.path.exists(src_dir)
    
    index = simhash.SimhashIndex([], k=threshold)
    if load_simhash_dict != '':
        with open(load_simhash_dict, 'rb') as f:
            index.bucket = pickle.load(f)


    file_path_list, file_nums = get_all_files(src_dir, legal_file_type=['.jsonl'], return_file_type='list')
    path_simhashs = {}
    # 定义一个SimhashIndex对象
    for i in tqdm.tqdm(range(file_nums), desc='读simhash值'):
        file_path = file_path_list[i]
        with jsonlines.open(file_path) as reader:
            for one_json in reader:
                # 获取simhash值
                one_simhash = simhash.Simhash(one_json['simhash'])
                path_simhashs[file_path+one_json['文件名']] = one_simhash
                index.add(file_path+one_json['文件名'], one_simhash)
    
    if save_simhash_dict != '':
        with open(save_simhash_dict, 'wb') as f:
            pickle.dump(index.bucket, f)

    result_dict = collections.defaultdict(dict)

    poped_keys = []
    for key, one_simhash in path_simhashs.items():
        if key in poped_keys:
            continue
        # 获取相似文件
        similar_files = index.get_near_dups(one_simhash)

        # 如果相似文件有在load的字典中的，就对src赶尽杀绝，全标记为重复文件
        # 如果相似文件都不在load的字典中的，就留下第一个，其他的都标记为重复文件

        similar_files.remove(key)
        if len(similar_files) > 0:
            print(similar_files)
        # 默认保留当前的文件
        whether_to_keep_key = True

        for similar_file in similar_files:
            split_keys = similar_file.split(".jsonl")
            if split_keys[0]+'.jsonl' in file_path_list:
                result_dict[split_keys[0]+'.jsonl'][split_keys[1]] = path_simhashs[key] 
                poped_keys.append(similar_file)
            else:
                # 如果有一个相似文件在load的字典中，就不保留当前的文件
                whether_to_keep_key = False

        if not whether_to_keep_key:
            result_dict[key.split(".jsonl")[0]+'.jsonl'][key.split(".jsonl")[1]] = path_simhashs[key] 
            poped_keys.append(key)


    for file_path in file_path_list:
        if file_path in result_dict:
            with jsonlines.open(file_path) as reader:
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
                    for one_json in reader:
                        if one_json['文件名'] in result_dict[file_path].keys():
                            one_json['是否重复文件'] = True
                        jsonlines.Writer(temp).write(one_json)
            shutil.move(temp.name, file_path)





if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="源文件夹路径")
    # 添加可选参数，指定进程数，默认为4
    # parser.add_argument('--n_process', type=int, default=4, help="指定进程数，默认为4")
    parser.add_argument('--load_simhash_dict', type=str, default='', help="是否加载simhash值，默认为不加载")
    parser.add_argument('--save_simhash_dict', type=str, default='', help="是否保存simhash值，默认为不保存")
    # 添加可选参数，指定去重阈值，默认为3
    parser.add_argument('--threshold', type=int, default=3, help="指定去重阈值，默认为3，也就是simhash值相差3以内算相似")
    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    files_deplication(args.src_dir, args.threshold, args.load_simhash_dict, args.save_simhash_dict)
