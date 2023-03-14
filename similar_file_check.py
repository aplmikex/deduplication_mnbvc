import simhash
import jsonlines
import argparse
import os, tqdm
from utils import get_all_files
from multiprocessing import Process, Queue
import collections
from itertools import groupby
import copy


def jaccard_distance(md5_list1, md5_list2):
    nominator = md5_list1.intersection(md5_list2)
    # 求集合 A 和集合 B 的并集
    denominator = md5_list1.union(md5_list2)
    # 计算比率
    similarity = len(nominator)/len(denominator)
    return similarity

def check(src, threshold):
    similar_files = []
    new_similar_files = []
    with jsonlines.open(src) as reader:
        for one_json in reader:
            similar_files.append(one_json)

    groupby_similar_files = groupby(similar_files, key=lambda x: x['来源'])
    
    tmp = copy.deepcopy(groupby_similar_files)


    for _ in tqdm.tqdm(range(len(dict(tmp)))):
        key, group = next(groupby_similar_files)

        group = list(group)
        file_name_lists = []
        for one_file in group:
            file_name_lists.append(one_file['文件名'])
        with jsonlines.open(key) as reader:
            for one_json in reader:
                if one_json['文件名'] in file_name_lists:
                    pos = file_name_lists.index(one_json['文件名'])
                    group[pos]['md5s'] = {one_json['段落'][i]['md5'] for i in range(len(one_json['段落']))}

        new_similar_files.extend(group)

    groupby_new_similar_files = groupby(new_similar_files, key=lambda x: x['重复ID'])
    print('groupby_new_similar_files')

    for key, group in groupby_new_similar_files:
        group = list(group)
        for i in range(len(group)):
            for j in range(i+1, len(group)):

                if jaccard_distance(group[i]['md5s'], group[j]['md5s']) <= threshold:
                    print(group[i]['文件名'], group[j]['文件名'], jaccard_distance(group[i]['md5s'], group[j]['md5s']))
                    group[i]['是否重复'] = True
                    group[j]['是否重复'] = True



if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src', type=str, default='result.jsonl', help="第一步去重后输出的jsonl文件")

    # 添加可选参数，指定去重阈值，默认为3
    parser.add_argument('--threshold', type=float, default=0.9, help="指定编辑距离阈值，默认为0.9")
    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    check(args.src, args.threshold)
