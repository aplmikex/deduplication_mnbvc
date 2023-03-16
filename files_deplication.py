import simhash
import jsonlines
import argparse
import os, tqdm
from utils import get_all_files, jaccard_distance
import csv
csv.field_size_limit(100000000)
import logging
logger = logging.getLogger('simhash_no_debug')
logger.setLevel(logging.ERROR)
from itertools import groupby
import time

def files_deplication_one_csv(src, threshold):
    """
    一个csv文件内部两两比较
    param src: csv文件路径
    param threshold: simhash的阈值
    """
    index = simhash.SimhashIndex([], k=threshold, log=logger)
    path_simhashs = []
    all_similar_files = []
    num = 0
    with open(src, encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            simhash_value = simhash.Simhash(int(row[2]))
            index.add(row[0]+row[1], simhash_value)
            path_simhashs.append((row[0]+row[1], simhash_value))
            num += 1

    poped_keys = []
    i = 0
    for _ in tqdm.tqdm(range(num)):
        key, one_simhash = path_simhashs.pop()

        if key in poped_keys:
            continue
        # 获取相似文件
        similar_files = index.get_near_dups(one_simhash)
        if len(similar_files) == 1:
            continue
        i += 1

        poped_keys.extend([similar_file for similar_file in similar_files])
        all_similar_files.extend([{
                'csv文件': src,
                '来源': similar_file.split(".jsonl")[0]+'.jsonl',
                '文件名': similar_file.split(".jsonl")[1],
                '重复ID' : i,
            } for similar_file in similar_files])

    return all_similar_files

# 两个csv文件比较
def files_deplication_two_csv(src1, src2, threshold):
    """
    两个csv文件之间比较，我们认为源csv文件不需要与被别人去重
    param src1: 源csv文件路径
    param src2: 需要被去重csv文件路径
    param threshold: simhash的阈值
    """
    index = simhash.SimhashIndex([], k=threshold, log=logger)
    all_similar_files = []
    file_name_list = []

    with open(src1, encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            simhash_value = simhash.Simhash(int(row[2]))
            index.add(row[0]+row[1], simhash_value)
            file_name_list.append(row[1])

    i = 0
    start = time.time() 
    with open(src2, encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            
            simhash_value = simhash.Simhash(int(row[2]))
            similar_files = index.get_near_dups(simhash_value)
            if len(similar_files) == 0:
                continue
            i += 1
            all_similar_files.append({
                'csv文件': src2,
                '来源': row[0],
                '文件名': row[1],
                '重复ID' : i,
                'md5s': {int(md5, 16) for md5 in row[3:]}
                })
            all_similar_files.append({
                'csv文件': src1,
                '来源': similar_files[0].split(".jsonl")[0]+'.jsonl',
                '文件名':   similar_files[0].split(".jsonl")[1],
                '重复ID' : i,
                })

    end = time.time()
    print(end-start)
    return all_similar_files

def similar_files_check(all_similar_files, jaccard_threshold, csv_src):
    """
    检查相似文件是否真的相似
    param all_similar_files: 所有相似文件
    param jaccard_threshold: jaccard距离阈值
    param csv_src: 需要再次读取的csv文件路径
    """
    print(len(all_similar_files))
    groupby_similar_files_by_csv = groupby(sorted(all_similar_files, key=lambda x: x['csv文件']), key=lambda x: x['csv文件'])
    all_similar_files_with_md5s = []
    for one_csv_src, group_files_by_csv in groupby_similar_files_by_csv:
        group_files_by_csv = list(group_files_by_csv)
        if one_csv_src != csv_src:
            all_similar_files_with_md5s.extend(group_files_by_csv)
        else:
            file_name_list = [file['文件名'] for file in group_files_by_csv]
            
            with open(csv_src, encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                bar = tqdm.tqdm(total=len(file_name_list))
                for row in reader:
                    if row[1] in file_name_list:
                        bar.update(1)
                        pos = file_name_list.index(row[1])
                        group_files_by_csv[pos]['md5s'] = {int(md5, 16) for md5 in row[3:]}
                        all_similar_files_with_md5s.append(group_files_by_csv[pos])

                bar.close()


    groupby_all_similar_files = groupby(sorted(all_similar_files_with_md5s, key=lambda x: x['重复ID']), key=lambda x: x['重复ID'])
    deplicate_files = []
    doubtful_deplicate_files = []

    for key, group in groupby_all_similar_files:
        doubtful = False
        group = list(group)
        for i in range(len(group)):
            for j in range(i+1, len(group)):
                if jaccard_distance(group[i]['md5s'], group[j]['md5s']) <= jaccard_threshold:
                    print(group[i]['文件名'], group[j]['文件名'], jaccard_distance(group[i]['md5s'], group[j]['md5s']))
                    doubtful = True
                    break

        if doubtful:
            doubtful_deplicate_files.extend([{k: v for k, v in d.items() if k != 'md5s'} for d in group])
        else:
            deplicate_files.extend([{k: v for k, v in d.items() if k != 'md5s'} for d in group])

    return deplicate_files, doubtful_deplicate_files



def files_deplication(srcs, simhash_threshold, jaccard_threshold, step, similarfiles_output):
    """
    将多个csv中的文件进行去重
    :param src: csv文件路径
    :param threshold: 指定去重阈值，默认为8，也就是simhash值相差8以内算相似
    :param step: 1: 对每个csv文件内部进行去重，2: 对多个csv文件之间进行去重
    :return:
    """
    assert step in [1, 2]
    for src in srcs:
        assert os.path.exists(src)

    if step == 1:
        for src in srcs:
            all_similar_files = files_deplication_one_csv(src, simhash_threshold)
            deplicate_files, doubtful_deplicate_files = similar_files_check(all_similar_files, jaccard_threshold, src)

            with open(similarfiles_output, 'a', encoding='utf-8') as f:
                jsonlines.Writer(f).write_all(deplicate_files)
    else:
        assert len(srcs) == 2
        all_similar_files = files_deplication_two_csv(srcs[0], srcs[1], simhash_threshold)
        deplicate_files, doubtful_deplicate_files = similar_files_check(all_similar_files, jaccard_threshold, srcs[0])

        with open(similarfiles_output, 'a', encoding='utf-8') as f:
            jsonlines.Writer(f).write_all(deplicate_files)




if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--srcs', nargs='+', help="请输入csv文件")

    parser.add_argument('--step', type=int, required=True, help="step=1: 对每个csv文件内部进行去重，step=2: 对两个csv文件之间进行去重")

    # 添加可选参数，指定去重阈值
    parser.add_argument('--simhash_threshold', type=int, default=8, help="指定simhash去重阈值，默认为8")

    # 添加可选参数，指定去重阈值
    parser.add_argument('--jaccard_threshold', type=float, default=0.85, help="指定jaccard二次检验阈值，默认为0.85")

    parser.add_argument('--similarfiles_output', required=True, help="输出相似文件的csv文件路径")

    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    files_deplication(args.srcs, args.simhash_threshold, args.jaccard_threshold, args.step, args.similarfiles_output)
