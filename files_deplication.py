# import customSimhash
# import jsonlines, multiprocessing
# import argparse
# import os, tqdm, sys
# from utils import get_all_files, max_memory
# import csv
# import logging
# logger = logging.getLogger('simhash_no_debug')
# logger.setLevel(logging.ERROR)
# from itertools import groupby
# import time,math
# from random import shuffle
# import collections 

# def files_deplication_one_csv(src, threshold):
#     """
#     一个csv文件内部两两比较
#     param src: csv文件路径
#     param threshold: simhash的阈值
#     """
#     index = simhash.SimhashIndex([], k=threshold, log=logger)
#     path_simhashs = []
#     all_similar_files = []
#     num = 0
#     with open(src, encoding="utf-8") as csvfile:
#         reader = csv.reader(csvfile)
#         next(reader)
#         for row in reader:
#             simhash_value = simhash.Simhash(int(row[2]))
#             index.add(row[0]+row[1], simhash_value)
#             path_simhashs.append((row[0]+row[1], simhash_value))
#             num += 1

#     poped_keys = []
#     i = 0
#     for _ in tqdm.tqdm(range(num)):
#         key, one_simhash = path_simhashs.pop()

#         if key in poped_keys:
#             continue
#         # 获取相似文件
#         similar_files = index.get_near_dups(one_simhash)
#         if len(similar_files) == 1:
#             continue
#         i += 1

#         poped_keys.extend([similar_file for similar_file in similar_files])
#         all_similar_files.extend([{
#                 'csv文件': src,
#                 '来源': similar_file.split(".jsonl")[0]+'.jsonl',
#                 '文件名': similar_file.split(".jsonl")[1],
#                 '重复ID' : i,
#             } for similar_file in similar_files])

#     return all_similar_files

# # 两个csv文件比较
# def files_deplication_two_csv(src1, src2, threshold):
#     """
#     两个csv文件之间比较，我们认为源csv文件不需要与被别人去重
#     param src1: 源csv文件路径
#     param src2: 需要被去重csv文件路径
#     param threshold: simhash的阈值
#     """
#     index = simhash.SimhashIndex([], k=threshold, log=logger)
#     all_similar_files = []
#     file_name_list = []

#     with open(src1, encoding="utf-8") as csvfile:
#         reader = csv.reader(csvfile)
#         next(reader)
#         for row in reader:
#             simhash_value = simhash.Simhash(int(row[2]))
#             index.add(row[0]+row[1], simhash_value)
#             file_name_list.append(row[1])

#     i = 0
#     start = time.time() 
#     with open(src2, encoding="utf-8") as csvfile:
#         reader = csv.reader(csvfile)
#         next(reader)
#         for row in reader:
            
#             simhash_value = simhash.Simhash(int(row[2]))
#             similar_files = index.get_near_dups(simhash_value)
#             if len(similar_files) == 0:
#                 continue
#             i += 1
#             all_similar_files.append({
#                 'csv文件': src2,
#                 '来源': row[0],
#                 '文件名': row[1],
#                 '重复ID' : i,
#                 'md5s': {int(md5, 16) for md5 in row[3:]}
#                 })
#             all_similar_files.append({
#                 'csv文件': src1,
#                 '来源': similar_files[0].split(".jsonl")[0]+'.jsonl',
#                 '文件名':   similar_files[0].split(".jsonl")[1],
#                 '重复ID' : i,
#                 })

#     end = time.time()
#     print(end-start)
#     return all_similar_files

# def similar_files_check(all_similar_files, jaccard_threshold, csv_src):
#     """
#     检查相似文件是否真的相似
#     param all_similar_files: 所有相似文件
#     param jaccard_threshold: jaccard距离阈值
#     param csv_src: 需要再次读取的csv文件路径
#     """
#     print(len(all_similar_files))
#     groupby_similar_files_by_csv = groupby(sorted(all_similar_files, key=lambda x: x['csv文件']), key=lambda x: x['csv文件'])
#     all_similar_files_with_md5s = []
#     for one_csv_src, group_files_by_csv in groupby_similar_files_by_csv:
#         group_files_by_csv = list(group_files_by_csv)
#         if one_csv_src != csv_src:
#             all_similar_files_with_md5s.extend(group_files_by_csv)
#         else:
#             file_name_list = [file['文件名'] for file in group_files_by_csv]
            
#             with open(csv_src, encoding="utf-8") as csvfile:
#                 reader = csv.reader(csvfile)
#                 next(reader)
#                 bar = tqdm.tqdm(total=len(file_name_list))
#                 for row in reader:
#                     if row[1] in file_name_list:
#                         bar.update(1)
#                         pos = file_name_list.index(row[1])
#                         group_files_by_csv[pos]['md5s'] = {int(md5, 16) for md5 in row[3:]}
#                         all_similar_files_with_md5s.append(group_files_by_csv[pos])

#                 bar.close()


#     groupby_all_similar_files = groupby(sorted(all_similar_files_with_md5s, key=lambda x: x['重复ID']), key=lambda x: x['重复ID'])
#     deplicate_files = []
#     doubtful_deplicate_files = []

#     for key, group in groupby_all_similar_files:
#         doubtful = False
#         group = list(group)
#         for i in range(len(group)):
#             for j in range(i+1, len(group)):
#                 if jaccard_distance(group[i]['md5s'], group[j]['md5s']) <= jaccard_threshold:
#                     print(group[i]['文件名'], group[j]['文件名'], jaccard_distance(group[i]['md5s'], group[j]['md5s']))
#                     doubtful = True
#                     break

#         if doubtful:
#             doubtful_deplicate_files.extend([{k: v for k, v in d.items() if k != 'md5s'} for d in group])
#         else:
#             deplicate_files.extend([{k: v for k, v in d.items() if k != 'md5s'} for d in group])

#     return deplicate_files, doubtful_deplicate_files

# def run_process(file_path_queue, in_queue, out_queue, simhash_threshold):
#     while True:
#         index = customSimhash.SimhashIndex([], k=simhash_threshold, log=logger)
#         while not file_path_queue.empty():
#             try:
#                 file_path = file_path_queue.get(timeout=0.2)
#             except:
#                 break
#             changed = False
#             with open(file_path, 'r') as csvfile:
#                 reader = csv.reader(csvfile)
#                 data = list(reader)
#                 simhash_value = customSimhash.Simhash(int(data[1][1]))
#                 file = index.add(file_path, simhash_value)
#                 if file != '' and data[1][2] != 'True':
#                     data[1][2] = True
#                     data[1][3] = file
#                     changed = True
#             if changed:
#                 with open(file_path, 'w', newline='') as csvfile:
#                     writer = csv.writer(file)
#                     writer.writerows(data)
#             if sys.getsizeof(index) > max_memory:
#                 break
        
#         out_queue.put(True)

#         while True:
#             file = in_queue.get()
#             if file == True:
#                 break
#             if 'simhash' in file.keys():
#                 simhash_value = customSimhash.Simhash(int(file['simhash']))
#                 file = index.get_near_dups(simhash_value)
#                 if file != '':
#                     file['是否重复'] = True
#                     out_queue.put(file)
                
#                     with open(file['file_path'], 'r') as csvfile:
#                         reader = csv.reader(csvfile)
#                         data = list(reader)

#                         if file != '' and data[1][2] != 'True':
#                             data[1][2] = True
#                             data[1][3] = file
#                             changed = True
#                     if changed:
#                         with open(file_path, 'w', newline='') as csvfile:
#                             writer = csv.writer(file)
#                             writer.writerows(data)



# def files_deplication(src_dir, simhash_threshold, n_process):
#     """
#     将多个csv中的文件进行去重
#     :param src_dir: csv文件路径
#     :param threshold: 指定去重阈值，默认为5，也就是simhash值相差5以内算相似
#     :return:
#     """

#     # 获取源文件列表，在这里可以先获取目录列表，根据目录列表来获取文件列表，这样可以更加灵活，避免内存不足
#     file_path_queue, file_nums = get_all_files(src_dir, ['.csv'])

#     in_queues = [multiprocessing.Queue() for i in range(n_process)]
#     out_queue = multiprocessing.Queue()

#     processes = []
#     for i in range(n_process):
#         p = multiprocessing.Process(target=run_process, args=(file_path_queue, in_queues[i], out_queue, simhash_threshold))
#         p.start()
#         processes.append(p)

#     while True:
#         # 返回内部去重结束的标志
#         for i in range(n_process):
#             out_queue.get()

#         # 某一轮内部去重结束，已经没有剩余文件了，就退出
#         if file_path_queue.empty():
#             break
        
#         file_nums = 0
#         while not file_path_queue.empty():
#             file = {
#                 'file_path': file_path_queue.get(),
#                 'list' : shuffle(list(range(n_process))),
#                 '是否重复': False
#             }
#             in_queues[file['list'].pop()].put(file)
#             file_nums += 1

#         i = 0
#         while i < file_nums:
#             file = out_queue.get()
#             if file['是否重复'] or len(file['list'])==0:
#                 file_path_queue.put(file['file_path'])
#                 i += 1
#             else:
#                 in_queues[file['list'].pop()].put(file)

#         # 告知子进程，对未处理的文件与进程内匹配这一步结束
#         for q in in_queues:
#             q.put(True)

#         # 子进程内部数据两两合并，每个数据由out_queue返回
#         num = n_process
#         while True:
#             for i in range(num):
#                 in_queues[i//2].put(out_queue.get())
#             if i % 2 == 0:
#                 in_queues[i//2].put(collections.defaultdict(set))

#             num = math.ceil(i/2)
#             if num==1:
#                 # 把最后一轮合并后的返回
#                 out_queue.get()
#                 break




# if __name__ == '__main__':
#     # 设置参数解析器
#     parser = argparse.ArgumentParser()
#     # 添加必须指定的参数
#     parser.add_argument('--src_dir', type=str, required=True, help="源文件夹路径")
#     # 添加可选参数，指定去重阈值
#     parser.add_argument('--simhash_threshold', type=int, default=5, help="指定simhash去重阈值，默认为5")
#     # 添加可选参数，指定进程数，默认为1
#     parser.add_argument('--n_process', type=int, default=1, help="指定进程数，默认为1")
#     # 解析参数
#     args = parser.parse_args()
#     # 调用convert函数
#     files_deplication(args.src_dir, args.simhash_threshold, args.n_process)
