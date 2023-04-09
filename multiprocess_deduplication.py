import argparse
from utils import jaccard_distance, get_all_files
import tqdm
import multiprocessing
import customSimhash
import csv


def deduplication(file_path_list, simhash_threshold, similar_file_queue, flag):

    lsh = customSimhash.SimhashIndex([], f=64, k=simhash_threshold)

    for i in tqdm.tqdm(range(len(file_path_list))):
        with open(file_path_list[i], encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            row = next(reader)
            simhash_value = customSimhash.Simhash(int(row[3]))
            similar= lsh.add(file_path_list[i], simhash_value, return_similar=True)
            if(similar != ""):
                similar_file = [file_path_list[i], similar]
                similar_file_queue.put(similar_file)
    
    flag.value = True

def check_similar_file(similar_file_queue, jaccard_thresold, flag):
    while True:
        try:
            similar_file = similar_file_queue.get(timeout=0.2) 
        except:
            if(flag.value):
                break
            else:
                continue

        with open(similar_file[0], encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            md5_set1 = set(next(reader))
        with open(similar_file[1], encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            md5_set2 = set(next(reader))
        if(jaccard_distance(md5_set1, md5_set2) < jaccard_thresold):
            print(similar_file[0], similar_file[1],'jaccard相似度检查失败')
            print('相似度为', jaccard_distance(md5_set1, md5_set2))
        else:
            with open(similar_file[0], 'r+') as file:
                # 将第一个字符替换成'1'
                file.write('1')


def files_deplication(src_dir = 'output_csv/', simhash_threshold = 3, jaccard_thresold = 0.8 , n_process = 13):
    """
    将多个csv中的文件进行去重
    :param src_dir: csv文件路径
    :param simhash_threshold: 指定去重阈值，默认为3，也就是simhash值相差3以内算相似
    :param n_process: 指定进程数，最低是2，也就是一个主进程一个检验去重结果进程，默认是13
    """
    # 获取所有jsonl文件
    file_path_list, file_nums = get_all_files(src_dir, ['.csv'], 'list')
    similar_file_queue = multiprocessing.Queue(200)
    flag = multiprocessing.Value('b', False)
    for _ in range(n_process-1):
        p = multiprocessing.Process(target=check_similar_file, args=(similar_file_queue, jaccard_thresold, flag))
        p.start()

    deduplication(file_path_list, simhash_threshold, similar_file_queue, flag)


if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, default='output_csv', help="源文件夹路径")
    # 添加可选参数，指定去重阈值
    parser.add_argument('--simhash_threshold', type=int, default=3, help="指定simhash去重阈值，默认为3")
    # 添加可选参数，指定jaccard相似度阈值
    parser.add_argument('--jaccard_thresold', type=float, default=0.8, help="指定jaccard相似度阈值，默认为0.8")
    # 添加可选参数，指定进程数，默认为13
    parser.add_argument('--n_process', type=int, default=13, help="指定进程数，默认为13")
    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    files_deplication(args.src_dir, args.simhash_threshold, args.jaccard_thresold, args.n_process)