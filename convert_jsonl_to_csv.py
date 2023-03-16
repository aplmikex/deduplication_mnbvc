import os
from multiprocessing import Process
import multiprocessing
import argparse
import tqdm
from utils import max_size, get_all_files
import jsonlines
import csv


def convert_jsonl_to_csv(src_dir, dst, dst_dir):

    # 如果输出目录不存在，则创建
    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)

    with open(os.path.join(dst_dir, dst), 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(('来源', '文件名', 'simhash', 'md5s'))

    # 获取所有jsonl文件
    file_path_list, file_nums = get_all_files(src_dir, ['.jsonl'], 'list')
    for i in tqdm.tqdm(range(file_nums)):
        with jsonlines.open(file_path_list[i]) as reader:
            for one_json in reader:
                with open(os.path.join(dst_dir, dst), 'a', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    row = [file_path_list[i], one_json['文件名'], one_json['simhash']]
                    row.extend({one_json['段落'][i]['md5'][8:-8] for i in range(len(one_json['段落']))})
                    writer.writerow(row)




if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="jsonl源文件夹路径")
    # 添加可选参数，指定目标文件类型，默认为jsonl
    parser.add_argument('--dst', type=str, default='result.csv', help="指定目标文件名称，默认result.csv")
    # 添加可选参数，指定转换后文件存放路径，默认为converted/
    parser.add_argument('--dst_dir', type=str, default='./', help="指定转换后文件存放路径，默认为./")

    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    convert_jsonl_to_csv(args.src_dir, args.dst, args.dst_dir)
