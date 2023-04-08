import os
import argparse
import tqdm
from utils import get_all_files
import jsonlines
import csv
import hashlib

def convert_jsonl_to_csv(src_dir, dst_dir):

    # 如果输出目录不存在，则创建
    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)

    # 获取所有jsonl文件
    file_path_list, file_nums = get_all_files(src_dir, ['.jsonl'], 'list')

    for i in tqdm.tqdm(range(file_nums)):
        with jsonlines.open(file_path_list[i]) as reader:
            for one_json in reader:
                file_name = hashlib.md5(file_path_list[i].encode('utf-8')).hexdigest() + hashlib.md5(one_json['文件名'].encode('utf-8')).hexdigest() + '.csv'
                with open(os.path.join(dst_dir, file_name), 'w', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    row = [0, file_path_list[i], one_json['文件名'], one_json['simhash']]
                    writer.writerow(row)
                    md5s = {one_json['段落'][i]['md5'][8:-8] for i in range(len(one_json['段落']))}
                    writer.writerow(md5s)


###
# @description: 将jsonl文件转换为csv文件
# @param src_dir: jsonl源文件夹路径
# @param dst_dir: 转换后文件存放路径
# 输出csv，第一行第一个是是否重复，第二个是jsonl文件名，第三个是txt文件名，第四个是simhash
# 第二行是md5集合
###

if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="jsonl源文件夹路径")
    # 添加可选参数，指定转换后文件存放路径，默认为converted/
    parser.add_argument('--dst_dir', type=str, default='output_csv/', help="指定转换后文件存放路径，默认为 output_csv/")

    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    convert_jsonl_to_csv(args.src_dir, args.dst_dir)
