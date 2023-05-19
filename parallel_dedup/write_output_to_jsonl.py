import os, sys
current_path = os.path.abspath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(current_path)))
import argparse
import tqdm
from utils.utils import get_all_files
import jsonlines
import hashlib
import tempfile    
import json

def write_output_to_jsonl(csv_dir, jsonl_dir):

    # 获取所有jsonl文件
    file_path_list, file_nums = get_all_files(jsonl_dir, ['.jsonl'], 'list')
    for i in tqdm.tqdm(range(file_nums)):
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            with jsonlines.open(file_path_list[i]) as reader:
                file_path_list[i] = os.path.abspath(file_path_list[i])
                for one_json in reader:
                    file_name = hashlib.md5(file_path_list[i].encode('utf-8')).hexdigest() + hashlib.md5(one_json['文件名'].encode('utf-8')).hexdigest() + '.csv'
                    with open(os.path.join(csv_dir, file_name), 'r', encoding='utf-8') as f:
                        if f.read(1) == '1':
                            one_json['是否重复文件'] = True
                        else:
                            one_json['是否重复文件'] = False
                    temp_file.write(json.dumps(one_json) + '\n')
            os.replace(temp_file.name, file_path_list[i])



if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--csv_dir', type=str, required=True, help="csv源文件夹路径")
    # 添加可选参数，指定转换后文件存放路径，默认为converted/
    parser.add_argument('--jsonl_dir', type=str, required=True, help="jsonl源文件夹路径")

    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    write_output_to_jsonl(args.csv_dir, args.jsonl_dir)
